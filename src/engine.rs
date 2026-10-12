//! Segmented HTTP download engine.
//!
//! The file is tracked as a list of segments. Each worker owns one segment at
//! a time and streams it with a single Range request. A worker that runs out
//! of work takes over the back half of the largest remaining segment, so slow
//! connections are relieved instead of holding up the tail of the download.
//!
//! Every segment keeps two cursors: `fetched` (bytes accepted from the
//! network, possibly still buffered) and `written` (bytes handed to the OS).
//! Splits use `fetched` so buffered data is never orphaned; checkpoints use
//! `written`, and are taken before an fsync so a saved state never claims
//! bytes that are not on disk.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow, bail};
use bytes::Bytes;
use reqwest::header::{self, HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, Response, StatusCode};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;

use crate::ratelimit::RateLimiter;

/// Received bytes a worker holds before writing them out. Chunks are kept as
/// they arrived (no copy) and written with one pwritev.
const FLUSH_SIZE: usize = 128 << 10;
/// Buffered bytes older than this are written even if the buffer is not full,
/// so slow connections still reach the disk (and checkpoints) promptly.
const FLUSH_AGE: Duration = Duration::from_secs(1);
const STATE_VERSION: u32 = 1;

#[derive(Clone, Debug)]
pub struct Options {
    pub url: String,
    pub output: PathBuf,
    /// Upper bound on concurrent connections.
    pub max_conns: usize,
    /// A segment is only split when at least twice this much remains.
    pub min_split: u64,
    pub headers: Vec<(String, String)>,
    pub checkpoint_interval: Duration,
    pub progress_interval: Duration,
    /// Consecutive failed attempts before a worker gives up its segment.
    pub max_retries: u32,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    /// Every received byte is taken from each of these (e.g. this download's
    /// own limit and a daemon-wide one).
    pub limiters: Vec<Arc<RateLimiter>>,
    /// Reconnect a connection whose speed over `low_speed_window` is below
    /// this many bytes per second (0 disables). Ignored while a rate limit is
    /// in force, since throttled connections are slow on purpose.
    pub lowest_speed_limit: u64,
    pub low_speed_window: Duration,
}

impl Options {
    pub fn new(url: impl Into<String>, output: impl Into<PathBuf>) -> Self {
        Options {
            url: url.into(),
            output: output.into(),
            max_conns: 16,
            min_split: 1 << 20,
            headers: Vec::new(),
            checkpoint_interval: Duration::from_secs(1),
            progress_interval: Duration::from_millis(500),
            max_retries: 5,
            connect_timeout: Duration::from_secs(15),
            read_timeout: Duration::from_secs(30),
            limiters: Vec::new(),
            lowest_speed_limit: 0,
            low_speed_window: Duration::from_secs(10),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    Started {
        url: String,
        output: PathBuf,
        size: Option<u64>,
        ranged: bool,
        resumed_bytes: u64,
    },
    Progress {
        downloaded: u64,
        size: Option<u64>,
        speed: u64,
        connections: usize,
    },
    Warning {
        message: String,
    },
    Finished {
        output: PathBuf,
        size: u64,
        elapsed_ms: u64,
    },
}

#[derive(Debug)]
pub struct Summary {
    pub size: u64,
    pub elapsed: Duration,
}

/// On-disk checkpoint next to the partial file.
#[derive(Serialize, Deserialize)]
struct SavedState {
    version: u32,
    url: String,
    size: u64,
    validator: Option<String>,
    /// Remaining half-open ranges.
    remaining: Vec<(u64, u64)>,
}

pub fn part_path(output: &Path) -> PathBuf {
    let mut p = output.as_os_str().to_owned();
    p.push(".part");
    PathBuf::from(p)
}

pub fn state_path(output: &Path) -> PathBuf {
    let mut p = output.as_os_str().to_owned();
    p.push(".part.state");
    PathBuf::from(p)
}

struct Seg {
    fetched: u64,
    written: u64,
    end: u64,
    owner: Option<usize>,
}

/// Segments and worker liveness live under one lock, so a worker can only
/// leave while another live worker is guaranteed to see what it released.
struct Plan {
    segs: Vec<Seg>,
    alive: Vec<bool>,
    /// Per worker: how fast its connection delivers.
    rates: Vec<Rate>,
    /// Smoothed time from sending a request to its first byte, in seconds:
    /// what a worker loses before a newly taken range starts flowing.
    latency: f64,
    /// The same for a worker without a usable connection, which first has
    /// to connect (TCP and TLS handshakes): from the probe at first, then
    /// from the requests that opened connections.
    connect_latency: f64,
    /// `latency` has been measured, not just derived from the probe.
    latency_measured: bool,
}

/// A connection's delivery speed, an exponential moving average over the
/// time its responses were actually streaming (idle gaps are not counted).
#[derive(Clone, Default)]
struct Rate {
    /// Bytes per second; 0 until the first sample.
    bps: f64,
    /// Start of the current sample, while a response is streaming.
    since: Option<Instant>,
    bytes: u64,
    /// The connection has carried a response before, so its next request
    /// pays one round trip, not a handshake.
    reused: bool,
    /// When the current connection delivered its first response; while it
    /// is young, TCP slow start keeps it below its eventual speed.
    born: Option<Instant>,
    /// When the connection last delivered bytes.
    last: Option<Instant>,
}

/// The worker a split would hand work to, and how long it needs for it.
struct Thief {
    /// It has a live connection (else it must connect first).
    connected: bool,
    /// Bytes per second once flowing.
    speed: f64,
    /// Time to first byte on a live connection.
    latency: f64,
    /// Time to first byte when connecting first.
    connect: f64,
    rtt: f64,
}

impl Thief {
    /// Seconds until it has delivered `x` bytes. A new connection pays its
    /// handshakes and then slow start: after k round trips it has delivered
    /// IW (2^k - 1) bytes, until its window carries `speed` per round trip.
    fn time(&self, x: f64) -> f64 {
        if self.connected {
            return self.latency + x / self.speed;
        }
        let iw = INITIAL_WINDOW;
        // Bytes delivered by the time slow start reaches `speed`.
        let full = (self.speed * self.rtt).max(iw);
        let ramp_bytes = full - iw;
        let ramp = if x <= ramp_bytes {
            self.rtt * (1.0 + x / iw).log2()
        } else {
            self.rtt * (full / iw).log2() + (x - ramp_bytes) / self.speed
        };
        self.connect + ramp
    }

    /// The share `x` of a victim's `r` remaining bytes (at `v` bytes per
    /// second) that makes both finish together: `time(x) = (r - x) / v`.
    /// `time` is increasing, so the root is found by bisection; 0 when the
    /// thief could not even start before the victim is done.
    fn balance(&self, r: f64, v: f64) -> f64 {
        let gap = |x: f64| self.time(x) - (r - x) / v;
        if gap(0.0) >= 0.0 {
            return 0.0;
        }
        let (mut lo, mut hi) = (0.0, r);
        for _ in 0..50 {
            let mid = (lo + hi) / 2.0;
            if gap(mid) < 0.0 {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        lo
    }
}

/// What a worker should do next.
enum Claim {
    Work(usize),
    /// Everything left is in progress and not worth splitting now; ask
    /// again shortly, since speeds change.
    Wait,
    /// Nothing left.
    Done,
}

/// How often a waiting worker looks for work again.
const WAIT_POLL: Duration = Duration::from_millis(100);
/// Smallest range taken from another worker once speeds are known (or the
/// user's minimum split, if smaller). Below it a request costs more than it
/// moves, except to rescue a stalled connection.
const STEAL_FLOOR: u64 = 64 << 10;
/// A split that brings its victim's predicted end forward by at least this
/// much is made whatever its size: the victim has all but stalled.
const RESCUE_SAVING: f64 = 1.0;
/// A new TCP connection's first flight (Linux's initial window of ten
/// segments); slow start doubles it every round trip.
const INITIAL_WINDOW: f64 = 14600.0;
/// Round trips after which a connection's measured speed is taken as its
/// steady speed (slow start is over by then on any realistic path).
const WARM_RTTS: f64 = 10.0;

/// Samples shorter than this are merged into the next one.
const RATE_SAMPLE: Duration = Duration::from_millis(200);
/// Time constant of the speed average.
const RATE_TAU: f64 = 1.0;

/// Idle time after which a kept-alive connection is assumed closed by the
/// server (Apache's default KeepAliveTimeout is 5 s).
const KEEPALIVE: Duration = Duration::from_secs(4);

impl Rate {
    fn start(&mut self, now: Instant) {
        self.since = Some(now);
        self.last = Some(now);
        self.bytes = 0;
    }

    /// The worker holds a connection its next request can reuse.
    fn live(&self, now: Instant) -> bool {
        self.reused && self.last.is_none_or(|t| now.duration_since(t) < KEEPALIVE)
    }

    fn add(&mut self, n: u64, now: Instant) {
        let Some(since) = self.since else { return };
        self.last = Some(now);
        self.bytes += n;
        let dt = now.duration_since(since);
        if dt < RATE_SAMPLE {
            return;
        }
        let dt = dt.as_secs_f64();
        let inst = self.bytes as f64 / dt;
        self.bps = if self.bps == 0.0 {
            inst
        } else {
            self.bps + (1.0 - (-dt / RATE_TAU).exp()) * (inst - self.bps)
        };
        self.start(now);
    }
}

impl Plan {
    fn workers(&self) -> usize {
        self.alive.iter().filter(|&&a| a).count()
    }
}

struct Shared {
    /// URL the user gave; requests fall back to it when `url` expires.
    origin_url: String,
    /// Credentials, sent only to `origin_url`'s origin.
    secret_headers: HeaderMap,
    /// Where requests go: the redirect target, refreshed when it expires.
    url: RwLock<String>,
    limiters: Vec<Arc<RateLimiter>>,
    lowest_speed_limit: u64,
    low_speed_window: Duration,
    validator: Option<String>,
    size: u64,
    min_split: u64,
    max_retries: u32,
    /// How many workers may be streaming at once; raised by `steer` while
    /// more connections keep paying off.
    target: AtomicUsize,
    file: Arc<File>,
    plan: Mutex<Plan>,
    cancel: CancellationToken,
    events: Option<UnboundedSender<Event>>,
}

impl Shared {
    fn emit(&self, e: Event) {
        if let Some(tx) = &self.events {
            let _ = tx.send(e);
        }
    }

    fn seg<R>(&self, i: usize, f: impl FnOnce(&mut Seg) -> R) -> R {
        f(&mut self.plan.lock().unwrap().segs[i])
    }

    /// Picks an unowned segment, or splits one being worked on. When
    /// nothing is left at all, the worker is marked as gone.
    fn claim(&self, id: usize) -> Claim {
        let mut plan = self.plan.lock().unwrap();
        // Workers streaming now; `target` caps them (see `steer`).
        let busy = plan
            .segs
            .iter()
            .filter(|s| s.owner.is_some() && s.written < s.end)
            .count();
        if busy < self.target.load(Ordering::Relaxed) {
            if let Some(seg) = Self::claim_locked(&mut plan, id, self.min_split) {
                return Claim::Work(seg);
            }
        }
        if plan.segs.iter().any(|s| s.written < s.end) {
            return Claim::Wait;
        }
        plan.alive[id] = false;
        Claim::Done
    }

    /// Gives worker `id` work: an unowned range if there is one, else the
    /// tail of the range predicted to finish last.
    ///
    /// The download ends when its last range does, so a split should bring
    /// that time forward as far as possible. A victim with `r` bytes left at
    /// speed `v` finishes in `r / v`. If the thief takes the last `x` bytes,
    /// the victim needs `(r - x) / v` and the thief `T(x)`; both end together,
    /// the best either can do, where `T(x) = (r - x) / v`. For a thief with
    /// a live connection `T(x) = l + x / t` (round trip `l`, speed `t`),
    /// giving `x = t (r - v l) / (v + t)`: halving when speeds are equal and
    /// `l` is 0, and nothing when the victim is done within `l`. A thief that
    /// must connect first also pays the handshakes and slow start (see
    /// `Thief::time`), which makes it take less. Before any speed is known,
    /// ranges are halved.
    fn claim_locked(plan: &mut Plan, id: usize, min_split: u64) -> Option<usize> {
        if let Some(i) = plan
            .segs
            .iter()
            .position(|s| s.owner.is_none() && s.written < s.end)
        {
            plan.segs[i].owner = Some(id);
            return Some(i);
        }
        let known: Vec<f64> = plan
            .rates
            .iter()
            .map(|r| r.bps)
            .filter(|&b| b > 0.0)
            .collect();
        let (i, x) = if known.is_empty() {
            let (i, remain) = plan
                .segs
                .iter()
                .enumerate()
                .filter(|(_, s)| s.owner.is_some())
                .map(|(i, s)| (i, s.end - s.fetched))
                .max_by_key(|&(_, r)| r)?;
            if remain < 2 * min_split {
                return None;
            }
            (i, remain - remain / 2)
        } else {
            let rtt = plan.latency.max(0.001);
            let now = Instant::now();
            let warm = |w: usize| {
                plan.rates[w]
                    .born
                    .is_some_and(|b| now.duration_since(b).as_secs_f64() >= WARM_RTTS * rtt)
            };
            let steady: Vec<f64> = (0..plan.rates.len())
                .filter(|&w| warm(w) && plan.rates[w].bps > 0.0)
                .map(|w| plan.rates[w].bps)
                .collect();
            let pool = if steady.is_empty() { &known } else { &steady };
            let mean = pool.iter().sum::<f64>() / pool.len() as f64;
            // A young connection is still ramping up: judge it by what an
            // established one does, not by its first slow round trips.
            let speed = |w: usize| match plan.rates[w].bps {
                b if b > 0.0 && warm(w) => b,
                b => b.max(mean),
            };
            let (i, remain, v) = plan
                .segs
                .iter()
                .enumerate()
                .filter_map(|(i, s)| Some((i, s.end - s.fetched, speed(s.owner?))))
                .filter(|&(_, r, _)| r > 0)
                .max_by(|a, b| (a.1 as f64 / a.2).total_cmp(&(b.1 as f64 / b.2)))?;
            let r = remain as f64;
            let thief = Thief {
                connected: plan.rates[id].live(now),
                speed: speed(id),
                latency: plan.latency,
                connect: plan.connect_latency,
                rtt,
            };
            let x = thief.balance(r, v);
            // The victim now ends x / v sooner, and so does the download if
            // it was the last to finish.
            let saving = x / v;
            if x < 1.0 || (x < min_split.min(STEAL_FLOOR) as f64 && saving < RESCUE_SAVING) {
                return None;
            }
            (i, (x as u64).clamp(1, remain))
        };
        let victim = &mut plan.segs[i];
        let cut = victim.end - x;
        let end = victim.end;
        victim.end = cut;
        plan.segs.push(Seg {
            fetched: cut,
            written: cut,
            end,
            owner: Some(id),
        });
        Some(plan.segs.len() - 1)
    }

    /// The worker's connection is gone; its next one is of unknown speed.
    fn forget_rate(&self, id: usize) {
        self.plan.lock().unwrap().rates[id] = Rate::default();
    }

    /// Records a request's time to first byte. Only requests on reused
    /// connections count: a split hands work to a worker that is already
    /// connected, and handshakes would inflate the estimate several times.
    fn observe_latency(&self, id: usize, secs: f64) {
        let mut plan = self.plan.lock().unwrap();
        let now = Instant::now();
        let rate = &mut plan.rates[id];
        let was_live = rate.live(now);
        rate.reused = true;
        if was_live {
            plan.latency = if plan.latency_measured {
                0.7 * plan.latency + 0.3 * secs
            } else {
                secs
            };
            plan.latency_measured = true;
        } else {
            // This request opened a connection: what that costs here, free
            // of the probe's DNS lookup, redirects and retries.
            rate.born = Some(now);
            plan.connect_latency = 0.5 * plan.connect_latency + 0.5 * secs;
            if !plan.latency_measured {
                plan.latency = plan.connect_latency / 3.0;
            }
        }
    }

    /// Workers streaming a range (waiting ones are not counted).
    fn active(&self) -> usize {
        let plan = self.plan.lock().unwrap();
        plan.segs
            .iter()
            .filter(|s| s.owner.is_some() && s.written < s.end)
            .count()
    }

    fn release_locked(plan: &mut Plan, seg: usize) {
        let s = &mut plan.segs[seg];
        s.owner = None;
        // Anything fetched but not written was lost with the worker's buffer.
        s.fetched = s.written;
    }

    /// Releases `seg` and marks the worker as gone.
    fn leave(&self, id: usize, seg: usize) {
        let mut plan = self.plan.lock().unwrap();
        Self::release_locked(&mut plan, seg);
        plan.alive[id] = false;
    }

    /// Leaves like `leave`, unless this is the last live worker, in which case
    /// it keeps the segment and returns false.
    fn try_leave(&self, id: usize, seg: usize) -> bool {
        let mut plan = self.plan.lock().unwrap();
        if plan.workers() <= 1 {
            return false;
        }
        Self::release_locked(&mut plan, seg);
        plan.alive[id] = false;
        true
    }

    /// For the last live worker, whose server has stopped honouring Range:
    /// turns `seg` into everything from the first missing byte to the end,
    /// so one plain stream (skipping what is already there) finishes the
    /// download. Returns where that is, or None while other workers are alive.
    fn take_over_all(&self, seg: usize) -> Option<u64> {
        let mut plan = self.plan.lock().unwrap();
        if plan.workers() > 1 {
            return None;
        }
        // Bytes below the first unfinished range are all on disk.
        let first = plan
            .segs
            .iter()
            .filter(|s| s.written < s.end)
            .map(|s| s.written)
            .min()?;
        for s in &mut plan.segs {
            s.fetched = s.end;
            s.written = s.end;
        }
        let s = &mut plan.segs[seg];
        s.fetched = first;
        s.written = first;
        s.end = self.size;
        Some(first)
    }

    /// Bytes received so far, including data still buffered in workers.
    fn received(&self) -> u64 {
        let plan = self.plan.lock().unwrap();
        let left: u64 = plan.segs.iter().map(|s| s.end - s.fetched).sum();
        self.size - left
    }

    fn remaining(&self) -> Vec<(u64, u64)> {
        let plan = self.plan.lock().unwrap();
        let mut r: Vec<_> = plan
            .segs
            .iter()
            .filter(|s| s.written < s.end)
            .map(|s| (s.written, s.end))
            .collect();
        r.sort();
        r
    }
}

enum SegError {
    /// Server refused this connection (connection limit, rate limiting), or
    /// answered with something other than the file being downloaded. Never
    /// written to the file; other connections carry on.
    Rejected(String),
    /// 401/403/410: the URL may have expired; worth resolving the original
    /// URL again before treating it as a refusal.
    Expired(StatusCode),
    /// The server sent the whole file (200) for a range request, labelled as
    /// the same file. Usable only by a connection that is the last one left.
    Unranged(Response),
    /// Transient; retry the same segment.
    Retry(anyhow::Error),
    /// The download cannot continue.
    Fatal(anyhow::Error),
    Cancelled,
}

impl SegError {
    /// What an `Expired` status means once re-resolving did not help: 403 is
    /// usually a connection limit, 401/410 will not get better by retrying.
    fn expired_as_final(self) -> SegError {
        match self {
            SegError::Expired(s @ StatusCode::FORBIDDEN) => {
                SegError::Rejected(format!("server responded {s}"))
            }
            SegError::Expired(s) => SegError::Fatal(anyhow!("server responded {s}")),
            SegError::Unranged(_) => SegError::Rejected("server ignored Range".into()),
            e => e,
        }
    }
}

/// One TLS configuration for the whole process. Each worker gets its own
/// `Client` (its own connection), and building the verifier per client would
/// re-read the system certificate store every time; the config is cheap to
/// clone because its parts are shared.
///
/// Only HTTP/1.1 is offered. Each connection carries one response at a time,
/// so HTTP/2 multiplexing has nothing to offer, while its flow-control window
/// caps a connection at window / RTT (measured on Google's CDN: HTTP/2 held
/// at ~60 MB/s where HTTP/1.1, left to TCP's own window, reached 100-140).
fn tls_config() -> anyhow::Result<rustls::ClientConfig> {
    static CONFIG: std::sync::OnceLock<Result<rustls::ClientConfig, String>> =
        std::sync::OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
            let verifier = rustls_platform_verifier::Verifier::new(provider.clone())
                .map_err(|e| e.to_string())?;
            let mut config = rustls::ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map_err(|e| e.to_string())?
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(verifier))
                .with_no_client_auth();
            config.alpn_protocols = vec![b"http/1.1".to_vec()];
            Ok(config)
        })
        .clone()
        .map_err(|e| anyhow!("TLS setup failed: {e}"))
}

/// Splits the user's headers into ones every request carries and
/// credentials, which only go to the origin the user named (as a browser or
/// `curl -L` would do across a redirect).
fn split_headers(opts: &Options) -> anyhow::Result<(HeaderMap, HeaderMap)> {
    let (mut plain, mut secret) = (HeaderMap::new(), HeaderMap::new());
    for (k, v) in &opts.headers {
        let name =
            HeaderName::from_bytes(k.as_bytes()).with_context(|| format!("header name {k}"))?;
        let value = HeaderValue::from_str(v).with_context(|| format!("header value for {k}"))?;
        let sensitive = [
            header::AUTHORIZATION,
            header::COOKIE,
            header::PROXY_AUTHORIZATION,
        ]
        .contains(&name);
        if sensitive { &mut secret } else { &mut plain }.append(name, value);
    }
    Ok((plain, secret))
}

/// Scheme, host and port match.
fn same_origin(a: &str, b: &str) -> bool {
    match (reqwest::Url::parse(a), reqwest::Url::parse(b)) {
        (Ok(a), Ok(b)) => {
            a.scheme() == b.scheme()
                && a.host_str() == b.host_str()
                && a.port_or_known_default() == b.port_or_known_default()
        }
        _ => false,
    }
}

fn build_client(opts: &Options, headers: &HeaderMap) -> anyhow::Result<Client> {
    Ok(Client::builder()
        .tls_backend_preconfigured(tls_config()?)
        .user_agent(concat!("pdl/", env!("CARGO_PKG_VERSION")))
        .default_headers(headers.clone())
        .connect_timeout(opts.connect_timeout)
        .read_timeout(opts.read_timeout)
        .build()?)
}

/// A status that ended a request, kept typed so callers can tell transient
/// refusals from final ones.
#[derive(Debug)]
struct HttpStatus(StatusCode);

impl std::fmt::Display for HttpStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "server responded {}", self.0)
    }
}

impl std::error::Error for HttpStatus {}

/// The body ended before its Content-Length.
#[derive(Debug)]
struct Short {
    total: u64,
    expected: u64,
}

impl std::fmt::Display for Short {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "received {} bytes, expected {}",
            self.total, self.expected
        )
    }
}

impl std::error::Error for Short {}

/// Network failures and overload statuses are worth another attempt; a 404
/// or a disk error is not.
fn is_transient(e: &anyhow::Error) -> bool {
    if let Some(HttpStatus(s)) = e.downcast_ref() {
        return s.is_server_error()
            || *s == StatusCode::TOO_MANY_REQUESTS
            || *s == StatusCode::REQUEST_TIMEOUT;
    }
    e.downcast_ref::<reqwest::Error>().is_some() || e.downcast_ref::<Short>().is_some()
}

fn backoff(failures: u32) -> Duration {
    Duration::from_millis(100 << failures.min(6))
}

/// `probe`, retried like a segment request, and abandoned on cancel.
async fn probe_retrying(
    client: &Client,
    secret: &HeaderMap,
    url: &str,
    if_range: Option<&str>,
    max_retries: u32,
    cancel: &CancellationToken,
    emit: &impl Fn(Event),
) -> anyhow::Result<Probe> {
    let mut failures = 0;
    loop {
        let r = tokio::select! {
            r = probe(client, secret, url, if_range) => r,
            _ = cancel.cancelled() => bail!("cancelled"),
        };
        match r {
            Err(e) if failures < max_retries && is_transient(&e) => {
                failures += 1;
                emit(Event::Warning {
                    message: format!("{e:#}; retrying"),
                });
                tokio::select! {
                    _ = tokio::time::sleep(backoff(failures)) => {}
                    _ = cancel.cancelled() => bail!("cancelled"),
                }
            }
            r => return r,
        }
    }
}

struct Probe {
    resp: Response,
    /// Seconds from sending the (successful) request to its response.
    latency: f64,
    final_url: String,
    /// Total size when the server answered 206 with a known length.
    ranged_size: Option<u64>,
    validator: Option<String>,
}

/// `url` is the origin, so it gets the credentials; reqwest drops them if a
/// redirect leaves that origin.
async fn probe(
    client: &Client,
    secret: &HeaderMap,
    url: &str,
    if_range: Option<&str>,
) -> anyhow::Result<Probe> {
    let started = Instant::now();
    let mut req = client
        .get(url)
        .headers(secret.clone())
        .header(header::RANGE, "bytes=0-");
    if let Some(v) = if_range {
        req = req.header(header::IF_RANGE, v);
    }
    let mut resp = req.send().await?;
    if resp.status() == StatusCode::RANGE_NOT_SATISFIABLE {
        // bytes=0- is unsatisfiable only for an empty file.
        resp = client.get(url).headers(secret.clone()).send().await?;
    }
    let status = resp.status();
    if status != StatusCode::OK && status != StatusCode::PARTIAL_CONTENT {
        return Err(HttpStatus(status).into());
    }
    let ranged_size = if status == StatusCode::PARTIAL_CONTENT {
        let (start, _, total) = content_range(&resp)?;
        if start != 0 {
            bail!("server returned a range starting at {start} for bytes=0-");
        }
        total
    } else {
        None
    };
    let validator = validator(resp.headers());
    Ok(Probe {
        latency: started.elapsed().as_secs_f64(),
        final_url: resp.url().to_string(),
        resp,
        ranged_size,
        validator,
    })
}

/// Size of the whole file a response streams from byte 0. For a 206 that is
/// the Content-Range total, never the length of the (possibly cut) range.
fn whole_length(resp: &Response) -> Option<u64> {
    if resp.status() == StatusCode::PARTIAL_CONTENT {
        content_range(resp).ok().and_then(|(_, _, total)| total)
    } else {
        resp.content_length()
    }
}

/// A strong ETag, else Last-Modified: what If-Range can safely compare.
fn validator(h: &HeaderMap) -> Option<String> {
    let get = |n| h.get(n).and_then(|v: &HeaderValue| v.to_str().ok());
    match get(header::ETAG) {
        Some(e) if !e.starts_with("W/") => Some(e.to_string()),
        _ => get(header::LAST_MODIFIED).map(str::to_string),
    }
}

/// Parses `Content-Range: bytes a-b/total` into (a, b+1, total).
fn content_range(resp: &Response) -> anyhow::Result<(u64, u64, Option<u64>)> {
    let v = resp
        .headers()
        .get(header::CONTENT_RANGE)
        .ok_or_else(|| anyhow!("206 without Content-Range"))?
        .to_str()?;
    let spec = v
        .strip_prefix("bytes ")
        .ok_or_else(|| anyhow!("bad Content-Range {v}"))?;
    let (range, total) = spec
        .split_once('/')
        .ok_or_else(|| anyhow!("bad Content-Range {v}"))?;
    let (a, b) = range
        .split_once('-')
        .ok_or_else(|| anyhow!("bad Content-Range {v}"))?;
    let total = if total == "*" {
        None
    } else {
        Some(total.parse()?)
    };
    Ok((a.parse()?, b.parse::<u64>()? + 1, total))
}

fn preallocate(file: &File, size: u64) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: valid fd for the lifetime of `file`; mode 0 allocates and extends.
        let r = unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, size as libc::off_t) };
        if r == 0 {
            return Ok(());
        }
        let e = std::io::Error::last_os_error();
        // A full disk must fail now, not halfway through the download.
        if !matches!(
            e.raw_os_error(),
            Some(libc::EOPNOTSUPP | libc::ENOSYS | libc::EINVAL)
        ) {
            return Err(e);
        }
    }
    file.set_len(size)
}

/// Opens (creating if needed) and exclusively locks the partial file, so two
/// downloads, in this process or another, never write the same file.
fn lock_part(part: &Path) -> anyhow::Result<File> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::MetadataExt;
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(part)
        .with_context(|| format!("cannot write {}", part.display()))?;
    // SAFETY: valid fd for the lifetime of `file`.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        let e = std::io::Error::last_os_error();
        if e.kind() == std::io::ErrorKind::WouldBlock {
            bail!("{} is being downloaded by another process", part.display());
        }
        // Filesystems without locks still get a download, just unguarded.
    }
    // The previous holder may have renamed the file away between our open
    // and the lock; then the lock guards the finished file, not this path.
    let id = |m: std::fs::Metadata| (m.dev(), m.ino());
    if std::fs::metadata(part).map(id).ok() != Some(id(file.metadata()?)) {
        bail!("{} was finished by another download", part.display());
    }
    Ok(file)
}

/// Whether a checkpoint can describe `part`: ranges in order, inside the
/// file, and the file still its full preallocated length.
fn consistent(s: &SavedState, part: &File) -> bool {
    part.metadata().is_ok_and(|m| m.len() == s.size)
        && s.remaining.iter().all(|&(a, b)| a < b && b <= s.size)
        && s.remaining.windows(2).all(|w| w[0].1 <= w[1].0)
}

fn load_state(path: &Path) -> Option<SavedState> {
    let data = std::fs::read(path).ok()?;
    let s: SavedState = serde_json::from_slice(&data).ok()?;
    (s.version == STATE_VERSION).then_some(s)
}

fn save_state(path: &Path, state: &SavedState) -> std::io::Result<()> {
    let tmp = path.with_extension("state.tmp");
    let r = (|| {
        let f = File::create(&tmp)?;
        serde_json::to_writer(&f, state)?;
        f.sync_data()?;
        std::fs::rename(&tmp, path)
    })();
    if r.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    r
}

/// Downloads `opts.url` to `opts.output`, resuming from a previous checkpoint
/// when one matches the remote file.
pub async fn download(
    opts: Options,
    events: Option<UnboundedSender<Event>>,
    cancel: CancellationToken,
) -> anyhow::Result<Summary> {
    let started = Instant::now();
    let part = part_path(&opts.output);
    let state_file = state_path(&opts.output);
    let emit = |e| {
        if let Some(tx) = &events {
            let _ = tx.send(e);
        }
    };

    if opts.output.exists() {
        bail!("{} already exists", opts.output.display());
    }
    let (plain_headers, secret_headers) = split_headers(&opts)?;
    let client = build_client(&opts, &plain_headers)?;

    let part_existed = part.exists();
    let file = lock_part(&part)?;
    // A checkpoint is only trusted when the server still has the same file.
    let saved = match load_state(&state_file).filter(|s| part_existed && s.url == opts.url) {
        Some(s) if !consistent(&s, &file) => {
            emit(Event::Warning {
                message: "checkpoint does not match the partial file; restarting from scratch"
                    .into(),
            });
            None
        }
        s => s,
    };
    let p = match probe_retrying(
        &client,
        &secret_headers,
        &opts.url,
        saved.as_ref().and_then(|s| s.validator.as_deref()),
        opts.max_retries,
        &cancel,
        &emit,
    )
    .await
    {
        Ok(p) => p,
        Err(e) => {
            if !part_existed {
                let _ = std::fs::remove_file(&part);
            }
            return Err(e);
        }
    };
    let Probe {
        resp: mut probe_resp,
        latency: probe_latency,
        final_url,
        ranged_size,
        validator,
    } = p;
    let saved = match saved {
        Some(s) if ranged_size == Some(s.size) && validator == s.validator => Some(s),
        Some(_) => {
            emit(Event::Warning {
                message: "remote file changed; restarting from scratch".into(),
            });
            None
        }
        None => None,
    };

    let Some(size) = ranged_size else {
        // No usable Range support: one sequential stream, no checkpoints.
        let _ = std::fs::remove_file(&state_file);
        let mut size = whole_length(&probe_resp);
        emit(Event::Started {
            url: opts.url.clone(),
            output: opts.output.clone(),
            size,
            ranged: false,
            resumed_bytes: 0,
        });
        file.set_len(0)?;
        let file = Arc::new(file);
        let downloaded = Arc::new(AtomicU64::new(0));
        let ticker = {
            let downloaded = downloaded.clone();
            spawn_progress(&opts, events.clone(), size, move || {
                (downloaded.load(Ordering::Relaxed), 1)
            })
        };
        // Without ranges a broken stream can only be restarted from zero.
        let mut failures = 0;
        let r = loop {
            let r = sequential(&mut probe_resp, &file, &downloaded, &opts.limiters, &cancel)
                .await
                .and_then(|total| match size {
                    Some(expected) if total != expected => {
                        Err(anyhow::Error::new(Short { total, expected }))
                    }
                    _ => Ok(total),
                });
            match r {
                Err(e) if failures < opts.max_retries && is_transient(&e) => {
                    failures += 1;
                    emit(Event::Warning {
                        message: format!("{e:#}; restarting (no Range support)"),
                    });
                    tokio::select! {
                        _ = tokio::time::sleep(backoff(failures)) => {}
                        _ = cancel.cancelled() => break Err(anyhow!("cancelled")),
                    }
                    match probe_retrying(
                        &client,
                        &secret_headers,
                        &opts.url,
                        None,
                        opts.max_retries,
                        &cancel,
                        &emit,
                    )
                    .await
                    {
                        Ok(p) => {
                            probe_resp = p.resp;
                            size = whole_length(&probe_resp);
                            if let Err(e) = file.set_len(0) {
                                break Err(e.into());
                            }
                            downloaded.store(0, Ordering::Relaxed);
                        }
                        Err(e) => break Err(e),
                    }
                }
                r => break r,
            }
        };
        drop(ticker);
        let total = r?;
        file.sync_data()?;
        std::fs::rename(&part, &opts.output)
            .with_context(|| format!("cannot move {} into place", part.display()))?;
        let elapsed = started.elapsed();
        emit(Event::Finished {
            output: opts.output.clone(),
            size: total,
            elapsed_ms: elapsed.as_millis() as u64,
        });
        return Ok(Summary {
            size: total,
            elapsed,
        });
    };

    if validator.is_none() {
        emit(Event::Warning {
            message:
                "server sent no strong ETag or Last-Modified; a changed file cannot be detected"
                    .into(),
        });
    }

    let (file, segs, resumed, first_resp) = match &saved {
        Some(s) => {
            let segs: Vec<Seg> = s
                .remaining
                .iter()
                .map(|&(a, b)| Seg {
                    fetched: a,
                    written: a,
                    end: b,
                    owner: None,
                })
                .collect();
            let left: u64 = s.remaining.iter().map(|(a, b)| b - a).sum();
            // The probe body starts at 0, which is useless when resuming;
            // drop it now so it does not hold a connection slot.
            drop(probe_resp);
            (file, segs, size - left, None)
        }
        None => {
            // A stale checkpoint must never describe the fresh file below.
            match std::fs::remove_file(&state_file) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
                _ => {}
            }
            file.set_len(0)?;
            preallocate(&file, size)
                .with_context(|| format!("cannot allocate {size} bytes for {}", part.display()))?;
            let segs = vec![Seg {
                fetched: 0,
                written: 0,
                end: size,
                owner: Some(0),
            }];
            (file, segs, 0, Some(probe_resp))
        }
    };

    emit(Event::Started {
        url: opts.url.clone(),
        output: opts.output.clone(),
        size: Some(size),
        ranged: true,
        resumed_bytes: resumed,
    });

    let max_conns = opts.max_conns.clamp(1, crate::daemon::MAX_CONNECTIONS);
    let shared = Arc::new(Shared {
        origin_url: opts.url.clone(),
        secret_headers: secret_headers.clone(),
        url: RwLock::new(final_url),
        limiters: opts.limiters.clone(),
        lowest_speed_limit: opts.lowest_speed_limit,
        low_speed_window: opts.low_speed_window,
        validator,
        size,
        min_split: opts.min_split.max(1),
        max_retries: opts.max_retries,
        target: AtomicUsize::new(1),
        file: Arc::new(file),
        plan: Mutex::new(Plan {
            segs,
            alive: vec![true; max_conns],
            rates: {
                let mut rates = vec![Rate::default(); max_conns];
                if first_resp.is_some() {
                    // Worker 0 streams the probe's response: connected now.
                    let now = Instant::now();
                    rates[0].reused = true;
                    rates[0].born = Some(now);
                    rates[0].last = Some(now);
                }
                rates
            },
            // The probe also paid for DNS and the TCP and TLS handshakes,
            // roughly three round trips in all.
            latency: probe_latency / 3.0,
            connect_latency: probe_latency,
            latency_measured: false,
        }),
        cancel: cancel.clone(),
        events: events.clone(),
    });

    // Built before any background task starts, so a failure here cannot
    // return early past the code that stops them.
    let clients = (0..max_conns)
        .map(|id| match id {
            0 => Ok(client.clone()),
            _ => build_client(&opts, &plain_headers),
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    // Joined (not aborted) at the end: an aborted task would leave its
    // spawn_blocking save running, racing the final state write or removal.
    let stop_checkpoints = CancellationToken::new();
    let checkpointer = {
        let shared = shared.clone();
        let state_file = state_file.clone();
        let url = opts.url.clone();
        let interval = opts.checkpoint_interval;
        let stop = stop_checkpoints.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.tick().await;
            loop {
                tokio::select! {
                    _ = tick.tick() => {}
                    _ = stop.cancelled() => return,
                }
                if let Err(e) = checkpoint(&shared, &state_file, &url).await {
                    shared.emit(Event::Warning {
                        message: format!("checkpoint failed: {e}"),
                    });
                }
            }
        })
    };
    let ticker = {
        let shared = shared.clone();
        spawn_progress(&opts, events.clone(), Some(size), move || {
            (shared.received(), shared.active())
        })
    };
    let steering = AbortOnDrop(tokio::spawn(steer(shared.clone(), max_conns)));

    let mut workers = tokio::task::JoinSet::new();
    let mut first_resp = first_resp;
    for (id, client) in clients.into_iter().enumerate() {
        let shared = shared.clone();
        let initial = if id == 0 {
            first_resp.take().map(|r| (0, r))
        } else {
            None
        };
        workers.spawn(worker(id, client, shared, initial));
    }

    let mut result = Ok(());
    while let Some(joined) = workers.join_next().await {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if result.is_ok() {
                    result = Err(e);
                }
                cancel.cancel();
            }
            Err(e) => {
                if result.is_ok() {
                    result = Err(anyhow!("worker panicked: {e}"));
                }
                cancel.cancel();
            }
        }
    }
    stop_checkpoints.cancel();
    let _ = checkpointer.await;
    // Both hold the shared state, and so the partial file and its lock: they
    // must be gone before this returns, or a download started right after
    // (a quick pause and resume) would find the file still locked.
    ticker.stop().await;
    steering.stop().await;

    let remaining = shared.remaining();
    if result.is_ok() && !remaining.is_empty() {
        result = Err(if cancel.is_cancelled() {
            anyhow!("cancelled")
        } else {
            anyhow!("all connections failed")
        });
    }
    if let Err(e) = result {
        // Leave an exact checkpoint so the next run resumes where this stopped.
        if let Err(ce) = checkpoint(&shared, &state_file, &opts.url).await {
            return Err(e.context(format!("and saving the checkpoint failed: {ce:#}")));
        }
        return Err(e);
    }

    shared.file.sync_data()?;
    std::fs::rename(&part, &opts.output)
        .with_context(|| format!("cannot move {} into place", part.display()))?;
    let _ = std::fs::remove_file(&state_file);
    let elapsed = started.elapsed();
    emit(Event::Finished {
        output: opts.output.clone(),
        size,
        elapsed_ms: elapsed.as_millis() as u64,
    });
    Ok(Summary { size, elapsed })
}

async fn checkpoint(shared: &Arc<Shared>, path: &Path, url: &str) -> anyhow::Result<()> {
    // Snapshot first: everything it counts as written is already in the page
    // cache, so the fsync below makes the snapshot true on disk.
    let state = SavedState {
        version: STATE_VERSION,
        url: url.to_string(),
        size: shared.size,
        validator: shared.validator.clone(),
        remaining: shared.remaining(),
    };
    let file = shared.file.clone();
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        file.sync_data()?;
        save_state(&path, &state)
    })
    .await??;
    Ok(())
}

/// Factor by which `steer` multiplies the number of connections per step.
const STEER_STEP: usize = 4;
/// Throughput a step must add to be kept: on a path one connection already
/// fills, more connections only add handshakes, slow starts and losses.
const STEER_GAIN: f64 = 1.25;

/// Chooses how many connections to use by measuring what they deliver.
///
/// Total throughput is a concave function of the number of connections:
/// it grows while the server limits each connection, and flattens (or
/// falls, as parallel slow starts overshoot the bottleneck) once the path
/// is full. So start with one, and once throughput has settled, multiply by
/// `STEER_STEP`; keep a step only if throughput rose by `STEER_GAIN`, else
/// go back and stop. Stop as well when what is left would be done before
/// new connections could get up to speed.
async fn steer(shared: Arc<Shared>, max: usize) {
    let (mut n, mut prev) = (1usize, 1usize);
    let mut base: Option<f64> = None;
    let mut changed = Instant::now();
    let mut last = (Instant::now(), shared.received());
    let mut last_rate = 0.0;
    while n < max {
        let (rtt, connect) = {
            let p = shared.plan.lock().unwrap();
            (p.latency.max(0.001), p.connect_latency)
        };
        let window = (2.0 * rtt).max(0.15);
        tokio::time::sleep(Duration::from_secs_f64(window)).await;
        let now = Instant::now();
        let got = shared.received();
        let rate = got.saturating_sub(last.1) as f64 / now.duration_since(last.0).as_secs_f64();
        last = (now, got);
        // Settled: the newest connections had time to connect, and the
        // total stopped climbing (slow start is over).
        let climbing = rate > 1.1 * last_rate;
        last_rate = rate;
        if now.duration_since(changed).as_secs_f64() < connect + 2.0 * window || climbing {
            continue;
        }
        if base.is_some_and(|b| rate < STEER_GAIN * b) {
            shared.target.store(prev, Ordering::Relaxed);
            return;
        }
        let left = shared.size.saturating_sub(got) as f64;
        if left < rate * (connect + WARM_RTTS * rtt) {
            return;
        }
        base = Some(rate);
        prev = n;
        n = (n * STEER_STEP).min(max);
        shared.target.store(n, Ordering::Relaxed);
        changed = now;
    }
}

/// A task aborted when dropped, so no return path can leave it running.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl AbortOnDrop {
    /// Aborts the task and waits until it is gone, along with whatever it
    /// held (abort alone only takes effect at the task's next poll).
    async fn stop(mut self) {
        self.0.abort();
        let _ = (&mut self.0).await;
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Emits a Progress event every interval; `sample` returns (bytes, connections).
/// It holds an event sender, which a consumer may wait on to see the end.
fn spawn_progress(
    opts: &Options,
    events: Option<UnboundedSender<Event>>,
    size: Option<u64>,
    sample: impl Fn() -> (u64, usize) + Send + 'static,
) -> AbortOnDrop {
    let interval = opts.progress_interval;
    AbortOnDrop(tokio::spawn(async move {
        let Some(tx) = events else { return };
        let mut tick = tokio::time::interval(interval);
        let mut last = (Instant::now(), sample().0);
        loop {
            tick.tick().await;
            let (now_bytes, conns) = sample();
            let now = Instant::now();
            let dt = now.duration_since(last.0).as_secs_f64();
            let speed = if dt > 0.0 {
                (now_bytes.saturating_sub(last.1) as f64 / dt) as u64
            } else {
                0
            };
            last = (now, now_bytes);
            let _ = tx.send(Event::Progress {
                downloaded: now_bytes,
                size,
                speed,
                connections: conns,
            });
        }
    }))
}

async fn sequential(
    resp: &mut Response,
    file: &Arc<File>,
    received: &AtomicU64,
    limiters: &[Arc<RateLimiter>],
    cancel: &CancellationToken,
) -> anyhow::Result<u64> {
    let mut buf = Pending::default();
    let mut offset = 0u64;
    let mut last_flush = Instant::now();
    loop {
        let chunk = tokio::select! {
            c = resp.chunk() => c?,
            _ = cancel.cancelled() => bail!("cancelled"),
        };
        let done = chunk.is_none();
        if let Some(c) = chunk {
            received.fetch_add(c.len() as u64, Ordering::Relaxed);
            let n = c.len() as u64;
            buf.push(c);
            if limiters.iter().any(|l| l.rate() > 0) {
                tokio::select! {
                    _ = throttle(limiters, n) => {}
                    _ = cancel.cancelled() => bail!("cancelled"),
                }
            }
        }
        if !buf.is_empty() && (done || buf.len >= FLUSH_SIZE || last_flush.elapsed() >= FLUSH_AGE) {
            let n = buf.len as u64;
            write_at(file, buf.take(), offset).await?;
            offset += n;
            last_flush = Instant::now();
        }
        if done {
            return Ok(offset);
        }
    }
}

/// Chunks waiting to be written, in file order.
#[derive(Default)]
struct Pending {
    chunks: Vec<Bytes>,
    len: usize,
}

impl Pending {
    fn push(&mut self, b: Bytes) {
        if !b.is_empty() {
            self.len += b.len();
            self.chunks.push(b);
        }
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn take(&mut self) -> Vec<Bytes> {
        self.len = 0;
        std::mem::take(&mut self.chunks)
    }
}

async fn write_at(file: &Arc<File>, chunks: Vec<Bytes>, offset: u64) -> std::io::Result<()> {
    let file = file.clone();
    tokio::task::spawn_blocking(move || pwritev_all(&file, &chunks, offset))
        .await
        .map_err(std::io::Error::other)?
}

/// Writes all of `chunks` contiguously at `offset`, retrying short writes.
fn pwritev_all(file: &File, chunks: &[Bytes], mut offset: u64) -> std::io::Result<()> {
    use std::io::IoSlice;
    use std::os::fd::AsRawFd;
    const IOV_MAX: usize = 1024;
    let mut slices: Vec<IoSlice<'_>> = chunks.iter().map(|c| IoSlice::new(c)).collect();
    let mut rest = &mut slices[..];
    while !rest.is_empty() {
        let n = rest.len().min(IOV_MAX);
        // SAFETY: IoSlice is ABI-compatible with iovec on Unix, and the first
        // `n` entries borrow live buffers from `chunks`.
        let r = unsafe {
            libc::pwritev(
                file.as_raw_fd(),
                rest.as_ptr().cast::<libc::iovec>(),
                n as libc::c_int,
                offset as libc::off_t,
            )
        };
        if r < 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        if r == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        offset += r as u64;
        IoSlice::advance_slices(&mut rest, r as usize);
    }
    Ok(())
}

async fn worker(
    id: usize,
    client: Client,
    shared: Arc<Shared>,
    mut initial: Option<(usize, Response)>,
) -> anyhow::Result<()> {
    let r = worker_loop(id, &client, &shared, &mut initial).await;
    shared.plan.lock().unwrap().alive[id] = false;
    r
}

async fn worker_loop(
    id: usize,
    client: &Client,
    shared: &Arc<Shared>,
    initial: &mut Option<(usize, Response)>,
) -> anyhow::Result<()> {
    loop {
        if shared.cancel.is_cancelled() {
            return Ok(());
        }
        let (seg, mut resp) = match initial.take() {
            Some((seg, resp)) => (seg, Some(resp)),
            None => match shared.claim(id) {
                Claim::Work(seg) => (seg, None),
                Claim::Done => return Ok(()),
                Claim::Wait => {
                    tokio::select! {
                        _ = tokio::time::sleep(WAIT_POLL) => continue,
                        _ = shared.cancel.cancelled() => return Ok(()),
                    }
                }
            },
        };
        // Failures in a row without any new bytes written.
        let mut failures = 0u32;
        loop {
            let before = shared.seg(seg, |s| s.written);
            let err = match fetch_segment(id, client, shared, seg, resp.take()).await {
                Ok(()) => break,
                Err(SegError::Cancelled) => {
                    shared.leave(id, seg);
                    return Ok(());
                }
                Err(SegError::Fatal(e)) => {
                    shared.leave(id, seg);
                    return Err(e);
                }
                Err(e @ (SegError::Rejected(_) | SegError::Expired(_) | SegError::Unranged(_))) => {
                    let why = match e {
                        SegError::Rejected(why) => why,
                        SegError::Expired(s) => format!("server responded {s}"),
                        _ => "server ignored Range".into(),
                    };
                    // Back off from a server that limits connections, unless
                    // this is the last connection standing.
                    if shared.try_leave(id, seg) {
                        shared.emit(Event::Warning {
                            message: format!("connection {id} closed: {why}"),
                        });
                        return Ok(());
                    }
                    anyhow!(why)
                }
                Err(SegError::Retry(e)) => e,
            };
            if shared.seg(seg, |s| s.written) > before {
                failures = 0;
            }
            failures += 1;
            if failures > shared.max_retries {
                if shared.try_leave(id, seg) {
                    shared.emit(Event::Warning {
                        message: format!("connection {id} gave up: {err:#}"),
                    });
                    return Ok(());
                }
                shared.leave(id, seg);
                return Err(err.context("download failed"));
            }
            tokio::select! {
                _ = tokio::time::sleep(backoff(failures)) => {}
                _ = shared.cancel.cancelled() => {
                    shared.leave(id, seg);
                    return Ok(());
                }
            }
        }
        // A finished segment stays unowned; claim() skips it.
        shared.seg(seg, |s| s.owner = None);
    }
}

/// Streams one segment until its (possibly shrinking) end.
async fn fetch_segment(
    id: usize,
    client: &Client,
    shared: &Shared,
    seg: usize,
    resp: Option<Response>,
) -> Result<(), SegError> {
    let (mut from, to) = shared.seg(seg, |s| (s.fetched, s.end));
    if from >= to {
        return Ok(());
    }
    // Leading bytes of a whole-file response that are already on disk.
    let mut skip = 0u64;

    let mut resp = match resp {
        Some(r) => r,
        None => {
            let url = shared.url.read().unwrap().clone();
            let r = match request_range(id, client, shared, &url, from, to).await {
                // A signed or temporary redirect target has expired: resolve
                // the original URL again and keep using what it points to now.
                Err(SegError::Expired(_)) if url != shared.origin_url => {
                    let resp = request_range(id, client, shared, &shared.origin_url, from, to)
                        .await
                        .map_err(|e| e.expired_as_final())?;
                    if !same_resource(shared, &url, &resp) {
                        return Err(SegError::Fatal(anyhow!(
                            "{} now leads to a different file ({}); not mixing it in",
                            shared.origin_url,
                            resp.url()
                        )));
                    }
                    *shared.url.write().unwrap() = resp.url().to_string();
                    Ok(resp)
                }
                r => r,
            };
            match r {
                Ok(resp) => resp,
                Err(SegError::Unranged(resp)) => match shared.take_over_all(seg) {
                    Some(first) => {
                        shared.emit(Event::Warning {
                            message: "server ignored Range; finishing in one stream".into(),
                        });
                        from = first;
                        skip = first;
                        resp
                    }
                    None => return Err(SegError::Rejected("server ignored Range".into())),
                },
                Err(e) => return Err(e.expired_as_final()),
            }
        }
    };

    shared.plan.lock().unwrap().rates[id].start(Instant::now());
    let mut buf = Pending::default();
    let mut write_pos = from;
    let mut last_flush = Instant::now();
    let watch_speed = shared.lowest_speed_limit > 0;
    let throttled = || shared.limiters.iter().any(|l| l.rate() > 0);
    // (window start, bytes in window, whether a limit applied at any point)
    let mut window = (Instant::now(), 0u64, throttled());
    loop {
        let deadline = tokio::time::Instant::from_std(window.0 + shared.low_speed_window);
        let chunk = tokio::select! {
            c = resp.chunk() => c,
            _ = shared.cancel.cancelled() => {
                flush(shared, seg, &mut buf, &mut write_pos).await.map_err(SegError::Fatal)?;
                return Err(SegError::Cancelled);
            }
            _ = tokio::time::sleep_until(deadline), if watch_speed => {
                let speed = window.1 as f64 / window.0.elapsed().as_secs_f64();
                // A window that saw a rate limit measures the limit, not the link.
                let limited = window.2 || throttled();
                if !limited && speed < shared.lowest_speed_limit as f64 {
                    flush(shared, seg, &mut buf, &mut write_pos).await.map_err(SegError::Fatal)?;
                    shared.forget_rate(id);
                    return Err(SegError::Retry(anyhow!(
                        "connection too slow ({} B/s); reconnecting",
                        speed as u64
                    )));
                }
                window = (Instant::now(), 0, throttled());
                continue;
            }
        };
        let mut chunk = match chunk {
            Ok(Some(c)) => c,
            Ok(None) => {
                flush(shared, seg, &mut buf, &mut write_pos)
                    .await
                    .map_err(SegError::Fatal)?;
                if shared.seg(seg, |s| s.written < s.end) {
                    shared.forget_rate(id);
                    return Err(SegError::Retry(anyhow!("connection closed early")));
                }
                return Ok(());
            }
            Err(e) => {
                flush(shared, seg, &mut buf, &mut write_pos)
                    .await
                    .map_err(SegError::Fatal)?;
                shared.forget_rate(id);
                return Err(SegError::Retry(e.into()));
            }
        };
        if skip > 0 {
            let n = skip.min(chunk.len() as u64);
            skip -= n;
            window.1 += n;
            chunk = chunk.slice(n as usize..);
            if chunk.is_empty() {
                continue;
            }
        }
        let (take, done) = {
            let mut plan = shared.plan.lock().unwrap();
            let s = &mut plan.segs[seg];
            let take = (s.end - s.fetched).min(chunk.len() as u64);
            s.fetched += take;
            let done = s.fetched == s.end;
            plan.rates[id].add(take, Instant::now());
            (take as usize, done)
        };
        buf.push(chunk.slice(..take));
        window.1 += take as u64;
        // Checked per chunk, so the unlimited case must stay a few loads.
        if throttled() {
            window.2 = true;
            tokio::select! {
                _ = throttle(&shared.limiters, take as u64) => {}
                _ = shared.cancel.cancelled() => {
                    flush(shared, seg, &mut buf, &mut write_pos).await.map_err(SegError::Fatal)?;
                    return Err(SegError::Cancelled);
                }
            }
        }
        if done || buf.len >= FLUSH_SIZE || last_flush.elapsed() >= FLUSH_AGE {
            flush(shared, seg, &mut buf, &mut write_pos)
                .await
                .map_err(SegError::Fatal)?;
            last_flush = Instant::now();
        }
        if done {
            // Dropping `resp` here closes a connection whose range was cut
            // short; the next request goes out on a new one, whose speed is
            // not this one's.
            if shared.seg(seg, |s| s.end) < to {
                shared.forget_rate(id);
            }
            return Ok(());
        }
    }
}

async fn throttle(limiters: &[Arc<RateLimiter>], n: u64) {
    for l in limiters {
        l.acquire(n).await;
    }
}

/// Sends one Range request for `[from, to)` and checks the response is that
/// range of the same file.
async fn request_range(
    id: usize,
    client: &Client,
    shared: &Shared,
    url: &str,
    from: u64,
    to: u64,
) -> Result<Response, SegError> {
    let r = send_range(id, client, shared, url, from, to).await;
    // A refused or unusable response is dropped unread, which closes its
    // connection; so does a failed send.
    if matches!(&r, Err(e) if !matches!(e, SegError::Cancelled)) {
        shared.forget_rate(id);
    }
    r
}

async fn send_range(
    id: usize,
    client: &Client,
    shared: &Shared,
    url: &str,
    from: u64,
    to: u64,
) -> Result<Response, SegError> {
    let mut req = client
        .get(url)
        .header(header::RANGE, format!("bytes={}-{}", from, to - 1));
    if same_origin(url, &shared.origin_url) {
        req = req.headers(shared.secret_headers.clone());
    }
    if let Some(v) = &shared.validator {
        req = req.header(header::IF_RANGE, v);
    }
    let sent = Instant::now();
    let resp = tokio::select! {
        r = req.send() => r.map_err(|e| SegError::Retry(e.into()))?,
        _ = shared.cancel.cancelled() => return Err(SegError::Cancelled),
    };
    shared.observe_latency(id, sent.elapsed().as_secs_f64());
    match resp.status() {
        StatusCode::PARTIAL_CONTENT => {}
        StatusCode::OK => {
            // Ignoring If-Range answers 200 too, so only a response that is
            // still labelled as the same file counts as a mere refusal.
            let same = match (&shared.validator, validator(resp.headers())) {
                (Some(want), Some(got)) => *want == got,
                (Some(_), None) => false,
                // Nothing to compare: at least the length must be the one
                // the file had (a 206 is held to the same via its total).
                (None, _) => resp.content_length() == Some(shared.size),
            } && resp.content_length().is_none_or(|n| n == shared.size);
            // A changed file is refused (and fails once every connection has
            // been refused) without mixing anything in.
            return Err(if same {
                SegError::Unranged(resp)
            } else {
                SegError::Rejected("remote file changed (server ignored Range)".into())
            });
        }
        s @ (StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN | StatusCode::GONE) => {
            return Err(SegError::Expired(s));
        }
        s @ (StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE) => {
            return Err(SegError::Rejected(format!("server responded {s}")));
        }
        s if s.is_server_error() => {
            return Err(SegError::Retry(anyhow!("server responded {s}")));
        }
        s => return Err(SegError::Fatal(anyhow!("server responded {s}"))),
    }
    let (start, _, total) = content_range(&resp).map_err(SegError::Fatal)?;
    if start != from || total != Some(shared.size) {
        return Err(SegError::Fatal(anyhow!(
            "server returned an unexpected range (start {start}, total {total:?})"
        )));
    }
    // Servers that ignore If-Range still label what they send; a different
    // label means a different file, or a mirror that labels it differently.
    if let (Some(want), Some(got)) = (&shared.validator, validator(resp.headers())) {
        if *want != got {
            return Err(SegError::Rejected(format!(
                "remote file changed (validator {want} became {got})"
            )));
        }
    }
    Ok(resp)
}

/// Whether a response obtained by re-resolving the original URL is the file
/// already being downloaded. With a validator, `request_range` has checked
/// it matches. Without one, only a URL that differs from the expired one in
/// its query (a fresh signature for the same object) is trusted.
fn same_resource(shared: &Shared, expired: &str, resp: &Response) -> bool {
    if shared.validator.is_some() {
        return validator(resp.headers()).is_some();
    }
    let strip = |u: &str| u.split(['?', '#']).next().unwrap_or("").to_string();
    strip(expired) == strip(resp.url().as_str())
}

async fn flush(
    shared: &Shared,
    seg: usize,
    buf: &mut Pending,
    pos: &mut u64,
) -> anyhow::Result<()> {
    if buf.is_empty() {
        return Ok(());
    }
    let n = buf.len as u64;
    write_at(&shared.file, buf.take(), *pos)
        .await
        .context("write failed")?;
    *pos += n;
    shared.seg(seg, |s| s.written = *pos);
    Ok(())
}
