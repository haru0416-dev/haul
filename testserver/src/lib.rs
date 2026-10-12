//! Test origin for haul: serves a deterministic pseudo-random file and can
//! misbehave on demand (per-connection throttling, no Range support,
//! connection limits, dropped connections, changing ETag).

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{HeaderMap, Method, StatusCode, Uri, header};
use axum::response::Response;
use bytes::Bytes;
use futures_util::stream;
use tokio::net::TcpListener;
use tokio::time::Instant;

#[derive(Clone, Debug)]
pub struct Config {
    /// Size of the served file in bytes.
    pub size: u64,
    /// Bytes per second for each response body. `None` means unthrottled.
    pub rate_per_conn: Option<u64>,
    /// Honour `Range` requests.
    pub range: bool,
    /// Respond 503 when more than this many bodies are streaming at once.
    pub max_conns: Option<usize>,
    /// Abort every response body after sending this many bytes.
    pub drop_after: Option<u64>,
    /// After this many requests, the ETag changes (simulates a replaced file).
    pub change_etag_after: Option<u64>,
    /// Size of each body chunk.
    pub chunk: usize,
    /// Responses (1-based, counting only those that send a body) that are
    /// throttled to `slow_rate`, to simulate one bad connection.
    pub slow_requests: Vec<u64>,
    pub slow_rate: u64,
    /// Answer the download path with a redirect to a fresh "signed" URL that
    /// expires (403) after serving this many requests, like a CDN token.
    pub signed_redirect: Option<u64>,
    /// Send neither ETag nor Last-Modified.
    pub no_validator: bool,
    /// Requests (1-based, counting all) answered with 503.
    pub fail_requests: Vec<u64>,
    /// Responses (1-based, as `slow_requests`) whose body is cut halfway.
    pub cut_requests: Vec<u64>,
    /// Requests (1-based, counting all) served by a "mirror" that labels the
    /// same bytes with a different ETag.
    pub mirror_requests: Vec<u64>,
    /// Serve ranges even when If-Range does not match, as some servers do.
    pub ignore_if_range: bool,
    /// Honour Range on the first request only; answer later ones with 200.
    pub range_first_only: bool,
    /// Per-TCP-connection speeds, like network paths that differ: the k-th
    /// connection to make a request streams at `conn_rates[k % len]` bytes
    /// per second (overrides `rate_per_conn`).
    pub conn_rates: Vec<u64>,
    /// Delay before every response's headers (round trip plus server time).
    pub latency: Duration,
    /// Simulated TCP/TLS path with this round trip time (zero: off). A new
    /// connection waits two extra round trips (handshakes) before its first
    /// response, and its speed starts at ten segments per round trip,
    /// doubling every round trip (slow start) up to its per-connection
    /// rate; after an idle second the ramp starts over, as Linux does with
    /// tcp_slow_start_after_idle.
    pub rtt: Duration,
    /// Bottleneck shared by all responses, bytes per second (the client's
    /// link). Each chunk takes its turn on it.
    pub link: Option<u64>,
    /// Responses (1-based, as `slow_requests`) that stop sending after 64 KiB
    /// (or halfway, if shorter) for a minute without closing: a stalled
    /// connection.
    pub stall_requests: Vec<u64>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            size: 16 << 20,
            rate_per_conn: None,
            range: true,
            max_conns: None,
            drop_after: None,
            change_etag_after: None,
            chunk: 16 << 10,
            slow_requests: Vec::new(),
            slow_rate: 16 << 10,
            signed_redirect: None,
            no_validator: false,
            fail_requests: Vec::new(),
            cut_requests: Vec::new(),
            mirror_requests: Vec::new(),
            ignore_if_range: false,
            range_first_only: false,
            conn_rates: Vec::new(),
            latency: Duration::ZERO,
            rtt: Duration::ZERO,
            link: None,
            stall_requests: Vec::new(),
        }
    }
}

#[derive(Default, Debug)]
pub struct Stats {
    pub requests: AtomicU64,
    pub active: AtomicUsize,
    pub max_active: AtomicUsize,
    pub rejected: AtomicU64,
    pub bytes_sent: AtomicU64,
    /// Responses that carried a body.
    pub served: AtomicU64,
    pub redirects: AtomicU64,
    pub expired: AtomicU64,
    /// Requests that carried an Authorization or Cookie header.
    pub credentialed: AtomicU64,
}

struct State {
    config: Config,
    stats: Arc<Stats>,
    /// Remaining uses per signed URL id.
    signed: std::sync::Mutex<std::collections::HashMap<u64, u64>>,
    /// Client connections in the order they first made a request.
    conns: std::sync::Mutex<std::collections::HashMap<SocketAddr, Arc<Conn>>>,
    /// When the shared link is next free.
    link_free: tokio::sync::Mutex<Instant>,
}

struct Conn {
    index: usize,
    /// (start of the current slow-start ramp, end of the last send)
    ramp: std::sync::Mutex<(Instant, Instant)>,
    fresh: std::sync::atomic::AtomicBool,
}

impl State {
    fn conn(&self, peer: SocketAddr) -> Arc<Conn> {
        let mut conns = self.conns.lock().unwrap();
        let next = conns.len();
        conns
            .entry(peer)
            .or_insert_with(|| {
                let now = Instant::now();
                Arc::new(Conn {
                    index: next,
                    ramp: std::sync::Mutex::new((now, now)),
                    fresh: std::sync::atomic::AtomicBool::new(true),
                })
            })
            .clone()
    }
}

/// Byte of the served file at `offset`.
pub fn byte_at(offset: u64) -> u8 {
    (splitmix64(offset / 8) >> ((offset % 8) * 8)) as u8
}

/// Fill `buf` with the file contents starting at `offset`.
pub fn fill(buf: &mut [u8], offset: u64) {
    for (i, b) in buf.iter_mut().enumerate() {
        *b = byte_at(offset + i as u64);
    }
}

/// Returns the first offset in `data` (which should hold the whole file) that
/// differs from the expected contents, if any.
pub fn first_mismatch(data: &[u8]) -> Option<u64> {
    data.iter()
        .enumerate()
        .find(|&(i, &b)| b != byte_at(i as u64))
        .map(|(i, _)| i as u64)
}

fn splitmix64(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

pub struct Server {
    pub addr: SocketAddr,
    pub stats: Arc<Stats>,
}

impl Server {
    pub fn url(&self) -> String {
        format!("http://{}/file.bin", self.addr)
    }
}

/// Bind on `addr` and serve in a background task.
pub async fn spawn(config: Config, addr: SocketAddr) -> anyhow::Result<Server> {
    let listener = TcpListener::bind(addr).await?;
    let addr = listener.local_addr()?;
    let stats = Arc::new(Stats::default());
    let state = Arc::new(State {
        config,
        stats: stats.clone(),
        signed: Default::default(),
        conns: Default::default(),
        link_free: tokio::sync::Mutex::new(Instant::now()),
    });
    let app = Router::new().fallback(
        move |ConnectInfo(peer): ConnectInfo<SocketAddr>,
              method: Method,
              uri: Uri,
              headers: HeaderMap| {
            let state = state.clone();
            async move {
                let conn = state.conn(peer);
                let mut wait = state.config.latency;
                if conn.fresh.swap(false, Ordering::SeqCst) {
                    wait += 2 * state.config.rtt;
                }
                if !wait.is_zero() {
                    tokio::time::sleep(wait).await;
                }
                handle(&state, &conn, method, &uri, &headers)
            }
        },
    );
    tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await;
    });
    Ok(Server { addr, stats })
}

/// Bind on an ephemeral localhost port.
pub async fn spawn_local(config: Config) -> anyhow::Result<Server> {
    spawn(config, "127.0.0.1:0".parse().unwrap()).await
}

fn handle(
    state: &Arc<State>,
    conn: &Arc<Conn>,
    method: Method,
    uri: &Uri,
    headers: &HeaderMap,
) -> Response {
    let cfg = &state.config;
    let stats = &state.stats;
    let n = stats.requests.fetch_add(1, Ordering::SeqCst) + 1;
    if headers.contains_key(header::AUTHORIZATION) || headers.contains_key(header::COOKIE) {
        stats.credentialed.fetch_add(1, Ordering::SeqCst);
    }
    if cfg.fail_requests.contains(&n) {
        return Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(Body::empty())
            .unwrap();
    }
    if let Some(uses) = cfg.signed_redirect {
        let mut signed = state.signed.lock().unwrap();
        // Like S3 presigned URLs: same path, a fresh signature in the query.
        let sig = uri
            .query()
            .and_then(|q| q.split('&').find_map(|kv| kv.strip_prefix("sig=")))
            .and_then(|id| id.parse::<u64>().ok());
        match sig {
            Some(id) => match signed.get_mut(&id) {
                Some(left) if *left > 0 => *left -= 1,
                _ => {
                    stats.expired.fetch_add(1, Ordering::SeqCst);
                    return Response::builder()
                        .status(StatusCode::FORBIDDEN)
                        .body(Body::from("token expired"))
                        .unwrap();
                }
            },
            None => {
                let id = stats.redirects.fetch_add(1, Ordering::SeqCst) + 1;
                signed.insert(id, uses);
                return Response::builder()
                    .status(StatusCode::FOUND)
                    .header(header::LOCATION, format!("{}?sig={id}", uri.path()))
                    .body(Body::empty())
                    .unwrap();
            }
        }
    }
    let etag = match cfg.change_etag_after {
        Some(after) if n > after => "\"v2\"",
        _ if cfg.mirror_requests.contains(&n) => "\"mirror\"",
        _ => "\"v1\"",
    };

    let mut range = if cfg.range && !(cfg.range_first_only && n > 1) {
        headers
            .get(header::RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| parse_range(v, cfg.size))
    } else {
        None
    };
    if let Some(if_range) = headers
        .get(header::IF_RANGE)
        .filter(|_| !cfg.ignore_if_range)
        && if_range.as_bytes() != etag.as_bytes()
    {
        range = None;
    }

    let (status, start, end) = match range {
        Some(Ok((s, e))) => (StatusCode::PARTIAL_CONTENT, s, e),
        Some(Err(())) => {
            return Response::builder()
                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                .header(header::CONTENT_RANGE, format!("bytes */{}", cfg.size))
                .body(Body::empty())
                .unwrap();
        }
        None => (StatusCode::OK, 0, cfg.size),
    };

    let mut builder = Response::builder()
        .status(status)
        .header(header::CONTENT_LENGTH, end - start)
        .header(header::CONTENT_TYPE, "application/octet-stream");
    if !cfg.no_validator {
        builder = builder.header(header::ETAG, etag);
    }
    if cfg.range {
        builder = builder.header(header::ACCEPT_RANGES, "bytes");
    }
    if status == StatusCode::PARTIAL_CONTENT {
        builder = builder.header(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", start, end - 1, cfg.size),
        );
    }
    if method == Method::HEAD {
        return builder.body(Body::empty()).unwrap();
    }

    let active = stats.active.fetch_add(1, Ordering::SeqCst) + 1;
    let guard = ActiveGuard(state.stats.clone());
    if cfg.max_conns.is_some_and(|max| active > max) {
        stats.rejected.fetch_add(1, Ordering::SeqCst);
        drop(guard);
        return Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(Body::empty())
            .unwrap();
    }
    stats.max_active.fetch_max(active, Ordering::SeqCst);
    let served = stats.served.fetch_add(1, Ordering::SeqCst) + 1;
    let rate = if cfg.slow_requests.contains(&served) {
        Some(cfg.slow_rate)
    } else if !cfg.conn_rates.is_empty() {
        Some(cfg.conn_rates[conn.index % cfg.conn_rates.len()])
    } else {
        cfg.rate_per_conn
    };

    let body = BodyState {
        pos: start,
        end,
        sent: 0,
        began: Instant::now(),
        rate,
        drop_after: if cfg.cut_requests.contains(&served) {
            Some((end - start) / 2)
        } else {
            cfg.drop_after
        },
        chunk: cfg.chunk,
        stats: state.stats.clone(),
        _guard: guard,
        next: Instant::now(),
        state: state.clone(),
        conn: conn.clone(),
        stall_at: cfg
            .stall_requests
            .contains(&served)
            .then(|| ((end - start) / 2).min(64 << 10)),
    };
    let s = stream::unfold(body, |mut b| async move {
        if b.pos >= b.end {
            return None;
        }
        if b.drop_after.is_some_and(|d| b.sent >= d) {
            return Some((Err(std::io::Error::other("dropped")), b));
        }
        if b.stall_at.is_some_and(|at| b.sent >= at) {
            b.stall_at = None;
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
        let len = (b.end - b.pos).min(b.chunk as u64) as usize;
        let rtt = b.state.config.rtt;
        if !rtt.is_zero() {
            // Slow start: ten 1460-byte segments per round trip, doubling.
            let now = Instant::now();
            let ramp = {
                let mut r = b.conn.ramp.lock().unwrap();
                if now.duration_since(r.1) > Duration::from_secs(1) {
                    r.0 = now;
                }
                r.1 = now;
                now.duration_since(r.0)
            };
            let doublings = (ramp.as_secs_f64() / rtt.as_secs_f64()).min(40.0);
            let mut rate = 14600.0 / rtt.as_secs_f64() * doublings.exp2();
            if let Some(cap) = b.rate {
                rate = rate.min(cap as f64);
            }
            // Catch up after oversleeping (timers tick in milliseconds),
            // but do not bank time spent idle.
            if b.next + Duration::from_millis(50) < now {
                b.next = now;
            }
            b.next += Duration::from_secs_f64(len as f64 / rate);
            // Sub-millisecond sleeps would round up to a whole tick.
            if b.next > now + Duration::from_millis(1) {
                tokio::time::sleep_until(b.next).await;
            }
        } else if let Some(rate) = b.rate {
            let due = b.began + Duration::from_secs_f64((b.sent + len as u64) as f64 / rate as f64);
            tokio::time::sleep_until(due).await;
        }
        if let Some(link) = b.state.config.link {
            let at = {
                let mut free = b.state.link_free.lock().await;
                *free =
                    (*free).max(Instant::now()) + Duration::from_secs_f64(len as f64 / link as f64);
                *free
            };
            if at > Instant::now() + Duration::from_millis(1) {
                tokio::time::sleep_until(at).await;
            }
        }
        let mut buf = vec![0u8; len];
        fill(&mut buf, b.pos);
        b.pos += len as u64;
        b.sent += len as u64;
        b.stats.bytes_sent.fetch_add(len as u64, Ordering::Relaxed);
        Some((Ok::<_, std::io::Error>(Bytes::from(buf)), b))
    });
    builder.body(Body::from_stream(s)).unwrap()
}

struct BodyState {
    pos: u64,
    end: u64,
    sent: u64,
    began: Instant,
    rate: Option<u64>,
    drop_after: Option<u64>,
    chunk: usize,
    stats: Arc<Stats>,
    _guard: ActiveGuard,
    next: Instant,
    state: Arc<State>,
    conn: Arc<Conn>,
    stall_at: Option<u64>,
}

struct ActiveGuard(Arc<Stats>);

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Parses a single `bytes=` range into a half-open interval.
fn parse_range(v: &str, size: u64) -> Option<Result<(u64, u64), ()>> {
    let spec = v.strip_prefix("bytes=")?;
    if spec.contains(',') {
        return None;
    }
    let (a, b) = spec.split_once('-')?;
    let (start, end) = if a.is_empty() {
        let n: u64 = b.parse().ok()?;
        (size.saturating_sub(n), size)
    } else {
        let s: u64 = a.parse().ok()?;
        let e = if b.is_empty() {
            size
        } else {
            b.parse::<u64>().ok()?.saturating_add(1).min(size)
        };
        (s, e)
    };
    if start >= end {
        return Some(Err(()));
    }
    Some(Ok((start, end)))
}
