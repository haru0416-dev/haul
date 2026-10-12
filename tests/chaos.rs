//! Randomised misbehaving server (503s, dropped and capped responses, stalls,
//! 200 answers to range requests) plus random cancels, resumes and rate
//! changes; every finished file must match byte for byte.
//!
//! Slow, so ignored by default:
//! `CHAOS_N=400 cargo test --release --test chaos -- --ignored --nocapture`
//! (`CHAOS_BASE` picks the first seed).
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::http::{HeaderMap, StatusCode, header};
use pdl::ratelimit::RateLimiter;
use pdl_testserver::{byte_at, fill};
use tokio_util::sync::CancellationToken;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }
    fn chance(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }
}

#[derive(Clone, Copy, Debug)]
struct Chaos {
    p503: u64,
    pdrop: u64,
    pcap: u64,
    pstall: u64,
    p200: u64,
}

struct Srv {
    size: u64,
    seed: AtomicU64,
    chaos: Chaos,
    reqs: AtomicU64,
}

fn handle(st: &Srv, h: &HeaderMap) -> axum::response::Response {
    let mut rng = Rng(st.seed.fetch_add(0x1234_5678_9abc, Ordering::SeqCst));
    st.reqs.fetch_add(1, Ordering::SeqCst);
    let c = st.chaos;
    let size = st.size;
    let resp = axum::response::Response::builder();
    if rng.chance(c.p503) {
        return resp
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(axum::body::Body::empty())
            .unwrap();
    }
    let range = h
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("bytes="))
        .map(|s| {
            let (a, b) = s.split_once('-').unwrap();
            let a: u64 = a.parse().unwrap();
            let b: u64 = if b.is_empty() {
                size
            } else {
                b.parse::<u64>().unwrap() + 1
            };
            (a, b.min(size))
        });
    let (status, start, mut end) = match range {
        Some((a, _)) if a >= size => {
            return resp
                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                .header(header::CONTENT_RANGE, format!("bytes */{size}"))
                .body(axum::body::Body::empty())
                .unwrap();
        }
        // Occasionally answer a range request with the whole file (allowed by RFC).
        Some(_) if rng.chance(c.p200) => (StatusCode::OK, 0, size),
        Some((a, b)) => (StatusCode::PARTIAL_CONTENT, a, b),
        None => (StatusCode::OK, 0, size),
    };
    if status == StatusCode::PARTIAL_CONTENT && rng.chance(c.pcap) && end - start > 1 {
        // CDN-style cap: serve less than asked.
        end = start + 1 + rng.below(end - start - 1);
    }
    let drop_at = if rng.chance(c.pdrop) {
        Some(rng.below(end - start + 1))
    } else {
        None
    };
    let stall = rng.chance(c.pstall);
    let mut b = resp
        .status(status)
        .header(header::ETAG, "\"x\"")
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, end - start);
    if status == StatusCode::PARTIAL_CONTENT {
        b = b.header(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", start, end - 1, size),
        );
    }
    let chunk_seed = rng.next();
    let s = futures_util::stream::unfold(
        (start, 0u64, Rng(chunk_seed), stall),
        move |(pos, sent, mut r, stall)| async move {
            if pos >= end {
                return None;
            }
            if let Some(d) = drop_at {
                if sent >= d {
                    return Some((Err(std::io::Error::other("drop")), (end, sent, r, stall)));
                }
            }
            if stall && sent > 0 && r.chance(5) {
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            let n = (1 + r.below(40 << 10)).min(end - pos);
            let n = match drop_at {
                Some(d) if sent + n > d => (d - sent).max(1).min(n),
                _ => n,
            };
            if r.chance(10) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            let mut buf = vec![0u8; n as usize];
            fill(&mut buf, pos);
            Some((
                Ok::<_, std::io::Error>(bytes::Bytes::from(buf)),
                (pos + n, sent + n, r, stall),
            ))
        },
    );
    b.body(axum::body::Body::from_stream(s)).unwrap()
}

async fn serve(size: u64, chaos: Chaos, seed: u64) -> (String, Arc<Srv>) {
    let st = Arc::new(Srv {
        size,
        seed: AtomicU64::new(seed),
        chaos,
        reqs: AtomicU64::new(0),
    });
    let s2 = st.clone();
    let app = axum::Router::new().fallback(move |h: HeaderMap| {
        let st = s2.clone();
        async move { handle(&st, &h) }
    });
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    (format!("http://{addr}/f.bin"), st)
}

fn tmpdir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("chaos-{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn check(path: &Path, size: u64) -> Result<(), String> {
    let data = std::fs::read(path).map_err(|e| e.to_string())?;
    if data.len() as u64 != size {
        return Err(format!("size {} != {size}", data.len()));
    }
    for (i, &b) in data.iter().enumerate() {
        if b != byte_at(i as u64) {
            return Err(format!("mismatch at {i}"));
        }
    }
    Ok(())
}

async fn one(iter: u64, dir: &Path) -> Result<String, String> {
    let mut r = Rng(iter.wrapping_mul(0xA24B_AED4_963E_E407) ^ 0x5555);
    let size = match r.below(4) {
        0 => r.below(64),
        1 => r.below(256 << 10),
        _ => r.below(6 << 20),
    };
    let chaos = Chaos {
        p503: r.below(15),
        pdrop: r.below(40),
        pcap: r.below(30),
        pstall: r.below(20),
        p200: if r.chance(35) { r.below(60) } else { 0 },
    };
    let (url, st) = serve(size, chaos, r.next()).await;
    let out = dir.join(format!("f{iter}.bin"));
    let conns = 1 + r.below(24) as usize;
    let min_split = 1 + r.below(512 << 10);
    let limiter = Arc::new(RateLimiter::new(if r.chance(20) { 2 << 20 } else { 0 }));
    let mut attempts = 0;
    let mut last_err = String::new();
    loop {
        attempts += 1;
        if attempts > 40 {
            return Err(format!(
                "gave up: size={size} chaos={chaos:?} conns={conns} split={min_split} last={last_err}"
            ));
        }
        let mut o = pdl::Options::new(url.clone(), &out);
        o.max_conns = conns;
        o.min_split = min_split;
        o.max_retries = 3;
        o.checkpoint_interval = Duration::from_millis(1 + r.below(300));
        o.read_timeout = Duration::from_secs(5);
        o.limiters = vec![limiter.clone()];
        let cancel = CancellationToken::new();
        let cancel_after = if r.chance(50) {
            Some(Duration::from_millis(r.below(400)))
        } else {
            None
        };
        let c2 = cancel.clone();
        let flip = limiter.clone();
        let flip_rate = if r.chance(30) {
            Some(if r.chance(30) {
                0
            } else {
                (512 << 10) + r.below(4 << 20)
            })
        } else {
            None
        };
        let t = tokio::spawn(async move {
            if let Some(d) = cancel_after {
                tokio::time::sleep(d).await;
                c2.cancel();
            }
            if let Some(rate) = flip_rate {
                flip.set_rate(rate);
            }
        });
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<pdl::Event>();
        let log = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let l2 = log.clone();
        let t0 = std::time::Instant::now();
        let collector = tokio::spawn(async move {
            while let Some(ev) = rx.recv().await {
                l2.lock().unwrap().push(format!(
                    "{:>7.2}s {}",
                    t0.elapsed().as_secs_f64(),
                    serde_json::to_string(&ev).unwrap()
                ));
            }
        });
        let res =
            tokio::time::timeout(Duration::from_secs(60), pdl::download(o, Some(tx), cancel)).await;
        t.abort();
        collector.abort();
        match res {
            Err(_) => {
                let log = log.lock().unwrap();
                let tail: Vec<_> = log.iter().rev().take(40).rev().cloned().collect();
                return Err(format!(
                    "HANG >60s: size={size} chaos={chaos:?} conns={conns} split={min_split} reqs={}\n{}",
                    st.reqs.load(Ordering::SeqCst),
                    tail.join("\n")
                ));
            }
            Ok(Ok(s)) => {
                if s.size != size {
                    return Err(format!("summary size {} != {size}", s.size));
                }
                check(&out, size).map_err(|e| format!("CORRUPT {e}: size={size} chaos={chaos:?} conns={conns} split={min_split} attempts={attempts}"))?;
                let sent = st.reqs.load(Ordering::SeqCst);
                return Ok(format!("ok size={size} attempts={attempts} reqs={sent}"));
            }
            Ok(Err(e)) => {
                last_err = format!("{e:#}");
                if cancel_after.is_none() {
                    eprintln!("    [{iter}] attempt {attempts} err: {last_err}");
                }
                if out.exists() {
                    return Err(format!("error but output exists: {last_err}"));
                }
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn chaos() {
    let dir = tmpdir("run");
    let n: u64 = std::env::var("CHAOS_N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);
    let base: u64 = std::env::var("CHAOS_BASE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let mut bad = Vec::new();
    for i in base..base + n {
        match one(i, &dir).await {
            Ok(s) => eprintln!("[{i}] {s}"),
            Err(e) => {
                eprintln!("[{i}] FAIL {e}");
                bad.push((i, e));
            }
        }
    }
    assert!(bad.is_empty(), "{} failures: {bad:#?}", bad.len());
}
