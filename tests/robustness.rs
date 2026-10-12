//! Misbehaving servers, credentials across redirects, and concurrent use of
//! one output path.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::http::{HeaderMap, StatusCode, header};
use pdl::engine::{part_path, state_path};
use pdl::{Event, Options, download};
use pdl_testserver::{Config, fill, first_mismatch, spawn_local};
use tokio_util::sync::CancellationToken;

fn tmpdir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("pdl-robust-{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn opts(url: String, out: &Path) -> Options {
    let mut o = Options::new(url, out);
    o.min_split = 256 << 10;
    o.checkpoint_interval = Duration::from_millis(100);
    o
}

fn assert_file(path: &Path, size: u64) {
    let data = std::fs::read(path).unwrap();
    assert_eq!(data.len() as u64, size);
    assert_eq!(first_mismatch(&data), None);
    assert!(!part_path(path).exists());
    assert!(!state_path(path).exists());
}

/// Serves the test file with ranges and counts requests carrying credentials.
async fn counting_origin(size: u64) -> (String, Arc<[AtomicU64; 2]>) {
    // [requests, requests with Authorization or Cookie]
    let seen: Arc<[AtomicU64; 2]> = Arc::new([AtomicU64::new(0), AtomicU64::new(0)]);
    let s = seen.clone();
    let app = axum::Router::new().fallback(move |h: HeaderMap| {
        let seen = s.clone();
        async move {
            seen[0].fetch_add(1, Ordering::SeqCst);
            if h.contains_key(header::AUTHORIZATION) || h.contains_key(header::COOKIE) {
                seen[1].fetch_add(1, Ordering::SeqCst);
            }
            let (a, b) = h
                .get(header::RANGE)
                .and_then(|v| {
                    v.to_str()
                        .ok()?
                        .strip_prefix("bytes=")?
                        .split_once('-')
                        .map(|(a, b)| (a.to_string(), b.to_string()))
                })
                .map(|(a, b)| {
                    (
                        a.parse::<u64>().unwrap(),
                        if b.is_empty() {
                            size
                        } else {
                            b.parse::<u64>().unwrap() + 1
                        },
                    )
                })
                .unwrap_or((0, size));
            let mut body = vec![0u8; (b - a) as usize];
            fill(&mut body, a);
            axum::response::Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(header::ETAG, "\"e\"")
                .header(header::CONTENT_RANGE, format!("bytes {a}-{}/{size}", b - 1))
                .body(axum::body::Body::from(body))
                .unwrap()
        }
    });
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/f.bin", l.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    (url, seen)
}

async fn redirector(to: String) -> String {
    let app = axum::Router::new().fallback(move || {
        let to = to.clone();
        async move {
            axum::response::Response::builder()
                .status(StatusCode::FOUND)
                .header(header::LOCATION, to)
                .body(axum::body::Body::empty())
                .unwrap()
        }
    });
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/get", l.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    url
}

fn with_credentials(mut o: Options) -> Options {
    o.headers = vec![
        ("Authorization".into(), "Bearer SECRET".into()),
        ("Cookie".into(), "sid=SECRET".into()),
        ("X-Plain".into(), "kept".into()),
    ];
    o
}

#[tokio::test]
async fn credentials_stay_with_the_origin() {
    let size = 4 << 20;
    let (target, seen) = counting_origin(size).await;
    let start = redirector(target).await;
    let out = tmpdir("creds").join("f.bin");
    download(
        with_credentials(opts(start, &out)),
        None,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_file(&out, size);
    assert!(
        seen[0].load(Ordering::SeqCst) > 1,
        "expected several range requests"
    );
    assert_eq!(
        seen[1].load(Ordering::SeqCst),
        0,
        "credentials reached the redirect target"
    );
}

#[tokio::test]
async fn credentials_still_reach_the_origin_itself() {
    let size = 4 << 20;
    let (url, seen) = counting_origin(size).await;
    let out = tmpdir("creds-same").join("f.bin");
    download(
        with_credentials(opts(url, &out)),
        None,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_file(&out, size);
    let (all, with) = (
        seen[0].load(Ordering::SeqCst),
        seen[1].load(Ordering::SeqCst),
    );
    assert!(all > 1);
    assert_eq!(
        all, with,
        "every request to the origin needs the credentials"
    );
}

#[tokio::test]
async fn second_download_to_the_same_path_is_refused() {
    let cfg = Config {
        size: 4 << 20,
        rate_per_conn: Some(64 << 10),
        ..Config::default()
    };
    let srv = spawn_local(cfg.clone()).await.unwrap();
    let out = tmpdir("lock").join("f.bin");
    let first = tokio::spawn(download(
        opts(srv.url(), &out),
        None,
        CancellationToken::new(),
    ));
    tokio::time::sleep(Duration::from_millis(300)).await;
    let err = download(opts(srv.url(), &out), None, CancellationToken::new())
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("another process"), "{err:#}");
    first.await.unwrap().unwrap();
    assert_file(&out, cfg.size);
}

#[tokio::test]
async fn truncated_partial_file_restarts_instead_of_resuming() {
    let cfg = Config {
        size: 4 << 20,
        rate_per_conn: Some(64 << 10),
        ..Config::default()
    };
    let srv = spawn_local(cfg.clone()).await.unwrap();
    let out = tmpdir("truncated").join("f.bin");
    let cancel = CancellationToken::new();
    let task = tokio::spawn(download(opts(srv.url(), &out), None, cancel.clone()));
    tokio::time::sleep(Duration::from_millis(600)).await;
    cancel.cancel();
    assert!(task.await.unwrap().is_err());
    std::fs::OpenOptions::new()
        .write(true)
        .open(part_path(&out))
        .unwrap()
        .set_len(1000)
        .unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    download(opts(srv.url(), &out), Some(tx), CancellationToken::new())
        .await
        .unwrap();
    assert_file(&out, cfg.size);
    let mut resumed = None;
    while let Ok(ev) = rx.try_recv() {
        if let Event::Started { resumed_bytes, .. } = ev {
            resumed = Some(resumed_bytes);
        }
    }
    assert_eq!(resumed, Some(0));
}

#[tokio::test]
async fn out_of_range_checkpoint_is_ignored() {
    let cfg = Config {
        size: 1 << 20,
        ..Config::default()
    };
    let srv = spawn_local(cfg.clone()).await.unwrap();
    let out = tmpdir("badstate").join("f.bin");
    std::fs::write(part_path(&out), vec![0u8; cfg.size as usize]).unwrap();
    let state = serde_json::json!({
        "version": 1, "url": srv.url(), "size": cfg.size, "validator": "\"v1\"",
        "remaining": [[5, 3]],
    });
    std::fs::write(state_path(&out), state.to_string()).unwrap();
    download(opts(srv.url(), &out), None, CancellationToken::new())
        .await
        .unwrap();
    assert_file(&out, cfg.size);
}

#[tokio::test]
async fn mirror_with_another_etag_does_not_end_the_download() {
    let cfg = Config {
        size: 8 << 20,
        // Requests 2-4 hit a mirror labelling the file differently.
        mirror_requests: vec![2, 3, 4],
        ..Config::default()
    };
    let srv = spawn_local(cfg.clone()).await.unwrap();
    let out = tmpdir("mirror").join("f.bin");
    download(opts(srv.url(), &out), None, CancellationToken::new())
        .await
        .unwrap();
    assert_file(&out, cfg.size);
}

#[tokio::test]
async fn mirror_ignoring_if_range_is_not_mixed_in() {
    let cfg = Config {
        size: 8 << 20,
        mirror_requests: vec![2, 3, 4],
        ignore_if_range: true,
        ..Config::default()
    };
    let srv = spawn_local(cfg.clone()).await.unwrap();
    let out = tmpdir("mirror206").join("f.bin");
    download(opts(srv.url(), &out), None, CancellationToken::new())
        .await
        .unwrap();
    assert_file(&out, cfg.size);
}

#[tokio::test]
async fn changed_file_fails_without_mixing() {
    let cfg = Config {
        size: 8 << 20,
        change_etag_after: Some(1),
        ..Config::default()
    };
    let srv = spawn_local(cfg).await.unwrap();
    let out = tmpdir("changed").join("f.bin");
    let mut o = opts(srv.url(), &out);
    o.max_retries = 1;
    let err = download(o, None, CancellationToken::new())
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("changed"), "{err:#}");
    assert!(!out.exists());
}

#[tokio::test]
async fn server_that_stops_honouring_range_still_finishes() {
    let cfg = Config {
        size: 6 << 20,
        range_first_only: true,
        ..Config::default()
    };
    let srv = spawn_local(cfg.clone()).await.unwrap();
    let out = tmpdir("rangeonce").join("f.bin");
    download(opts(srv.url(), &out), None, CancellationToken::new())
        .await
        .unwrap();
    assert_file(&out, cfg.size);
    // Each refused connection may have had part of a 200 body pushed at it
    // (on loopback, up to whole socket buffers), plus one full stream: what
    // must not happen is a refetch per segment.
    let sent = srv.stats.bytes_sent.load(Ordering::SeqCst);
    assert!(sent <= (16 + 2) * cfg.size, "server sent {sent} bytes");
}

/// First answer: 200 without ranges, cut halfway. Every later answer: a 206
/// that covers only the first half. Taking that 206's length as the file
/// size would "complete" a truncated file.
#[tokio::test]
async fn restarted_stream_is_checked_against_the_whole_size() {
    let size: u64 = 2 << 20;
    let n = Arc::new(AtomicU64::new(0));
    let app = axum::Router::new().fallback(move || {
        let n = n.clone();
        async move {
            let first = n.fetch_add(1, Ordering::SeqCst) == 0;
            let half = size / 2;
            let mut body = vec![0u8; half as usize];
            fill(&mut body, 0);
            let b = axum::response::Response::builder();
            if first {
                // Promises the whole file, delivers half, then breaks.
                let s = futures_util::stream::iter(vec![
                    Ok(bytes::Bytes::from(body)),
                    Err(std::io::Error::other("cut")),
                ]);
                b.status(StatusCode::OK)
                    .header(header::CONTENT_LENGTH, size)
                    .body(axum::body::Body::from_stream(s))
                    .unwrap()
            } else {
                b.status(StatusCode::PARTIAL_CONTENT)
                    .header(
                        header::CONTENT_RANGE,
                        format!("bytes 0-{}/{size}", half - 1),
                    )
                    .body(axum::body::Body::from(body))
                    .unwrap()
            }
        }
    });
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/f.bin", l.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    let out = tmpdir("capped-restart").join("f.bin");
    let mut o = opts(url, &out);
    o.max_retries = 2;
    let r = download(o, None, CancellationToken::new()).await;
    let len = std::fs::metadata(&out).map(|m| m.len()).ok();
    assert!(
        r.is_err(),
        "a half file was reported complete: {r:?} len={len:?}"
    );
    assert!(!out.exists());
}

/// No validator; the probe's 206 is file A (4 MiB) and breaks halfway, then
/// every request gets a chunked 200 of a different, larger file B. Taking
/// that 200 for "the same file, ranges ignored" would splice B onto A.
#[tokio::test]
async fn unlabelled_whole_file_of_another_length_is_not_spliced_in() {
    let size: u64 = 4 << 20;
    let n = Arc::new(AtomicU64::new(0));
    let app = axum::Router::new().fallback(move || {
        let n = n.clone();
        async move {
            let b = axum::response::Response::builder();
            if n.fetch_add(1, Ordering::SeqCst) == 0 {
                let s = futures_util::stream::iter(vec![
                    Ok(bytes::Bytes::from(vec![0xAA; (size / 2) as usize])),
                    Err(std::io::Error::other("cut")),
                ]);
                b.status(StatusCode::PARTIAL_CONTENT)
                    .header(
                        header::CONTENT_RANGE,
                        format!("bytes 0-{}/{size}", size - 1),
                    )
                    .header(header::CONTENT_LENGTH, size)
                    .body(axum::body::Body::from_stream(s))
                    .unwrap()
            } else {
                // No Content-Length: chunked.
                let s = futures_util::stream::iter(
                    (0..8)
                        .map(|_| Ok::<_, std::io::Error>(bytes::Bytes::from(vec![0xBB; 1 << 20]))),
                );
                b.status(StatusCode::OK)
                    .body(axum::body::Body::from_stream(s))
                    .unwrap()
            }
        }
    });
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/f.bin", l.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    let out = tmpdir("splice").join("f.bin");
    let mut o = opts(url, &out);
    o.max_conns = 1;
    o.max_retries = 1;
    let r = download(o, None, CancellationToken::new()).await;
    assert!(r.is_err(), "spliced file reported complete: {r:?}");
    assert!(!out.exists());
}
