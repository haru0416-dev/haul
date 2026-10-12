//! A download must never be assembled from two different files, even when
//! re-resolving an expired URL lands somewhere else.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::http::{HeaderMap, StatusCode, header};
use axum::response::Response;
use axum::routing::get;
use pdl::{Options, download};
use pdl_testserver::{Config, first_mismatch, spawn_local};
use tokio_util::sync::CancellationToken;

mod common;
use common::tmpdir;

const SIZE: u64 = 4 << 20;

/// A 206 response filled with `fill`, optionally labelled with an ETag.
/// If-Range is deliberately ignored, as some servers do.
fn ranged(fill: u8, headers: &HeaderMap, etag: Option<&'static str>) -> Response {
    let spec = headers[header::RANGE]
        .to_str()
        .unwrap()
        .strip_prefix("bytes=")
        .unwrap()
        .to_string();
    let (a, b) = spec.split_once('-').unwrap();
    let start: u64 = a.parse().unwrap();
    let end: u64 = if b.is_empty() {
        SIZE
    } else {
        b.parse::<u64>().unwrap() + 1
    };
    let body = futures_util::stream::unfold(start, move |pos| async move {
        if pos >= end {
            return None;
        }
        let n = (end - pos).min(16 << 10);
        tokio::time::sleep(Duration::from_millis(2)).await;
        Some((
            Ok::<_, std::io::Error>(bytes::Bytes::from(vec![fill; n as usize])),
            pos + n,
        ))
    });
    let mut r = Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(header::CONTENT_LENGTH, end - start)
        .header(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", start, end - 1, SIZE),
        );
    if let Some(e) = etag {
        r = r.header(header::ETAG, e);
    }
    r.body(axum::body::Body::from_stream(body)).unwrap()
}

/// `/orig` first redirects to `/a`, which expires after three requests;
/// afterwards it redirects to `/b`, a different file of the same size.
async fn switching_origin(etags: Option<(&'static str, &'static str)>) -> String {
    let resolved = Arc::new(AtomicU64::new(0));
    let a_used = Arc::new(AtomicU64::new(0));
    let app = axum::Router::new()
        .route(
            "/orig",
            get(move || {
                let n = resolved.fetch_add(1, Ordering::SeqCst);
                async move {
                    Response::builder()
                        .status(StatusCode::FOUND)
                        .header(header::LOCATION, if n == 0 { "/a" } else { "/b" })
                        .body(axum::body::Body::empty())
                        .unwrap()
                }
            }),
        )
        .route(
            "/a",
            get(move |h: HeaderMap| {
                let n = a_used.fetch_add(1, Ordering::SeqCst);
                async move {
                    if n >= 3 {
                        return Response::builder()
                            .status(StatusCode::FORBIDDEN)
                            .body(axum::body::Body::empty())
                            .unwrap();
                    }
                    ranged(0xAA, &h, etags.map(|e| e.0))
                }
            }),
        )
        .route(
            "/b",
            get(move |h: HeaderMap| async move { ranged(0xBB, &h, etags.map(|e| e.1)) }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}/orig")
}

async fn assert_never_mixed(url: String, name: &str) {
    let out = tmpdir(name).join("f.bin");
    let mut o = Options::new(url, &out);
    o.max_conns = 8;
    o.min_split = 128 << 10;
    let result = download(o, None, CancellationToken::new()).await;
    if result.is_ok() {
        let data = std::fs::read(&out).unwrap();
        let foreign = data.iter().filter(|&&b| b != 0xAA).count();
        panic!("download succeeded with {foreign} bytes from another file");
    }
    assert!(!out.exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reresolving_to_another_path_without_validator_fails() {
    assert_never_mixed(switching_origin(None).await, "mix-novalidator").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ignored_if_range_with_other_etag_fails() {
    assert_never_mixed(switching_origin(Some(("\"A\"", "\"B\""))).await, "mix-etag").await;
}

/// Without any validator, a fresh signature for the same path is still
/// accepted, so presigned-URL downloads keep working.
#[tokio::test]
async fn resigned_same_path_without_validator_succeeds() {
    let cfg = Config {
        size: 8 << 20,
        signed_redirect: Some(3),
        drop_after: Some(512 << 10),
        no_validator: true,
        ..Config::default()
    };
    let srv = spawn_local(cfg.clone()).await.unwrap();
    let out = tmpdir("resign").join("f.bin");
    let mut o = Options::new(srv.url(), &out);
    o.min_split = 256 << 10;
    download(o, None, CancellationToken::new()).await.unwrap();
    let data = std::fs::read(&out).unwrap();
    assert_eq!(data.len() as u64, cfg.size);
    assert_eq!(first_mismatch(&data), None);
    assert!(srv.stats.expired.load(Ordering::SeqCst) > 0);
}

/// Lifting a tiny limit must release connections already sleeping off debt.
#[tokio::test]
async fn lifting_a_tiny_limit_takes_effect_at_once() {
    let cfg = Config {
        size: SIZE,
        ..Config::default()
    };
    let srv = spawn_local(cfg).await.unwrap();
    let out = tmpdir("lift").join("f.bin");
    let limiter = Arc::new(pdl::ratelimit::RateLimiter::new(4 << 10));
    let mut o = Options::new(srv.url(), &out);
    o.limiters = vec![limiter.clone()];
    let task = tokio::spawn(download(o, None, CancellationToken::new()));
    tokio::time::sleep(Duration::from_secs(2)).await;
    let lifted = std::time::Instant::now();
    limiter.set_rate(0);
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("still sleeping off debt from the old limit")
        .unwrap()
        .unwrap();
    assert!(
        lifted.elapsed() < Duration::from_secs(3),
        "{:?}",
        lifted.elapsed()
    );
}
