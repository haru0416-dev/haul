//! Misbehaving servers, credentials across redirects, and concurrent use of
//! one output path.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::http::{StatusCode, header};
use pdl::engine::{part_path, state_path};
use pdl::{Event, Options, download};
use pdl_testserver::{Config, fill, spawn_local};
use tokio_util::sync::CancellationToken;

mod common;
use common::{assert_file, opts, tmpdir};

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

/// Limited per connection, so several connections (and requests) are used.
fn credentials_server() -> Config {
    Config {
        size: 4 << 20,
        rate_per_conn: Some(4 << 20),
        ..Config::default()
    }
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
    let cfg = credentials_server();
    let srv = spawn_local(cfg.clone()).await.unwrap();
    let start = redirector(srv.url()).await;
    let out = tmpdir("creds").join("f.bin");
    download(
        with_credentials(opts(start, &out)),
        None,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_file(&out, cfg.size);
    assert!(
        srv.stats.requests.load(Ordering::SeqCst) > 1,
        "expected several range requests"
    );
    assert_eq!(
        srv.stats.credentialed.load(Ordering::SeqCst),
        0,
        "credentials reached the redirect target"
    );
}

#[tokio::test]
async fn credentials_still_reach_the_origin_itself() {
    let cfg = credentials_server();
    let srv = spawn_local(cfg.clone()).await.unwrap();
    let out = tmpdir("creds-same").join("f.bin");
    download(
        with_credentials(opts(srv.url(), &out)),
        None,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_file(&out, cfg.size);
    let all = srv.stats.requests.load(Ordering::SeqCst);
    assert!(all > 1);
    assert_eq!(
        srv.stats.credentialed.load(Ordering::SeqCst),
        all,
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
        // Slow per connection so that more requests follow the first one.
        rate_per_conn: Some(2 << 20),
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

/// A pause followed at once by a resume (the daemon does exactly this) must
/// not find the partial file still locked by the stopped download's tasks.
/// On one thread an aborted task lingers until the caller yields, so a
/// download that returns before its tasks are gone is caught every time.
#[tokio::test(flavor = "current_thread")]
async fn restart_right_after_cancel_finds_the_file_free() {
    let cfg = Config {
        size: 8 << 20,
        rate_per_conn: Some(1 << 20),
        ..Config::default()
    };
    let srv = spawn_local(cfg.clone()).await.unwrap();
    let out = tmpdir("quickresume").join("f.bin");
    for round in 0..50 {
        let cancel = CancellationToken::new();
        let c = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            c.cancel();
        });
        let r = download(opts(srv.url(), &out), None, cancel).await;
        // The moment it returns, nothing of it may hold the file.
        if let Ok(f) = std::fs::File::open(part_path(&out)) {
            use std::os::fd::AsRawFd;
            // SAFETY: valid fd for the lifetime of `f`.
            let locked = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            assert_eq!(
                locked, 0,
                "round {round}: partial file still locked after return"
            );
        }
        if let Err(e) = r {
            let e = format!("{e:#}");
            assert!(!e.contains("another process"), "round {round}: {e}");
        }
        if out.exists() {
            break;
        }
    }
}

/// Retries of the first request must not count as connection setup time:
/// that would make every new connection look too slow to be worth it.
#[tokio::test]
async fn retried_first_request_still_goes_parallel() {
    let cfg = Config {
        size: 24 << 20,
        rate_per_conn: Some(2 << 20),
        fail_requests: vec![1, 2, 3, 4],
        ..Config::default()
    };
    let srv = spawn_local(cfg.clone()).await.unwrap();
    let out = tmpdir("retried-parallel").join("f.bin");
    download(opts(srv.url(), &out), None, CancellationToken::new())
        .await
        .unwrap();
    assert_file(&out, cfg.size);
    let active = srv.stats.max_active.load(Ordering::SeqCst);
    assert!(active > 4, "stayed at {active} connection(s)");
}

/// On a path one connection already fills, more connections are tried,
/// found useless and actually closed again, also when the step reached the
/// maximum (which used to go unjudged).
#[tokio::test]
async fn useless_extra_connections_are_closed_again() {
    let cfg = Config {
        size: 3 << 20,
        // A shared 512 KiB/s link: one connection already gets all of it
        // (slow enough that the test server's per-chunk overhead is noise,
        // even with other tests loading the machine).
        link: Some(512 << 10),
        ..Config::default()
    };
    let srv = spawn_local(cfg.clone()).await.unwrap();
    let out = tmpdir("steer-back").join("f.bin");
    let mut o = opts(srv.url(), &out);
    o.max_conns = 4;
    o.progress_interval = Duration::from_millis(100);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    download(o, Some(tx), CancellationToken::new())
        .await
        .unwrap();
    assert_file(&out, cfg.size);
    let mut conns = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        if let Event::Progress { connections, .. } = ev {
            conns.push(connections);
        }
    }
    assert!(conns.iter().any(|&c| c > 1), "never tried more: {conns:?}");
    // The last second (10 samples) runs on one connection.
    let tail = &conns[conns.len().saturating_sub(10)..];
    assert!(
        tail.iter().all(|&c| c <= 1),
        "extra connections kept: {conns:?}"
    );
}

/// A connection that stops delivering without closing must have its range
/// taken over long before the read timeout notices it.
#[tokio::test]
async fn stalled_connection_is_rescued() {
    let cfg = Config {
        size: 16 << 20,
        rate_per_conn: Some(2 << 20),
        // The second response (the first after the probe's) stalls halfway.
        stall_requests: vec![2],
        ..Config::default()
    };
    let srv = spawn_local(cfg.clone()).await.unwrap();
    let out = tmpdir("stall").join("f.bin");
    let t = std::time::Instant::now();
    download(opts(srv.url(), &out), None, CancellationToken::new())
        .await
        .unwrap();
    assert_file(&out, cfg.size);
    // Well under the 30 s read timeout and the 60 s stall.
    assert!(
        t.elapsed() < Duration::from_secs(12),
        "took {:?}",
        t.elapsed()
    );
}

/// After `steer` went back to one connection, that one stalling must still
/// be rescued: a stalled worker must not hold the only slot.
#[tokio::test]
async fn stall_after_going_back_to_one_connection_is_rescued() {
    let cfg = Config {
        size: 4 << 20,
        link: Some(512 << 10),
        // Requests after the 1 -> 4 step and back stall without closing.
        stall_requests: vec![5, 6, 7],
        ..Config::default()
    };
    let srv = spawn_local(cfg.clone()).await.unwrap();
    let out = tmpdir("stall-after-back").join("f.bin");
    let mut o = opts(srv.url(), &out);
    o.max_conns = 4;
    o.read_timeout = Duration::from_secs(20);
    let t = std::time::Instant::now();
    download(o, None, CancellationToken::new()).await.unwrap();
    assert_file(&out, cfg.size);
    // 4 MiB at 512 KiB/s is 8 s; a stall waited out would add 20 s.
    assert!(
        t.elapsed() < Duration::from_secs(16),
        "took {:?}",
        t.elapsed()
    );
}
