use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::Duration;

use pdl::engine::{part_path, state_path};
use pdl::{Event, Options, download};
use pdl_testserver::{Config, first_mismatch, spawn_local};
use tokio_util::sync::CancellationToken;

fn tmpdir(name: &str) -> PathBuf {
    // On disk under target/, not a RAM-backed /tmp.
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("pdl-test-{name}"));
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

#[tokio::test]
async fn segmented_download_is_exact() {
    // Limited per connection, so more connections pay off and get used.
    let cfg = Config {
        size: (8 << 20) + 12345,
        rate_per_conn: Some(2 << 20),
        ..Config::default()
    };
    let srv = spawn_local(cfg.clone()).await.unwrap();
    let out = tmpdir("segmented").join("f.bin");
    download(opts(srv.url(), &out), None, CancellationToken::new())
        .await
        .unwrap();
    assert_file(&out, cfg.size);
    assert!(srv.stats.max_active.load(Ordering::SeqCst) > 1);
}

#[tokio::test]
async fn no_range_falls_back_to_single_stream() {
    let cfg = Config {
        size: 3 << 20,
        range: false,
        ..Config::default()
    };
    let srv = spawn_local(cfg.clone()).await.unwrap();
    let out = tmpdir("norange").join("f.bin");
    download(opts(srv.url(), &out), None, CancellationToken::new())
        .await
        .unwrap();
    assert_file(&out, cfg.size);
    assert_eq!(srv.stats.requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn tiny_file_uses_one_request() {
    let cfg = Config {
        size: 1000,
        ..Config::default()
    };
    let srv = spawn_local(cfg.clone()).await.unwrap();
    let out = tmpdir("tiny").join("f.bin");
    download(opts(srv.url(), &out), None, CancellationToken::new())
        .await
        .unwrap();
    assert_file(&out, cfg.size);
    assert_eq!(srv.stats.requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn survives_dropped_connections() {
    let cfg = Config {
        size: 6 << 20,
        drop_after: Some(300 << 10),
        ..Config::default()
    };
    let srv = spawn_local(cfg.clone()).await.unwrap();
    let out = tmpdir("drop").join("f.bin");
    download(opts(srv.url(), &out), None, CancellationToken::new())
        .await
        .unwrap();
    assert_file(&out, cfg.size);
}

#[tokio::test]
async fn adapts_to_connection_limit() {
    let cfg = Config {
        size: 8 << 20,
        max_conns: Some(3),
        rate_per_conn: Some(8 << 20),
        ..Config::default()
    };
    let srv = spawn_local(cfg.clone()).await.unwrap();
    let out = tmpdir("limit").join("f.bin");
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    download(opts(srv.url(), &out), Some(tx), CancellationToken::new())
        .await
        .unwrap();
    assert_file(&out, cfg.size);
    assert!(srv.stats.rejected.load(Ordering::SeqCst) > 0);
    let mut warned = false;
    while let Ok(ev) = rx.try_recv() {
        warned |= matches!(ev, Event::Warning { .. });
    }
    assert!(warned);
}

#[tokio::test]
async fn changed_remote_file_is_an_error() {
    // Slow per connection so that more requests follow the first one.
    let cfg = Config {
        size: 8 << 20,
        change_etag_after: Some(1),
        rate_per_conn: Some(2 << 20),
        ..Config::default()
    };
    let srv = spawn_local(cfg).await.unwrap();
    let out = tmpdir("etag").join("f.bin");
    let err = download(opts(srv.url(), &out), None, CancellationToken::new())
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("changed"), "{err:#}");
    assert!(!out.exists());
}

#[tokio::test]
async fn resumes_after_cancel_without_refetching() {
    let cfg = Config {
        size: 8 << 20,
        rate_per_conn: Some(256 << 10),
        ..Config::default()
    };
    let srv = spawn_local(cfg.clone()).await.unwrap();
    let out = tmpdir("resume").join("f.bin");

    let cancel = CancellationToken::new();
    let task = tokio::spawn(download(opts(srv.url(), &out), None, cancel.clone()));
    tokio::time::sleep(Duration::from_millis(700)).await;
    cancel.cancel();
    assert!(task.await.unwrap().is_err());
    assert!(state_path(&out).exists());
    let sent_first = srv.stats.bytes_sent.load(Ordering::SeqCst);
    assert!(sent_first > 0 && sent_first < cfg.size);

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    download(opts(srv.url(), &out), Some(tx), CancellationToken::new())
        .await
        .unwrap();
    assert_file(&out, cfg.size);

    let mut resumed = 0;
    while let Ok(ev) = rx.try_recv() {
        if let Event::Started { resumed_bytes, .. } = ev {
            resumed = resumed_bytes;
        }
    }
    assert!(resumed > 0);
    // Only the probe's in-flight bytes and unflushed buffers may be refetched.
    let sent_total = srv.stats.bytes_sent.load(Ordering::SeqCst);
    let refetched = sent_total - cfg.size;
    assert!(refetched < 4 << 20, "refetched {refetched} bytes");
}

#[tokio::test]
async fn empty_file() {
    let srv = spawn_local(Config {
        size: 0,
        ..Config::default()
    })
    .await
    .unwrap();
    let out = tmpdir("empty").join("f.bin");
    download(opts(srv.url(), &out), None, CancellationToken::new())
        .await
        .unwrap();
    assert_file(&out, 0);
}

/// A checkpoint left over from an earlier attempt must not survive a fresh
/// start, or a crash before the first new checkpoint would resume over zeros.
#[tokio::test]
async fn fresh_start_discards_stale_checkpoint() {
    let cfg = Config {
        size: 8 << 20,
        rate_per_conn: Some(256 << 10),
        ..Config::default()
    };
    let srv = spawn_local(cfg).await.unwrap();
    let out = tmpdir("stale").join("f.bin");

    let cancel = CancellationToken::new();
    let task = tokio::spawn(download(opts(srv.url(), &out), None, cancel.clone()));
    tokio::time::sleep(Duration::from_millis(500)).await;
    cancel.cancel();
    assert!(task.await.unwrap().is_err());
    assert!(state_path(&out).exists());
    std::fs::remove_file(part_path(&out)).unwrap();

    let mut o = opts(srv.url(), &out);
    o.checkpoint_interval = Duration::from_secs(3600);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let cancel = CancellationToken::new();
    let task = tokio::spawn(download(o, Some(tx), cancel.clone()));
    while let Some(ev) = rx.recv().await {
        if let Event::Started { resumed_bytes, .. } = ev {
            assert_eq!(resumed_bytes, 0);
            break;
        }
    }
    assert!(!state_path(&out).exists());
    cancel.cancel();
    let _ = task.await;
}

/// The probe response must not keep holding a connection while resuming.
#[tokio::test]
async fn resume_under_single_connection_limit() {
    let cfg = Config {
        size: 4 << 20,
        rate_per_conn: Some(1 << 20),
        max_conns: Some(1),
        ..Config::default()
    };
    let srv = spawn_local(cfg.clone()).await.unwrap();
    let out = tmpdir("resume1").join("f.bin");
    let cancel = CancellationToken::new();
    let task = tokio::spawn(download(opts(srv.url(), &out), None, cancel.clone()));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    cancel.cancel();
    assert!(task.await.unwrap().is_err());
    download(opts(srv.url(), &out), None, CancellationToken::new())
        .await
        .unwrap();
    assert_file(&out, cfg.size);
}

/// A checkpoint save still in flight when the download finishes must not
/// recreate the state file next to the finished output.
#[tokio::test]
async fn no_state_left_after_success_with_frequent_checkpoints() {
    let cfg = Config {
        size: 16 << 20,
        ..Config::default()
    };
    let srv = spawn_local(cfg.clone()).await.unwrap();
    let dir = tmpdir("frequent");
    for i in 0..20 {
        let out = dir.join(format!("f{i}.bin"));
        let mut o = opts(srv.url(), &out);
        o.checkpoint_interval = Duration::from_millis(1);
        download(o, None, CancellationToken::new()).await.unwrap();
        assert_file(&out, cfg.size);
        std::fs::remove_file(&out).unwrap();
    }
}

/// CDN-style signed URLs expire mid-download; workers must re-resolve the
/// original URL instead of treating 403 as the end.
#[tokio::test]
async fn expired_redirect_target_is_resolved_again() {
    let cfg = Config {
        size: 8 << 20,
        signed_redirect: Some(3),
        drop_after: Some(512 << 10),
        ..Config::default()
    };
    let srv = spawn_local(cfg.clone()).await.unwrap();
    let out = tmpdir("signed").join("f.bin");
    download(opts(srv.url(), &out), None, CancellationToken::new())
        .await
        .unwrap();
    assert_file(&out, cfg.size);
    assert!(srv.stats.expired.load(Ordering::SeqCst) > 0);
    assert!(srv.stats.redirects.load(Ordering::SeqCst) > 1);
}

#[tokio::test]
async fn rate_limit_is_respected() {
    let size = 3u64 << 20;
    let srv = spawn_local(Config {
        size,
        ..Config::default()
    })
    .await
    .unwrap();
    let out = tmpdir("ratelimit").join("f.bin");
    let mut o = opts(srv.url(), &out);
    o.limiters = vec![std::sync::Arc::new(pdl::ratelimit::RateLimiter::new(
        1 << 20,
    ))];
    let started = std::time::Instant::now();
    download(o, None, CancellationToken::new()).await.unwrap();
    let secs = started.elapsed().as_secs_f64();
    assert_file(&out, size);
    // 3 MiB at 1 MiB/s, minus the initial burst allowance.
    assert!((2.4..4.0).contains(&secs), "took {secs:.2}s");
}

/// One connection crawls; with a lowest speed it is reconnected instead of
/// holding the tail of the download hostage.
#[tokio::test]
async fn slow_connection_is_reconnected() {
    let size = 4u64 << 20;
    let srv = spawn_local(Config {
        size,
        slow_requests: vec![2],
        slow_rate: 8 << 10,
        ..Config::default()
    })
    .await
    .unwrap();
    let out = tmpdir("lowspeed").join("f.bin");
    let mut o = opts(srv.url(), &out);
    o.max_conns = 4;
    o.lowest_speed_limit = 64 << 10;
    o.low_speed_window = Duration::from_millis(500);
    let started = std::time::Instant::now();
    download(o, None, CancellationToken::new()).await.unwrap();
    assert_file(&out, size);
    // Without the reconnect the slow connection keeps up to 2*min_split
    // (512 KiB) at 8 KiB/s: about a minute.
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn first_request_is_retried() {
    let cfg = Config {
        size: 2 << 20,
        fail_requests: vec![1, 2],
        ..Config::default()
    };
    let srv = spawn_local(cfg.clone()).await.unwrap();
    let out = tmpdir("probe-retry").join("f.bin");
    download(opts(srv.url(), &out), None, CancellationToken::new())
        .await
        .unwrap();
    assert_file(&out, cfg.size);
}

#[tokio::test]
async fn retries_are_bounded() {
    let out = tmpdir("probe-refused").join("f.bin");
    let mut o = opts("http://127.0.0.1:1/x".into(), &out);
    o.max_retries = 2;
    let t = std::time::Instant::now();
    assert!(download(o, None, CancellationToken::new()).await.is_err());
    // Two backoffs (200ms + 400ms), not more.
    assert!(t.elapsed() < Duration::from_secs(3), "{:?}", t.elapsed());
}

#[tokio::test]
async fn cancel_is_prompt_while_waiting_for_headers() {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((s, _)) = l.accept().await {
            held.push(s);
        }
    });
    let out = tmpdir("hung").join("f.bin");
    let cancel = CancellationToken::new();
    let c = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        c.cancel();
    });
    let t = std::time::Instant::now();
    let err = download(opts(format!("http://{addr}/x"), &out), None, cancel)
        .await
        .unwrap_err();
    assert!(
        t.elapsed() < Duration::from_secs(2),
        "{:?} {err:#}",
        t.elapsed()
    );
}

#[tokio::test]
async fn no_range_stream_is_restarted_after_a_cut() {
    let cfg = Config {
        size: 3 << 20,
        range: false,
        cut_requests: vec![1],
        ..Config::default()
    };
    let srv = spawn_local(cfg.clone()).await.unwrap();
    let out = tmpdir("norange-cut").join("f.bin");
    download(opts(srv.url(), &out), None, CancellationToken::new())
        .await
        .unwrap();
    assert_file(&out, cfg.size);
    assert_eq!(srv.stats.requests.load(Ordering::SeqCst), 2);
}
