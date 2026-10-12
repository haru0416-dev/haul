use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use haul::daemon::{self, Handle};
use haul::engine::{part_path, state_path};
use haul_testserver::{Config, spawn_local};
use serde_json::{Value, json};

mod common;
use common::{assert_file, tmpdir};

async fn start(dir: &Path, f: impl FnOnce(&mut daemon::Config)) -> Handle {
    let mut cfg = daemon::Config {
        listen: "127.0.0.1:0".parse().unwrap(),
        dir: dir.to_path_buf(),
        min_split: 256 << 10,
        ..daemon::Config::default()
    };
    f(&mut cfg);
    daemon::spawn(cfg).await.unwrap()
}

/// Returns the whole JSON-RPC response object.
async fn call(d: &Handle, method: &str, params: Value) -> Value {
    let body = json!({"jsonrpc": "2.0", "id": "t", "method": method, "params": params});
    let resp = reqwest::Client::new()
        .post(d.url())
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    serde_json::from_slice(&resp.bytes().await.unwrap()).unwrap()
}

async fn ok(d: &Handle, method: &str, params: Value) -> Value {
    let r = call(d, method, params).await;
    assert!(r.get("error").is_none(), "{method} failed: {r}");
    r["result"].clone()
}

async fn wait_status(d: &Handle, gid: &str, want: &str) -> Value {
    for _ in 0..200 {
        let s = ok(d, "aria2.tellStatus", json!([gid])).await;
        if s["status"] == want {
            return s;
        }
        assert!(
            !(s["status"] == "error" && want != "error"),
            "download failed: {s}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{gid} never became {want}");
}

async fn wait_progress(d: &Handle, gid: &str) {
    for _ in 0..200 {
        let s = ok(d, "aria2.tellStatus", json!([gid, ["completedLength"]])).await;
        if s["completedLength"].as_str().unwrap() != "0" {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no progress");
}

#[tokio::test]
async fn add_uri_downloads_and_reports_like_aria2() {
    let srv = spawn_local(Config {
        size: 8 << 20,
        ..Config::default()
    })
    .await
    .unwrap();
    let dir = tmpdir("basic");
    let d = start(&dir, |_| {}).await;

    let gid = ok(&d, "aria2.addUri", json!([[srv.url()], {"out": "a.bin"}])).await;
    let gid = gid.as_str().unwrap().to_string();
    assert_eq!(gid.len(), 16);
    assert!(gid.chars().all(|c| c.is_ascii_hexdigit()));

    let s = wait_status(&d, &gid, "complete").await;
    assert_eq!(s["totalLength"], json!((8u64 << 20).to_string()));
    assert_eq!(s["completedLength"], s["totalLength"]);
    assert_eq!(
        s["files"][0]["path"],
        json!(dir.join("a.bin").to_string_lossy())
    );
    assert_file(&dir.join("a.bin"), 8 << 20);

    let stopped = ok(&d, "aria2.tellStopped", json!([0, 10, ["gid", "status"]])).await;
    assert_eq!(stopped, json!([{"gid": gid, "status": "complete"}]));
    let stat = ok(&d, "aria2.getGlobalStat", json!([])).await;
    assert_eq!(stat["numActive"], "0");
    assert_eq!(stat["numStopped"], "1");

    // Same name again: aria2-style renaming instead of clobbering.
    let gid2 = ok(&d, "aria2.addUri", json!([[srv.url()], {"out": "a.bin"}])).await;
    let s = wait_status(&d, gid2.as_str().unwrap(), "complete").await;
    assert_eq!(
        s["files"][0]["path"],
        json!(dir.join("a.1.bin").to_string_lossy())
    );
}

#[tokio::test]
async fn pause_and_unpause_resume_from_checkpoint() {
    let size = 8u64 << 20;
    let srv = spawn_local(Config {
        size,
        rate_per_conn: Some(128 << 10),
        ..Config::default()
    })
    .await
    .unwrap();
    let dir = tmpdir("pause");
    let d = start(&dir, |_| {}).await;
    let gid = ok(&d, "aria2.addUri", json!([[srv.url()]])).await;
    let gid = gid.as_str().unwrap();

    wait_status(&d, gid, "active").await;
    wait_progress(&d, gid).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(ok(&d, "aria2.pause", json!([gid])).await, json!(gid));
    let s = wait_status(&d, gid, "paused").await;
    let paused_at: u64 = s["completedLength"].as_str().unwrap().parse().unwrap();
    assert!(paused_at > 0);
    let out = dir.join("file.bin");
    assert!(state_path(&out).exists());

    // Paused downloads are listed with the waiting ones, as in aria2.
    let waiting = ok(&d, "aria2.tellWaiting", json!([0, 10, ["gid", "status"]])).await;
    assert_eq!(waiting, json!([{"gid": gid, "status": "paused"}]));

    ok(&d, "aria2.unpause", json!([gid])).await;
    wait_status(&d, gid, "complete").await;
    assert_file(&out, size);
    let refetched = srv.stats.bytes_sent.load(Ordering::SeqCst) - size;
    assert!(refetched < 4 << 20, "refetched {refetched} bytes");
}

#[tokio::test]
async fn remove_active_download_deletes_partial_files() {
    let srv = spawn_local(Config {
        size: 8 << 20,
        rate_per_conn: Some(128 << 10),
        ..Config::default()
    })
    .await
    .unwrap();
    let dir = tmpdir("remove");
    let d = start(&dir, |_| {}).await;
    let gid = ok(&d, "aria2.addUri", json!([[srv.url()]])).await;
    let gid = gid.as_str().unwrap();
    wait_progress(&d, gid).await;
    ok(&d, "aria2.remove", json!([gid])).await;
    wait_status(&d, gid, "removed").await;
    let out = dir.join("file.bin");
    assert!(!out.exists() && !part_path(&out).exists() && !state_path(&out).exists());

    ok(&d, "aria2.removeDownloadResult", json!([gid])).await;
    let r = call(&d, "aria2.tellStatus", json!([gid])).await;
    assert_eq!(r["error"]["code"], 1);
}

#[tokio::test]
async fn secret_is_enforced() {
    let srv = spawn_local(Config {
        size: 1 << 20,
        ..Config::default()
    })
    .await
    .unwrap();
    let dir = tmpdir("secret");
    let d = start(&dir, |c| c.secret = Some("s3cret".into())).await;

    let r = call(&d, "aria2.getGlobalStat", json!([])).await;
    assert_eq!(r["error"]["message"], "Unauthorized");
    let r = call(&d, "aria2.getGlobalStat", json!(["token:wrong"])).await;
    assert_eq!(r["error"]["message"], "Unauthorized");
    ok(&d, "aria2.getGlobalStat", json!(["token:s3cret"])).await;
    // Listing methods needs no token, as in aria2.
    ok(&d, "system.listMethods", json!([])).await;

    let r = ok(
        &d,
        "system.multicall",
        json!([[
            {"methodName": "aria2.addUri", "params": ["token:s3cret", [srv.url()]]},
            {"methodName": "aria2.getVersion", "params": []},
        ]]),
    )
    .await;
    assert!(r[0][0].as_str().is_some(), "{r}");
    assert_eq!(r[1]["message"], "Unauthorized");
}

#[tokio::test]
async fn websocket_gets_responses_and_notifications() {
    let srv = spawn_local(Config {
        size: 4 << 20,
        ..Config::default()
    })
    .await
    .unwrap();
    let dir = tmpdir("ws");
    let d = start(&dir, |_| {}).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{}/jsonrpc", d.addr))
        .await
        .unwrap();
    let req = json!({"jsonrpc": "2.0", "id": 7, "method": "aria2.addUri", "params": [[srv.url()]]});
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        req.to_string().into(),
    ))
    .await
    .unwrap();

    let mut gid = None;
    let mut seen = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !seen.iter().any(|m| m == "aria2.onDownloadComplete") {
        let msg = tokio::time::timeout_at(deadline, ws.next())
            .await
            .expect("timed out waiting for notifications")
            .unwrap()
            .unwrap();
        let v: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
        if v["id"] == 7 {
            gid = Some(v["result"].as_str().unwrap().to_string());
        } else if let Some(m) = v["method"].as_str() {
            assert_eq!(
                v["params"][0]["gid"].as_str(),
                gid.as_deref().or(v["params"][0]["gid"].as_str())
            );
            seen.push(m.to_string());
        }
    }
    assert!(gid.is_some());
    assert_eq!(seen, ["aria2.onDownloadStart", "aria2.onDownloadComplete"]);
    assert_file(&dir.join("file.bin"), 4 << 20);
}

#[tokio::test]
async fn respects_max_concurrent_downloads() {
    let srv = spawn_local(Config {
        size: 4 << 20,
        rate_per_conn: Some(256 << 10),
        ..Config::default()
    })
    .await
    .unwrap();
    let dir = tmpdir("queue");
    let d = start(&dir, |c| c.max_concurrent = 1).await;
    let a = ok(&d, "aria2.addUri", json!([[srv.url()], {"out": "a.bin"}])).await;
    let b = ok(&d, "aria2.addUri", json!([[srv.url()], {"out": "b.bin"}])).await;
    let active = ok(&d, "aria2.tellActive", json!([["gid"]])).await;
    assert_eq!(active, json!([{"gid": a}]));
    let waiting = ok(&d, "aria2.tellWaiting", json!([0, 10, ["gid", "status"]])).await;
    assert_eq!(waiting, json!([{"gid": b, "status": "waiting"}]));

    // Raising the limit starts the queued download right away.
    ok(
        &d,
        "aria2.changeGlobalOption",
        json!([{"max-concurrent-downloads": "2"}]),
    )
    .await;
    let active = ok(&d, "aria2.tellActive", json!([["gid"]])).await;
    assert_eq!(active.as_array().unwrap().len(), 2);
    wait_status(&d, a.as_str().unwrap(), "complete").await;
    wait_status(&d, b.as_str().unwrap(), "complete").await;
    assert_file(&dir.join("a.bin"), 4 << 20);
    assert_file(&dir.join("b.bin"), 4 << 20);
}

#[tokio::test]
async fn out_cannot_escape_dir() {
    let dir = tmpdir("escape");
    let d = start(&dir, |_| {}).await;
    for out in ["../x.bin", "/etc/x.bin", "a/../../x.bin"] {
        let r = call(
            &d,
            "aria2.addUri",
            json!([["http://127.0.0.1:1/f"], {"out": out}]),
        )
        .await;
        assert_eq!(r["error"]["code"], 1, "{out} accepted: {r}");
    }
}

#[tokio::test]
async fn shutdown_pauses_active_downloads_with_checkpoint() {
    let srv = spawn_local(Config {
        size: 8 << 20,
        rate_per_conn: Some(128 << 10),
        ..Config::default()
    })
    .await
    .unwrap();
    let dir = tmpdir("shutdown");
    let d = start(&dir, |_| {}).await;
    let gid = ok(&d, "aria2.addUri", json!([[srv.url()]])).await;
    wait_progress(&d, gid.as_str().unwrap()).await;
    ok(&d, "aria2.shutdown", json!([])).await;
    tokio::time::timeout(Duration::from_secs(10), d.wait())
        .await
        .expect("daemon did not stop");
    let out = dir.join("file.bin");
    assert!(part_path(&out).exists() && state_path(&out).exists());

    // A new daemon given the same URL picks the partial file back up.
    let d = start(&dir, |_| {}).await;
    let gid = ok(&d, "aria2.addUri", json!([[srv.url()]])).await;
    wait_status(&d, gid.as_str().unwrap(), "complete").await;
    assert_file(&out, 8 << 20);
}

async fn raw_post(d: &Handle, headers: &[(&str, &str)], body: Value) -> reqwest::Response {
    let mut req = reqwest::Client::new()
        .post(d.url())
        .header("content-type", "text/plain")
        .body(body.to_string());
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    req.send().await.unwrap()
}

#[tokio::test]
async fn web_pages_cannot_drive_the_daemon() {
    let dir = tmpdir("origin");
    let d = start(&dir, |c| {
        c.allowed_origins = vec!["http://127.0.0.1:18098".into()]
    })
    .await;
    let stat = json!({"jsonrpc": "2.0", "id": 1, "method": "aria2.getGlobalStat", "params": []});

    let r = raw_post(&d, &[("origin", "http://evil.example")], stat.clone()).await;
    assert_eq!(r.status(), 403);
    let r = raw_post(&d, &[("host", "evil.example:6800")], stat.clone()).await;
    assert_eq!(r.status(), 403);

    let r = raw_post(&d, &[("origin", "chrome-extension://abcdef")], stat.clone()).await;
    assert_eq!(r.status(), 200);
    let r = raw_post(&d, &[("origin", "http://127.0.0.1:18098")], stat.clone()).await;
    assert_eq!(r.status(), 200);
    assert_eq!(
        r.headers()["access-control-allow-origin"],
        "http://127.0.0.1:18098"
    );

    // WebSockets are not covered by CORS, so the upgrade itself is refused.
    let mut req = tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(
        format!("ws://{}/jsonrpc", d.addr),
    )
    .unwrap();
    req.headers_mut()
        .insert("origin", "http://evil.example".parse().unwrap());
    assert!(tokio_tungstenite::connect_async(req).await.is_err());
}

#[tokio::test]
async fn one_file_cannot_be_claimed_twice() {
    let srv = spawn_local(Config {
        size: 4 << 20,
        rate_per_conn: Some(128 << 10),
        ..Config::default()
    })
    .await
    .unwrap();
    let dir = tmpdir("samepath");
    std::fs::create_dir_all(dir.join("sub")).unwrap();
    // A finished download that happens to be named like a partial file.
    std::fs::write(dir.join("g.bin.part"), b"keep me").unwrap();
    let d = start(&dir, |_| {}).await;

    let a = ok(&d, "aria2.addUri", json!([[srv.url()], {"out": "f.bin"}])).await;
    let other_spelling = dir.join("sub").join("..");
    let b = ok(
        &d,
        "aria2.addUri",
        json!([[srv.url()], {"out": "f.bin", "dir": other_spelling.to_string_lossy()}]),
    )
    .await;
    let c = ok(&d, "aria2.addUri", json!([[srv.url()], {"out": "g.bin"}])).await;
    let path = |s: Value| s["files"][0]["path"].as_str().unwrap().to_string();
    let pa = path(ok(&d, "aria2.tellStatus", json!([a])).await);
    let pb = path(ok(&d, "aria2.tellStatus", json!([b])).await);
    let pc = path(ok(&d, "aria2.tellStatus", json!([c])).await);
    assert_ne!(pa, pb);
    assert!(pb.ends_with("f.1.bin"), "{pb}");
    assert!(pc.ends_with("g.1.bin"), "{pc}");

    ok(&d, "aria2.remove", json!([c])).await;
    wait_status(&d, c.as_str().unwrap(), "removed").await;
    assert_eq!(std::fs::read(dir.join("g.bin.part")).unwrap(), b"keep me");
}

#[tokio::test]
async fn pause_or_shutdown_after_remove_still_removes() {
    let srv = spawn_local(Config {
        size: 8 << 20,
        rate_per_conn: Some(128 << 10),
        ..Config::default()
    })
    .await
    .unwrap();
    let dir = tmpdir("rmpause");
    let d = start(&dir, |_| {}).await;
    let a = ok(&d, "aria2.addUri", json!([[srv.url()], {"out": "a.bin"}])).await;
    let b = ok(&d, "aria2.addUri", json!([[srv.url()], {"out": "b.bin"}])).await;
    wait_progress(&d, a.as_str().unwrap()).await;
    wait_progress(&d, b.as_str().unwrap()).await;

    // One batch, so pause lands before the engine has stopped.
    let batch = json!([
        {"jsonrpc": "2.0", "id": 1, "method": "aria2.remove", "params": [a]},
        {"jsonrpc": "2.0", "id": 2, "method": "aria2.pause", "params": [a]},
        {"jsonrpc": "2.0", "id": 3, "method": "aria2.remove", "params": [b]},
        {"jsonrpc": "2.0", "id": 4, "method": "aria2.shutdown", "params": []},
    ]);
    raw_post(&d, &[], batch).await;
    tokio::time::timeout(Duration::from_secs(10), d.wait())
        .await
        .expect("daemon did not stop");
    for name in ["a.bin", "b.bin"] {
        let out = dir.join(name);
        assert!(!part_path(&out).exists(), "{name}.part left behind");
        assert!(!state_path(&out).exists(), "{name}.part.state left behind");
    }
}

#[tokio::test]
async fn connection_count_is_capped() {
    let dir = tmpdir("cap");
    let d = start(&dir, |c| c.max_concurrent = 1).await;
    // Unreachable URL: the task just needs to exist.
    ok(
        &d,
        "aria2.addUri",
        json!([["http://127.0.0.1:9/x"], {"out": "x"}]),
    )
    .await;
    let g = ok(
        &d,
        "aria2.addUri",
        json!([["http://127.0.0.1:9/y"], {"split": "100000000"}]),
    )
    .await;
    let o = ok(&d, "aria2.getOption", json!([g])).await;
    assert_eq!(o["max-connection-per-server"], "64");
}

fn hook_script(dir: &Path, name: &str) -> (PathBuf, PathBuf) {
    use std::os::unix::fs::PermissionsExt;
    let log = dir.join(format!("{name}.log"));
    let script = dir.join(format!("{name}.sh"));
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\necho \"$1|$2|$3|$HAUL_EVENT|$HAUL_ERROR\" >> '{}'\n",
            log.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    (script, log)
}

async fn wait_for_file(path: &Path) -> String {
    // Failed first requests are retried for about 6 s before erroring out.
    for _ in 0..300 {
        if let Ok(s) = std::fs::read_to_string(path)
            && !s.is_empty()
        {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{} never written", path.display());
}

#[tokio::test]
async fn hooks_run_with_aria2_arguments() {
    let srv = spawn_local(Config {
        size: 1 << 20,
        ..Config::default()
    })
    .await
    .unwrap();
    let dir = tmpdir("hooks");
    let (done, done_log) = hook_script(&dir, "done");
    let (fail, fail_log) = hook_script(&dir, "fail");
    let d = start(&dir, |c| {
        c.hooks.on_complete = Some(done);
        c.hooks.on_error = Some(fail);
    })
    .await;

    let gid = ok(&d, "aria2.addUri", json!([[srv.url()], {"out": "h.bin"}])).await;
    let line = wait_for_file(&done_log).await;
    let path = dir.canonicalize().unwrap().join("h.bin");
    assert_eq!(
        line.trim(),
        format!("{}|1|{}|complete|", gid.as_str().unwrap(), path.display())
    );

    // Port 9 (discard) refuses connections: the download errors out.
    let bad = ok(
        &d,
        "aria2.addUri",
        json!([["http://127.0.0.1:9/x"], {"out": "x"}]),
    )
    .await;
    let line = wait_for_file(&fail_log).await;
    let fields: Vec<&str> = line.trim().split('|').collect();
    assert_eq!(fields[0], bad.as_str().unwrap());
    assert_eq!(fields[3], "error");
    assert!(!fields[4].is_empty(), "error message missing: {line}");
}

#[tokio::test]
async fn speed_limits_apply_and_change_at_runtime() {
    let size = 4u64 << 20;
    let srv = spawn_local(Config {
        size,
        ..Config::default()
    })
    .await
    .unwrap();
    let dir = tmpdir("limits");
    let d = start(&dir, |c| c.max_overall_download_limit = 512 << 10).await;
    let gid = ok(
        &d,
        "aria2.addUri",
        json!([[srv.url()], {"max-download-limit": "256K"}]),
    )
    .await;
    let gid = gid.as_str().unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let s = ok(
        &d,
        "aria2.tellStatus",
        json!([gid, ["completedLength", "status"]]),
    )
    .await;
    let done: u64 = s["completedLength"].as_str().unwrap().parse().unwrap();
    // 256 KiB/s for 1.5 s plus the burst allowance; far below 4 MiB.
    assert!(done < 1 << 20, "per-download limit ignored: {done}");
    let o = ok(&d, "aria2.getOption", json!([gid])).await;
    assert_eq!(o["max-download-limit"], (256u64 << 10).to_string());

    // Lifting both limits lets the running download finish quickly.
    ok(
        &d,
        "aria2.changeOption",
        json!([gid, {"max-download-limit": "0"}]),
    )
    .await;
    ok(
        &d,
        "aria2.changeGlobalOption",
        json!([{"max-overall-download-limit": "0"}]),
    )
    .await;
    wait_status(&d, gid, "complete").await;
    assert_file(&dir.join("file.bin"), size);
    let g = ok(&d, "aria2.getGlobalOption", json!([])).await;
    assert_eq!(g["max-overall-download-limit"], "0");
}

#[tokio::test]
async fn session_survives_restart() {
    use std::os::unix::fs::PermissionsExt;
    let size = 8u64 << 20;
    let srv = spawn_local(Config {
        size,
        rate_per_conn: Some(128 << 10),
        ..Config::default()
    })
    .await
    .unwrap();
    let dir = tmpdir("session");
    let session = dir.join("session.json");
    let d = start(&dir, |c| c.session = Some(session.clone())).await;
    let running = ok(
        &d,
        "aria2.addUri",
        json!([[srv.url()], {"out": "run.bin", "header": "X-Token: abc"}]),
    )
    .await;
    let held = ok(
        &d,
        "aria2.addUri",
        json!([[srv.url()], {"out": "held.bin"}]),
    )
    .await;
    ok(&d, "aria2.pause", json!([held])).await;
    wait_status(&d, held.as_str().unwrap(), "paused").await;
    wait_progress(&d, running.as_str().unwrap()).await;
    ok(&d, "aria2.shutdown", json!([])).await;
    tokio::time::timeout(Duration::from_secs(10), d.wait())
        .await
        .unwrap();

    let mode = std::fs::metadata(&session).unwrap().permissions().mode();
    assert_eq!(
        mode & 0o777,
        0o600,
        "session holds headers; must be private"
    );

    let d = start(&dir, |c| c.session = Some(session.clone())).await;
    // Same GIDs; the user's pause is kept, the shutdown pause is not.
    let s = ok(&d, "aria2.tellStatus", json!([held, ["status"]])).await;
    assert_eq!(s["status"], "paused");
    let o = ok(&d, "aria2.getOption", json!([running])).await;
    assert_eq!(o["header"], json!(["X-Token: abc"]));
    wait_status(&d, running.as_str().unwrap(), "complete").await;
    assert_file(&dir.join("run.bin"), size);
    // Only the paused one is left to remember.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let saved: Value = serde_json::from_slice(&std::fs::read(&session).unwrap()).unwrap();
    assert_eq!(saved["tasks"].as_array().unwrap().len(), 1);
    assert_eq!(saved["tasks"][0]["gid"], held);
}

#[tokio::test]
async fn empty_session_file_is_an_empty_session() {
    let dir = tmpdir("emptysession");
    let session = dir.join("haul.session");
    std::fs::write(&session, b"").unwrap();
    let d = start(&dir, |c| c.session = Some(session.clone())).await;
    let stat = ok(&d, "aria2.getGlobalStat", json!([])).await;
    assert_eq!(stat["numWaiting"], "0");
}

#[tokio::test]
async fn bad_option_leaves_nothing_applied() {
    let dir = tmpdir("atomic");
    let d = start(&dir, |c| c.max_concurrent = 1).await;
    let r = call(
        &d,
        "aria2.changeGlobalOption",
        json!([{"max-concurrent-downloads": "3", "min-split-size": "zz"}]),
    )
    .await;
    assert!(r.get("error").is_some(), "{r}");
    let g = ok(&d, "aria2.getGlobalOption", json!([])).await;
    assert_eq!(g["max-concurrent-downloads"], "1");

    let gid = ok(
        &d,
        "aria2.addUri",
        json!([["http://127.0.0.1:9/x"], {"pause": "true"}]),
    )
    .await;
    let r = call(
        &d,
        "aria2.changeOption",
        json!([gid, {"max-connection-per-server": "3", "max-download-limit": -5}]),
    )
    .await;
    assert!(r.get("error").is_some(), "{r}");
    let o = ok(&d, "aria2.getOption", json!([gid])).await;
    assert_ne!(o["max-connection-per-server"], "3");
}

#[tokio::test]
async fn add_paused_and_unsupported_checksum() {
    let dir = tmpdir("addpaused");
    let d = start(&dir, |_| {}).await;
    let gid = ok(
        &d,
        "aria2.addUri",
        json!([["http://127.0.0.1:9/x"], {"pause": "true"}]),
    )
    .await;
    let s = ok(&d, "aria2.tellStatus", json!([gid, ["status"]])).await;
    assert_eq!(s["status"], "paused");
    let r = call(
        &d,
        "aria2.addUri",
        json!([["http://127.0.0.1:9/y"], {"checksum": "sha-256=00"}]),
    )
    .await;
    assert!(
        r.get("error").is_some(),
        "a checksum must not be silently skipped: {r}"
    );
    let r = call(
        &d,
        "aria2.addUri",
        json!([["http://127.0.0.1:9/z"], "notobj"]),
    )
    .await;
    assert!(r.get("error").is_some(), "{r}");
    let r = call(
        &d,
        "aria2.addUri",
        json!([["http://127.0.0.1:9/z"], {"out": "a\u{0}b"}]),
    )
    .await;
    assert!(r.get("error").is_some(), "{r}");
}

#[tokio::test]
async fn session_info_and_save_session() {
    let dir = tmpdir("sessioninfo");
    let d = start(&dir, |_| {}).await;
    let a = ok(&d, "aria2.getSessionInfo", json!([])).await;
    let b = ok(&d, "aria2.getSessionInfo", json!([])).await;
    assert_eq!(a, b);
    assert!(
        call(&d, "aria2.saveSession", json!([]))
            .await
            .get("error")
            .is_some()
    );

    let session = dir.join("s.json");
    let d = start(&dir, |c| c.session = Some(session.clone())).await;
    ok(
        &d,
        "aria2.addUri",
        json!([["http://127.0.0.1:9/x"], {"pause": "true"}]),
    )
    .await;
    ok(&d, "aria2.saveSession", json!([])).await;
    // Written by the call itself, not by the debounced writer later.
    let saved: Value = serde_json::from_slice(&std::fs::read(&session).unwrap()).unwrap();
    assert_eq!(saved["tasks"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn finished_download_in_a_stale_session_is_complete() {
    let dir = tmpdir("stalesession");
    let session = dir.join("s.json");
    let out = dir.canonicalize().unwrap().join("done.bin");
    std::fs::write(&out, b"hello").unwrap();
    let file = json!({"version": 1, "tasks": [{
        "gid": "00000000000000aa", "uri": "http://127.0.0.1:9/done.bin", "path": out,
        "connections": 4, "min_split": 1048576, "headers": [],
        "max_download_limit": 0, "lowest_speed_limit": 0, "paused": false,
    }]});
    std::fs::write(&session, file.to_string()).unwrap();
    let d = start(&dir, |c| c.session = Some(session.clone())).await;
    let s = ok(&d, "aria2.tellStatus", json!(["00000000000000aa"])).await;
    assert_eq!(s["status"], "complete", "{s}");
    assert_eq!(s["completedLength"], "5");
    assert_eq!(std::fs::read(&out).unwrap(), b"hello");
}

#[tokio::test]
async fn stopped_results_are_bounded() {
    let dir = tmpdir("results");
    let d = start(&dir, |_| {}).await;
    let calls: Vec<Value> = (0..1010)
        .map(|i| {
            json!({"methodName": "aria2.addUri",
            "params": [[format!("http://127.0.0.1:9/f{i}")], {"pause": "true"}]})
        })
        .collect();
    let added = ok(&d, "system.multicall", json!([calls])).await;
    let removes: Vec<Value> = added
        .as_array()
        .unwrap()
        .iter()
        .map(|r| json!({"methodName": "aria2.remove", "params": [r[0]]}))
        .collect();
    ok(&d, "system.multicall", json!([removes])).await;
    let st = ok(&d, "aria2.getGlobalStat", json!([])).await;
    assert_eq!(st["numStopped"], "1000");
}

#[tokio::test]
async fn hooks_do_not_get_the_rpc_secret() {
    use std::os::unix::fs::PermissionsExt;
    let srv = spawn_local(Config {
        size: 64 << 10,
        ..Config::default()
    })
    .await
    .unwrap();
    let dir = tmpdir("hookenv");
    let log = dir.join("env.log");
    let script = dir.join("env.sh");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\necho \"secret=[$HAUL_RPC_SECRET]\" > '{}'\n",
            log.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    // SAFETY: set before the daemon starts; tests in this file do not read it.
    unsafe { std::env::set_var("HAUL_RPC_SECRET", "hunter2") };
    let d = start(&dir, |c| c.hooks.on_complete = Some(script)).await;
    ok(&d, "aria2.addUri", json!([[srv.url()], {"out": "e.bin"}])).await;
    let line = wait_for_file(&log).await;
    unsafe { std::env::remove_var("HAUL_RPC_SECRET") };
    assert_eq!(line.trim(), "secret=[]");
}
