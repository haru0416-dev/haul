//! Headless download daemon with an aria2-compatible JSON-RPC interface.
//!
//! Requests are accepted over HTTP (`POST /jsonrpc`) and WebSocket
//! (`GET /jsonrpc`); WebSocket clients also receive aria2's
//! `aria2.onDownload*` notifications. Numbers in responses are strings and
//! GIDs are 16 hex digits, as in aria2, so existing aria2 front ends and
//! browser extensions can drive pdl unchanged.
//!
//! Pausing cancels the engine, which leaves a checkpoint; unpausing queues the
//! download again and the engine resumes from that checkpoint.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde_json::{Map, Value, json};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::engine::{self, Event, Options, part_path, state_path};
use crate::ratelimit::RateLimiter;
use crate::util::{file_name_from_url, parse_size, unique_path};

#[derive(Clone, Debug)]
pub struct Config {
    pub listen: SocketAddr,
    /// When set, every call must pass `token:<secret>` as its first parameter.
    pub secret: Option<String>,
    pub dir: PathBuf,
    pub max_concurrent: usize,
    pub connections: usize,
    pub min_split: u64,
    /// Accept requests from any web origin. Without a secret this lets any
    /// page the user visits drive the daemon, so the CLI refuses that combination.
    pub allow_origin_all: bool,
    /// Web origins (e.g. `http://127.0.0.1:8080`) allowed to call the daemon.
    /// Requests without an Origin header (curl, native clients) and from
    /// browser extensions are always allowed.
    pub allowed_origins: Vec<String>,
    /// Commands run on download events, called like aria2's hooks:
    /// `<command> <gid> 1 <path>`, plus PDL_* environment variables.
    pub hooks: Hooks,
    /// Daemon-wide speed cap in bytes per second (0 = unlimited).
    pub max_overall_download_limit: u64,
    /// Default per-download speed cap for new downloads (0 = unlimited).
    pub max_download_limit: u64,
    /// Default lowest speed per connection for new downloads (0 = off).
    pub lowest_speed_limit: u64,
    /// Unfinished downloads are kept here and restored on the next start.
    pub session: Option<PathBuf>,
}

#[derive(Clone, Debug, Default)]
pub struct Hooks {
    pub on_start: Option<PathBuf>,
    pub on_pause: Option<PathBuf>,
    pub on_stop: Option<PathBuf>,
    pub on_complete: Option<PathBuf>,
    pub on_error: Option<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            listen: "127.0.0.1:6800".parse().unwrap(),
            secret: None,
            dir: PathBuf::from("."),
            max_concurrent: 5,
            connections: 16,
            min_split: 1 << 20,
            allow_origin_all: false,
            allowed_origins: Vec::new(),
            hooks: Hooks::default(),
            max_overall_download_limit: 0,
            max_download_limit: 0,
            lowest_speed_limit: 0,
            session: None,
        }
    }
}

/// Upper bound on connections per download, whatever a client asks for.
pub const MAX_CONNECTIONS: usize = 64;

/// Finished, failed and removed downloads kept for tellStopped (aria2's
/// max-download-result), so a long-running daemon does not grow forever.
const MAX_RESULTS: usize = 1000;

fn push_stopped(q: &mut Queue, gid: String) {
    q.stopped.push(gid);
    while q.stopped.len() > MAX_RESULTS {
        let old = q.stopped.remove(0);
        q.tasks.remove(&old);
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Status {
    Active,
    Waiting,
    Paused,
    Error,
    Complete,
    Removed,
}

impl Status {
    fn as_str(self) -> &'static str {
        match self {
            Status::Active => "active",
            Status::Waiting => "waiting",
            Status::Paused => "paused",
            Status::Error => "error",
            Status::Complete => "complete",
            Status::Removed => "removed",
        }
    }
}

struct Task {
    gid: String,
    uri: String,
    path: PathBuf,
    connections: usize,
    min_split: u64,
    headers: Vec<(String, String)>,
    status: Status,
    total: u64,
    completed: u64,
    speed: u64,
    conns: usize,
    error: Option<String>,
    cancel: Option<CancellationToken>,
    /// What an active download becomes once its engine stops.
    stop_as: Option<Status>,
    /// Paused by daemon shutdown rather than by a user; resumes on restore.
    auto_paused: bool,
    /// This download's own speed cap; shared with the running engine so a
    /// change applies immediately.
    limiter: Arc<RateLimiter>,
    lowest_speed_limit: u64,
}

struct Globals {
    dir: PathBuf,
    max_concurrent: usize,
    connections: usize,
    min_split: u64,
    overall: Arc<RateLimiter>,
    max_download_limit: u64,
    lowest_speed_limit: u64,
}

struct Queue {
    tasks: HashMap<String, Task>,
    active: Vec<String>,
    /// Waiting and paused downloads in queue order.
    waiting: Vec<String>,
    stopped: Vec<String>,
    globals: Globals,
    next_gid: u64,
    shutting_down: bool,
}

struct Inner {
    queue: Mutex<Queue>,
    secret: Option<String>,
    notify: broadcast::Sender<String>,
    allow_origin_all: bool,
    allowed_origins: Vec<String>,
    /// Listening on loopback only: Host must name loopback (DNS rebinding).
    loopback: bool,
    idle: tokio::sync::Notify,
    shutdown: CancellationToken,
    hooks: Hooks,
    session: Option<PathBuf>,
    /// Signals the session writer that the queue changed.
    dirty: tokio::sync::Notify,
    /// aria2.getSessionInfo: fixed for the life of the process.
    session_id: String,
    /// One session save at a time: they share the temporary file.
    saving: Mutex<()>,
}

type Daemon = Arc<Inner>;

pub struct Handle {
    pub addr: SocketAddr,
    shutdown: CancellationToken,
    join: tokio::task::JoinHandle<()>,
}

impl Handle {
    pub fn url(&self) -> String {
        format!("http://{}/jsonrpc", self.addr)
    }

    /// Resolves when the daemon has stopped (after `shutdown` or an
    /// `aria2.shutdown` call).
    pub async fn wait(self) {
        let _ = self.join.await;
    }

    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Resolves once shutdown was requested, locally or by `aria2.shutdown`.
    pub async fn shutdown_requested(&self) {
        self.shutdown.cancelled().await
    }
}

/// Binds and starts serving in the background.
pub async fn spawn(config: Config) -> anyhow::Result<Handle> {
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    let addr = listener.local_addr()?;
    let shutdown = CancellationToken::new();
    let d: Daemon = Arc::new(Inner {
        queue: Mutex::new(Queue {
            tasks: HashMap::new(),
            active: Vec::new(),
            waiting: Vec::new(),
            stopped: Vec::new(),
            globals: Globals {
                dir: config.dir,
                max_concurrent: config.max_concurrent.max(1),
                connections: config.connections.clamp(1, MAX_CONNECTIONS),
                min_split: config.min_split,
                overall: Arc::new(RateLimiter::new(config.max_overall_download_limit)),
                max_download_limit: config.max_download_limit,
                lowest_speed_limit: config.lowest_speed_limit,
            },
            next_gid: seed_gid(),
            shutting_down: false,
        }),
        secret: config.secret,
        notify: broadcast::channel(256).0,
        allow_origin_all: config.allow_origin_all,
        allowed_origins: config
            .allowed_origins
            .iter()
            .map(|o| o.trim_end_matches('/').to_ascii_lowercase())
            .collect(),
        loopback: addr.ip().is_loopback(),
        idle: tokio::sync::Notify::new(),
        shutdown: shutdown.clone(),
        hooks: config.hooks,
        session: config.session,
        dirty: tokio::sync::Notify::new(),
        session_id: format!("{:016x}", seed_gid()),
        saving: Mutex::new(()),
    });
    if let Some(path) = &d.session {
        restore_session(&d, path)?;
    }

    let app = Router::new()
        .route(
            "/jsonrpc",
            get(ws_upgrade).post(http_rpc).options(preflight),
        )
        .with_state(d.clone());
    let join = {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let server = axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.clone().cancelled_owned());
            let stopper = async {
                shutdown.cancelled().await;
                // Pause active downloads so each leaves a checkpoint.
                stop_all(&d).await;
                if let Err(e) = save_session(&d) {
                    eprintln!("pdl: saving session failed: {e:#}");
                }
            };
            let writer = session_writer(&d, shutdown.clone());
            schedule(&d);
            let _ = tokio::join!(server, stopper, writer);
        })
    };
    Ok(Handle {
        addr,
        shutdown,
        join,
    })
}

fn seed_gid() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    nanos ^ ((std::process::id() as u64) << 48)
}

async fn stop_all(d: &Daemon) {
    {
        let mut q = d.queue.lock().unwrap();
        q.shutting_down = true;
        for gid in q.active.clone() {
            let t = q.tasks.get_mut(&gid).unwrap();
            // A pending remove still wins: its files must be deleted.
            if t.stop_as.is_none() {
                t.stop_as = Some(Status::Paused);
                t.auto_paused = true;
            }
            if let Some(c) = &t.cancel {
                c.cancel();
            }
        }
    }
    let _ = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let idle = d.idle.notified();
            if d.queue.lock().unwrap().active.is_empty() {
                return;
            }
            idle.await;
        }
    })
    .await;
}

// ---------------------------------------------------------------------------
// Scheduling
// ---------------------------------------------------------------------------

fn schedule(d: &Daemon) {
    loop {
        let start = {
            let mut q = d.queue.lock().unwrap();
            if q.shutting_down || q.active.len() >= q.globals.max_concurrent {
                return;
            }
            let Some(pos) = q
                .waiting
                .iter()
                .position(|g| q.tasks[g].status == Status::Waiting)
            else {
                return;
            };
            let gid = q.waiting.remove(pos);
            q.active.push(gid.clone());
            let overall = q.globals.overall.clone();
            let t = q.tasks.get_mut(&gid).unwrap();
            let cancel = CancellationToken::new();
            t.status = Status::Active;
            t.auto_paused = false;
            t.cancel = Some(cancel.clone());
            t.stop_as = None;
            t.error = None;
            t.speed = 0;
            let mut opts = Options::new(t.uri.clone(), t.path.clone());
            opts.max_conns = t.connections;
            opts.min_split = t.min_split;
            opts.headers = t.headers.clone();
            opts.limiters = vec![t.limiter.clone(), overall];
            opts.lowest_speed_limit = t.lowest_speed_limit;
            (gid, opts, cancel, HookInfo::of(t))
        };
        let (gid, opts, cancel, info) = start;
        notify(d, "aria2.onDownloadStart", &gid, info);
        mark_dirty(d);
        tokio::spawn(run(d.clone(), gid, opts, cancel));
    }
}

async fn run(d: Daemon, gid: String, opts: Options, cancel: CancellationToken) {
    if let Some(dir) = opts.output.parent() {
        let _ = tokio::fs::create_dir_all(dir).await;
    }
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let forward = {
        let d = d.clone();
        let gid = gid.clone();
        tokio::spawn(async move {
            while let Some(ev) = rx.recv().await {
                let mut q = d.queue.lock().unwrap();
                let Some(t) = q.tasks.get_mut(&gid) else {
                    continue;
                };
                match ev {
                    Event::Started {
                        size,
                        resumed_bytes,
                        ..
                    } => {
                        t.total = size.unwrap_or(0);
                        t.completed = resumed_bytes;
                    }
                    Event::Progress {
                        downloaded,
                        size,
                        speed,
                        connections,
                    } => {
                        t.completed = downloaded;
                        t.total = size.unwrap_or(t.total);
                        t.speed = speed;
                        t.conns = connections;
                    }
                    Event::Finished { size, .. } => {
                        t.total = size;
                        t.completed = size;
                    }
                    Event::Warning { .. } => {}
                }
            }
        })
    };

    let result = engine::download(opts, Some(tx), cancel).await;
    let _ = forward.await;

    let method = {
        let mut q = d.queue.lock().unwrap();
        q.active.retain(|g| g != &gid);
        let t = q.tasks.get_mut(&gid).unwrap();
        t.cancel = None;
        t.speed = 0;
        t.conns = 0;
        let stop_as = t.stop_as.take();
        let (method, cleanup) = match (&result, stop_as) {
            (Ok(_), _) => {
                t.status = Status::Complete;
                t.completed = t.total;
                ("aria2.onDownloadComplete", None)
            }
            (Err(_), Some(Status::Paused)) => {
                t.status = Status::Paused;
                ("aria2.onDownloadPause", None)
            }
            (Err(_), Some(_)) => {
                t.status = Status::Removed;
                ("aria2.onDownloadStop", Some(t.path.clone()))
            }
            (Err(e), None) => {
                t.status = Status::Error;
                t.error = Some(format!("{e:#}"));
                ("aria2.onDownloadError", None)
            }
        };
        // Delete under the lock: once this task is stopped its path is free
        // for a new download, which must not see these files disappear.
        if let Some(path) = cleanup {
            remove_partial(&path);
        }
        let info = HookInfo::of(&q.tasks[&gid]);
        match q.tasks[&gid].status {
            Status::Paused => q.waiting.insert(0, gid.clone()),
            _ => push_stopped(&mut q, gid.clone()),
        }
        (method, info)
    };
    let (method, info) = method;
    notify(&d, method, &gid, info);
    mark_dirty(&d);
    d.idle.notify_waiters();
    schedule(&d);
    if d.queue.lock().unwrap().active.is_empty() {
        release_free_memory();
    }
}

/// Hands memory freed by finished downloads back to the OS. glibc keeps it
/// in its arenas otherwise, so an idle daemon would sit at its peak size.
fn release_free_memory() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    // SAFETY: malloc_trim only releases free heap pages; it has no
    // preconditions and is thread-safe.
    unsafe {
        libc::malloc_trim(0);
    }
}

fn remove_partial(path: &Path) {
    let _ = std::fs::remove_file(part_path(path));
    let _ = std::fs::remove_file(state_path(path));
}

/// What a hook needs, taken while the task's new state is decided (under the
/// queue lock), so a result removed right after cannot make the hook vanish.
struct HookInfo {
    path: PathBuf,
    uri: String,
    error: Option<String>,
    auto_paused: bool,
}

impl HookInfo {
    fn of(t: &Task) -> Self {
        HookInfo {
            path: t.path.clone(),
            uri: t.uri.clone(),
            error: t.error.clone(),
            auto_paused: t.auto_paused,
        }
    }
}

fn notify(d: &Daemon, method: &str, gid: &str, info: HookInfo) {
    run_hook(d, method, gid, info);
    let msg = json!({"jsonrpc": "2.0", "method": method, "params": [{"gid": gid}]});
    let _ = d.notify.send(msg.to_string());
}

/// Runs the configured hook for `method`, aria2-style: `<cmd> <gid> 1 <path>`.
/// The command is executed directly (no shell) and never blocks the daemon.
fn run_hook(d: &Daemon, method: &str, gid: &str, info: HookInfo) {
    let h = &d.hooks;
    let (cmd, event) = match method {
        "aria2.onDownloadStart" => (&h.on_start, "start"),
        "aria2.onDownloadPause" => (&h.on_pause, "pause"),
        "aria2.onDownloadStop" => (&h.on_stop, "stop"),
        "aria2.onDownloadComplete" => (&h.on_complete, "complete"),
        "aria2.onDownloadError" => (&h.on_error, "error"),
        _ => return,
    };
    let Some(cmd) = cmd.clone() else { return };
    let HookInfo {
        path,
        uri,
        error,
        auto_paused,
    } = info;
    // Shutdown pauses everything; that is not a user-visible pause.
    if auto_paused && event == "pause" {
        return;
    }
    let mut c = tokio::process::Command::new(&cmd);
    c.arg(gid)
        .arg("1")
        .arg(&path)
        .env("PDL_EVENT", event)
        .env("PDL_GID", gid)
        .env("PDL_PATH", &path)
        .env("PDL_URI", &uri)
        .env("PDL_ERROR", error.unwrap_or_default())
        // Hooks need no RPC access; do not hand them the secret.
        .env_remove("PDL_RPC_SECRET")
        .stdin(std::process::Stdio::null());
    let gid = gid.to_string();
    tokio::spawn(async move {
        match c.status().await {
            Ok(st) if st.success() => {}
            Ok(st) => eprintln!("pdl: {event} hook for {gid} exited with {st}"),
            Err(e) => eprintln!("pdl: {event} hook {} failed to start: {e}", cmd.display()),
        }
    });
}

fn mark_dirty(d: &Daemon) {
    if d.session.is_some() {
        d.dirty.notify_one();
    }
}

// ---------------------------------------------------------------------------
// Session
// ---------------------------------------------------------------------------

const SESSION_VERSION: u32 = 1;

#[derive(serde::Serialize, serde::Deserialize)]
struct SessionFile {
    version: u32,
    tasks: Vec<SessionTask>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SessionTask {
    gid: String,
    uri: String,
    path: PathBuf,
    connections: usize,
    min_split: u64,
    headers: Vec<(String, String)>,
    max_download_limit: u64,
    lowest_speed_limit: u64,
    paused: bool,
}

/// Writes the session whenever the queue changes, coalescing bursts.
async fn session_writer(d: &Daemon, stop: CancellationToken) {
    if d.session.is_none() {
        return;
    }
    loop {
        tokio::select! {
            _ = d.dirty.notified() => {}
            _ = stop.cancelled() => return,
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        if let Err(e) = save_session(d) {
            eprintln!("pdl: saving session failed: {e:#}");
        }
    }
}

fn save_session(d: &Daemon) -> anyhow::Result<()> {
    let Some(path) = &d.session else {
        return Ok(());
    };
    // Held from the snapshot to the rename, so saves land in snapshot order.
    let _saving = d.saving.lock().unwrap();
    let file = {
        let q = d.queue.lock().unwrap();
        let tasks = q
            .active
            .iter()
            .chain(q.waiting.iter())
            .map(|g| &q.tasks[g])
            .map(|t| SessionTask {
                gid: t.gid.clone(),
                uri: t.uri.clone(),
                path: t.path.clone(),
                connections: t.connections,
                min_split: t.min_split,
                headers: t.headers.clone(),
                max_download_limit: t.limiter.rate(),
                lowest_speed_limit: t.lowest_speed_limit,
                // Downloads stopped by shutdown pick up where they were.
                paused: t.status == Status::Paused && !t.auto_paused,
            })
            .collect();
        SessionFile {
            version: SESSION_VERSION,
            tasks,
        }
    };
    let data = serde_json::to_vec_pretty(&file)?;
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        // Request headers may carry cookies or credentials.
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)?;
        f.write_all(&data)?;
        f.sync_data()?;
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

fn restore_session(d: &Daemon, path: &Path) -> anyhow::Result<()> {
    let data = match std::fs::read(path) {
        Ok(data) => data,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(anyhow::Error::new(e).context(format!("reading {}", path.display()))),
    };
    // An empty file (e.g. created with `touch`, as for aria2) is an empty session.
    if data.iter().all(u8::is_ascii_whitespace) {
        return Ok(());
    }
    let file: SessionFile = serde_json::from_slice(&data)
        .map_err(|e| anyhow::anyhow!("{} is not a pdl session: {e}", path.display()))?;
    anyhow::ensure!(
        file.version == SESSION_VERSION,
        "{}: unsupported session version {}",
        path.display(),
        file.version
    );
    let mut q = d.queue.lock().unwrap();
    for t in file.tasks {
        if q.tasks.contains_key(&t.gid) {
            continue;
        }
        // The session is written shortly after changes, so a crash can leave
        // a download listed that has in fact finished.
        let finished = std::fs::metadata(&t.path)
            .ok()
            .filter(|m| m.is_file() && !part_path(&t.path).exists())
            .map(|m| m.len());
        match finished {
            Some(_) => push_stopped(&mut q, t.gid.clone()),
            None => q.waiting.push(t.gid.clone()),
        }
        q.tasks.insert(
            t.gid.clone(),
            Task {
                gid: t.gid,
                uri: t.uri,
                path: t.path,
                connections: t.connections.clamp(1, MAX_CONNECTIONS),
                min_split: t.min_split,
                headers: t.headers,
                status: match finished {
                    Some(_) => Status::Complete,
                    None if t.paused => Status::Paused,
                    None => Status::Waiting,
                },
                total: finished.unwrap_or(0),
                completed: finished.unwrap_or(0),
                speed: 0,
                conns: 0,
                error: None,
                cancel: None,
                stop_as: None,
                auto_paused: false,
                limiter: Arc::new(RateLimiter::new(t.max_download_limit)),
                lowest_speed_limit: t.lowest_speed_limit,
            },
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

fn is_extension_origin(origin: &str) -> bool {
    [
        "chrome-extension://",
        "moz-extension://",
        "safari-web-extension://",
    ]
    .iter()
    .any(|p| origin.starts_with(p))
}

/// Whether a browser request from `origin` may use the RPC.
fn origin_allowed(d: &Daemon, origin: &str) -> bool {
    let origin = origin.trim_end_matches('/').to_ascii_lowercase();
    d.allow_origin_all || is_extension_origin(&origin) || d.allowed_origins.contains(&origin)
}

/// Rejects requests a web page could forge: a foreign Origin (simple
/// cross-site POSTs and WebSockets are not stopped by CORS), and when bound
/// to loopback, a Host that is not loopback (DNS rebinding).
fn check_caller(d: &Daemon, headers: &HeaderMap) -> Result<(), Response> {
    if d.loopback {
        if let Some(host) = headers.get(header::HOST) {
            let host = host.to_str().unwrap_or("");
            let name = match host.strip_prefix('[') {
                Some(v6) => v6.split(']').next().unwrap_or(""),
                None => host.split(':').next().unwrap_or(""),
            };
            let ok = name.eq_ignore_ascii_case("localhost")
                || name
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback());
            if !ok {
                return Err((StatusCode::FORBIDDEN, "Host not allowed").into_response());
            }
        }
    }
    if let Some(origin) = headers.get(header::ORIGIN) {
        if !origin_allowed(d, origin.to_str().unwrap_or("")) {
            return Err((StatusCode::FORBIDDEN, "Origin not allowed").into_response());
        }
    }
    Ok(())
}

fn with_cors(d: &Daemon, headers: &HeaderMap, mut resp: Response) -> Response {
    let Some(origin) = headers
        .get(header::ORIGIN)
        .and_then(|o| o.to_str().ok())
        .filter(|o| origin_allowed(d, o))
    else {
        return resp;
    };
    let allow = if d.allow_origin_all {
        HeaderValue::from_static("*")
    } else {
        HeaderValue::from_str(origin).unwrap_or(HeaderValue::from_static("null"))
    };
    {
        let h = resp.headers_mut();
        h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, allow);
        h.insert(header::VARY, HeaderValue::from_static("Origin"));
        h.insert(
            header::ACCESS_CONTROL_ALLOW_HEADERS,
            HeaderValue::from_static("Content-Type"),
        );
        h.insert(
            header::ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_static("POST, GET, OPTIONS"),
        );
    }
    resp
}

async fn preflight(State(d): State<Daemon>, headers: HeaderMap) -> Response {
    if let Err(r) = check_caller(&d, &headers) {
        return r;
    }
    with_cors(&d, &headers, StatusCode::NO_CONTENT.into_response())
}

async fn http_rpc(State(d): State<Daemon>, headers: HeaderMap, body: Bytes) -> Response {
    if let Err(r) = check_caller(&d, &headers) {
        return r;
    }
    let reply = handle_text(&d, &body);
    // aria2 answers a single failed call with 400; batches always get 200.
    let status = match serde_json::from_str::<Value>(&reply) {
        Ok(Value::Object(o)) if o.contains_key("error") => StatusCode::BAD_REQUEST,
        _ => StatusCode::OK,
    };
    let resp = (
        status,
        [(header::CONTENT_TYPE, "application/json-rpc")],
        reply,
    )
        .into_response();
    with_cors(&d, &headers, resp)
}

async fn ws_upgrade(State(d): State<Daemon>, headers: HeaderMap, ws: WebSocketUpgrade) -> Response {
    if let Err(r) = check_caller(&d, &headers) {
        return r;
    }
    ws.on_upgrade(move |socket| ws_session(d, socket))
}

async fn ws_session(d: Daemon, mut socket: WebSocket) {
    let mut notes = d.notify.subscribe();
    loop {
        tokio::select! {
            msg = socket.recv() => {
                let Some(Ok(msg)) = msg else { return };
                let text = match msg {
                    Message::Text(t) => t.to_string(),
                    Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
                    Message::Close(_) => return,
                    _ => continue,
                };
                let reply = handle_text(&d, text.as_bytes());
                if socket.send(Message::Text(reply.into())).await.is_err() {
                    return;
                }
            }
            note = notes.recv() => match note {
                Ok(n) => {
                    if socket.send(Message::Text(n.into())).await.is_err() {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(_) => return,
            },
            _ = d.shutdown.cancelled() => return,
        }
    }
}

/// Handles a single JSON-RPC request or a batch.
fn handle_text(d: &Daemon, body: &[u8]) -> String {
    let reply = match serde_json::from_slice::<Value>(body) {
        Err(_) => error_response(Value::Null, -32700, "Parse error"),
        Ok(Value::Array(calls)) => Value::Array(calls.iter().map(|c| handle_call(d, c)).collect()),
        Ok(call) => handle_call(d, &call),
    };
    reply.to_string()
}

fn error_response(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

fn handle_call(d: &Daemon, call: &Value) -> Value {
    let id = call.get("id").cloned().unwrap_or(Value::Null);
    let Some(method) = call.get("method").and_then(Value::as_str) else {
        return error_response(id, -32600, "Invalid Request");
    };
    let params = match call.get("params") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(a)) => a.clone(),
        Some(_) => return error_response(id, -32602, "Invalid params"),
    };
    match call_method(d, method, params) {
        Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
        Err(e) => error_response(id, e.code, &e.message),
    }
}

// ---------------------------------------------------------------------------
// Methods
// ---------------------------------------------------------------------------

struct RpcError {
    code: i64,
    message: String,
}

fn fail(message: impl Into<String>) -> RpcError {
    RpcError {
        code: 1,
        message: message.into(),
    }
}

type RpcResult = Result<Value, RpcError>;

const METHODS: &[&str] = &[
    "aria2.addUri",
    "aria2.remove",
    "aria2.forceRemove",
    "aria2.pause",
    "aria2.pauseAll",
    "aria2.forcePause",
    "aria2.forcePauseAll",
    "aria2.unpause",
    "aria2.unpauseAll",
    "aria2.tellStatus",
    "aria2.getUris",
    "aria2.getFiles",
    "aria2.tellActive",
    "aria2.tellWaiting",
    "aria2.tellStopped",
    "aria2.changePosition",
    "aria2.getOption",
    "aria2.changeOption",
    "aria2.getGlobalOption",
    "aria2.changeGlobalOption",
    "aria2.getGlobalStat",
    "aria2.purgeDownloadResult",
    "aria2.removeDownloadResult",
    "aria2.getVersion",
    "aria2.getSessionInfo",
    "aria2.shutdown",
    "aria2.forceShutdown",
    "aria2.saveSession",
    "system.multicall",
    "system.listMethods",
    "system.listNotifications",
];

const NOTIFICATIONS: &[&str] = &[
    "aria2.onDownloadStart",
    "aria2.onDownloadPause",
    "aria2.onDownloadStop",
    "aria2.onDownloadComplete",
    "aria2.onDownloadError",
];

fn call_method(d: &Daemon, method: &str, mut params: Vec<Value>) -> RpcResult {
    match method {
        "system.listMethods" => return Ok(json!(METHODS)),
        "system.listNotifications" => return Ok(json!(NOTIFICATIONS)),
        "system.multicall" => return multicall(d, params),
        _ => {}
    }
    // aria2 accepts a token even when no secret is configured.
    let token = match params.first().and_then(Value::as_str) {
        Some(t) if t.starts_with("token:") => {
            let t = t["token:".len()..].to_string();
            params.remove(0);
            Some(t)
        }
        _ => None,
    };
    if let Some(secret) = &d.secret {
        if token.as_deref() != Some(secret.as_str()) {
            return Err(fail("Unauthorized"));
        }
    }
    let p = Params(params);
    match method {
        "aria2.addUri" => add_uri(d, &p),
        "aria2.remove" | "aria2.forceRemove" => remove(d, p.gid(0)?),
        "aria2.pause" | "aria2.forcePause" => {
            let gid = p.gid(0)?;
            pause(d, gid)?;
            Ok(json!(gid))
        }
        "aria2.pauseAll" | "aria2.forcePauseAll" => {
            let gids: Vec<String> = {
                let q = d.queue.lock().unwrap();
                q.active.iter().chain(q.waiting.iter()).cloned().collect()
            };
            for g in gids {
                let _ = pause(d, &g);
            }
            Ok(json!("OK"))
        }
        "aria2.unpause" => {
            unpause(d, p.gid(0)?)?;
            Ok(json!(p.gid(0)?))
        }
        "aria2.unpauseAll" => {
            let gids = d.queue.lock().unwrap().waiting.clone();
            for g in gids {
                let _ = unpause(d, &g);
            }
            Ok(json!("OK"))
        }
        "aria2.tellStatus" => {
            let q = d.queue.lock().unwrap();
            let t = find(&q, p.gid(0)?)?;
            Ok(status_json(&q, t, p.keys(1)))
        }
        "aria2.getUris" => {
            let q = d.queue.lock().unwrap();
            let t = find(&q, p.gid(0)?)?;
            Ok(uris_json(t))
        }
        "aria2.getFiles" => {
            let q = d.queue.lock().unwrap();
            let t = find(&q, p.gid(0)?)?;
            Ok(files_json(t))
        }
        "aria2.tellActive" => {
            let q = d.queue.lock().unwrap();
            let keys = p.keys(0);
            Ok(Value::Array(
                q.active
                    .iter()
                    .map(|g| status_json(&q, &q.tasks[g], keys.clone()))
                    .collect(),
            ))
        }
        "aria2.tellWaiting" | "aria2.tellStopped" => {
            let q = d.queue.lock().unwrap();
            let list = if method == "aria2.tellWaiting" {
                &q.waiting
            } else {
                &q.stopped
            };
            let picked = window(list, p.int(0)?, p.int(1)?);
            let keys = p.keys(2);
            Ok(Value::Array(
                picked
                    .iter()
                    .map(|g| status_json(&q, &q.tasks[*g], keys.clone()))
                    .collect(),
            ))
        }
        "aria2.changePosition" => change_position(d, p.gid(0)?, p.int(1)?, p.str(2)?),
        "aria2.getOption" => {
            let q = d.queue.lock().unwrap();
            let t = find(&q, p.gid(0)?)?;
            Ok(task_options(t))
        }
        "aria2.changeOption" => {
            let gid = p.gid(0)?.to_string();
            let opts = p.object(1)?;
            check_supported(opts)?;
            // Parse everything first: a bad value must leave nothing applied.
            let connections = opt_usize(opts, "max-connection-per-server")?;
            let min_split = opt_size(opts, "min-split-size")?;
            let limit = opt_size(opts, "max-download-limit")?;
            let lowest = opt_size(opts, "lowest-speed-limit")?;
            let mut q = d.queue.lock().unwrap();
            let t = q
                .tasks
                .get_mut(&gid)
                .ok_or_else(|| fail(format!("GID {gid} is not found")))?;
            if let Some(n) = connections {
                t.connections = n.clamp(1, MAX_CONNECTIONS);
            }
            if let Some(n) = min_split {
                t.min_split = n;
            }
            if let Some(n) = limit {
                // Shared with the engine, so a running download slows at once.
                t.limiter.set_rate(n);
            }
            if let Some(n) = lowest {
                t.lowest_speed_limit = n;
            }
            drop(q);
            mark_dirty(d);
            Ok(json!("OK"))
        }
        "aria2.getGlobalOption" => {
            let q = d.queue.lock().unwrap();
            let g = &q.globals;
            Ok(json!({
                "dir": g.dir.to_string_lossy(),
                "max-concurrent-downloads": g.max_concurrent.to_string(),
                "max-connection-per-server": g.connections.to_string(),
                "split": g.connections.to_string(),
                "min-split-size": g.min_split.to_string(),
                "max-overall-download-limit": g.overall.rate().to_string(),
                "max-download-limit": g.max_download_limit.to_string(),
                "lowest-speed-limit": g.lowest_speed_limit.to_string(),
            }))
        }
        "aria2.changeGlobalOption" => {
            let opts = p.object(0)?;
            check_supported(opts)?;
            // Parse everything first: a bad value must leave nothing applied.
            let concurrent = opt_usize(opts, "max-concurrent-downloads")?;
            let connections = opt_usize(opts, "max-connection-per-server")?;
            let min_split = opt_size(opts, "min-split-size")?;
            let dir = opt_path(opts, "dir")?;
            let overall = opt_size(opts, "max-overall-download-limit")?;
            let limit = opt_size(opts, "max-download-limit")?;
            let lowest = opt_size(opts, "lowest-speed-limit")?;
            {
                let mut q = d.queue.lock().unwrap();
                if let Some(n) = concurrent {
                    q.globals.max_concurrent = n.max(1);
                }
                if let Some(n) = connections {
                    q.globals.connections = n.clamp(1, MAX_CONNECTIONS);
                }
                if let Some(n) = min_split {
                    q.globals.min_split = n;
                }
                if let Some(dir) = dir {
                    q.globals.dir = dir;
                }
                if let Some(n) = overall {
                    q.globals.overall.set_rate(n);
                }
                if let Some(n) = limit {
                    q.globals.max_download_limit = n;
                }
                if let Some(n) = lowest {
                    q.globals.lowest_speed_limit = n;
                }
            }
            schedule(d);
            Ok(json!("OK"))
        }
        "aria2.getGlobalStat" => {
            let q = d.queue.lock().unwrap();
            let speed: u64 = q.active.iter().map(|g| q.tasks[g].speed).sum();
            Ok(json!({
                "downloadSpeed": speed.to_string(),
                "uploadSpeed": "0",
                "numActive": q.active.len().to_string(),
                "numWaiting": q.waiting.len().to_string(),
                "numStopped": q.stopped.len().to_string(),
                "numStoppedTotal": q.stopped.len().to_string(),
            }))
        }
        "aria2.purgeDownloadResult" => {
            let mut q = d.queue.lock().unwrap();
            let stopped = std::mem::take(&mut q.stopped);
            for g in stopped {
                q.tasks.remove(&g);
            }
            Ok(json!("OK"))
        }
        "aria2.removeDownloadResult" => {
            let mut q = d.queue.lock().unwrap();
            let gid = p.gid(0)?.to_string();
            let pos =
                q.stopped.iter().position(|g| *g == gid).ok_or_else(|| {
                    fail(format!("Could not remove download result of GID#{gid}"))
                })?;
            q.stopped.remove(pos);
            q.tasks.remove(&gid);
            Ok(json!("OK"))
        }
        "aria2.getVersion" => Ok(json!({
            // Reported as aria2 so front ends enable their aria2 code paths.
            "version": "1.37.0",
            "enabledFeatures": ["HTTPS"],
            "pdlVersion": env!("CARGO_PKG_VERSION"),
        })),
        "aria2.getSessionInfo" => Ok(json!({"sessionId": d.session_id})),
        "aria2.saveSession" => {
            if d.session.is_none() {
                return Err(fail("Filename is not given."));
            }
            save_session(d).map_err(|e| fail(format!("{e:#}")))?;
            Ok(json!("OK"))
        }
        "aria2.shutdown" | "aria2.forceShutdown" => {
            d.shutdown.cancel();
            Ok(json!("OK"))
        }
        _ => Err(RpcError {
            code: 1,
            message: format!("No such method: {method}"),
        }),
    }
}

fn multicall(d: &Daemon, params: Vec<Value>) -> RpcResult {
    let Some(Value::Array(calls)) = params.into_iter().next() else {
        return Err(fail("system.multicall expects an array of calls"));
    };
    let results = calls
        .into_iter()
        .map(|c| {
            let method = c.get("methodName").and_then(Value::as_str);
            let params = match c.get("params") {
                Some(Value::Array(a)) => a.clone(),
                _ => Vec::new(),
            };
            match method {
                Some("system.multicall") => {
                    json!({"code": 1, "message": "Recursive system.multicall forbidden."})
                }
                Some(m) => match call_method(d, m, params) {
                    Ok(v) => json!([v]),
                    Err(e) => json!({"code": e.code, "message": e.message}),
                },
                None => json!({"code": 1, "message": "Missing methodName."}),
            }
        })
        .collect();
    Ok(Value::Array(results))
}

struct Params(Vec<Value>);

impl Params {
    fn get(&self, i: usize) -> Option<&Value> {
        self.0.get(i)
    }

    fn str(&self, i: usize) -> Result<&str, RpcError> {
        self.get(i)
            .and_then(Value::as_str)
            .ok_or_else(|| fail(format!("parameter {} must be a string", i + 1)))
    }

    fn gid(&self, i: usize) -> Result<&str, RpcError> {
        self.str(i)
    }

    fn int(&self, i: usize) -> Result<i64, RpcError> {
        match self.get(i) {
            Some(Value::Number(n)) => n.as_i64(),
            Some(Value::String(s)) => s.parse().ok(),
            _ => None,
        }
        .ok_or_else(|| fail(format!("parameter {} must be an integer", i + 1)))
    }

    fn object(&self, i: usize) -> Result<&Map<String, Value>, RpcError> {
        self.get(i)
            .and_then(Value::as_object)
            .ok_or_else(|| fail(format!("parameter {} must be a struct", i + 1)))
    }

    fn keys(&self, i: usize) -> Option<Vec<String>> {
        let keys: Vec<String> = self
            .get(i)?
            .as_array()?
            .iter()
            .filter_map(|k| k.as_str().map(str::to_string))
            .collect();
        (!keys.is_empty()).then_some(keys)
    }
}

fn opt_str<'a>(opts: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    opts.get(key).and_then(Value::as_str)
}

/// A path option; NUL cannot be part of a path, so it is refused up front
/// instead of failing the download later.
fn opt_path(opts: &Map<String, Value>, key: &str) -> Result<Option<PathBuf>, RpcError> {
    match opts.get(key) {
        None => Ok(None),
        Some(Value::String(s)) if !s.is_empty() && !s.contains('\0') => Ok(Some(PathBuf::from(s))),
        Some(_) => Err(fail(format!("{key} must be a non-empty path"))),
    }
}

/// aria2 options that pdl does not implement and whose absence would make
/// a "complete" mean something else (a checksum that was never verified).
/// Other unknown options are ignored, as front ends send many.
fn check_supported(opts: &Map<String, Value>) -> Result<(), RpcError> {
    if opts.get("checksum").is_some_and(|v| v.as_str() != Some("")) {
        return Err(fail("option checksum is not supported by pdl"));
    }
    Ok(())
}

fn opt_usize(opts: &Map<String, Value>, key: &str) -> Result<Option<usize>, RpcError> {
    match opts.get(key) {
        None => Ok(None),
        Some(Value::Number(n)) => n
            .as_u64()
            .map(|n| Some(n as usize))
            .ok_or_else(|| fail(format!("{key} must be a positive integer"))),
        Some(Value::String(s)) => s
            .parse()
            .map(Some)
            .map_err(|_| fail(format!("{key} must be a positive integer"))),
        Some(_) => Err(fail(format!("{key} must be a positive integer"))),
    }
}

fn opt_size(opts: &Map<String, Value>, key: &str) -> Result<Option<u64>, RpcError> {
    match opts.get(key) {
        None => Ok(None),
        Some(Value::Number(n)) => n
            .as_u64()
            .map(Some)
            .ok_or_else(|| fail(format!("{key} must be a size"))),
        Some(Value::String(s)) => parse_size(s).map(Some).map_err(fail),
        Some(_) => Err(fail(format!("{key} must be a size"))),
    }
}

/// aria2 accepts `header` as a single string or a list of `Name: value`.
fn opt_headers(opts: &Map<String, Value>) -> Result<Vec<(String, String)>, RpcError> {
    let mut lines: Vec<&str> = match opts.get("header") {
        None => Vec::new(),
        Some(Value::String(s)) => vec![s.as_str()],
        Some(Value::Array(a)) => a.iter().filter_map(Value::as_str).collect(),
        Some(_) => return Err(fail("header must be a string or a list")),
    };
    let referer = opt_str(opts, "referer").map(|r| format!("Referer: {r}"));
    let ua = opt_str(opts, "user-agent").map(|u| format!("User-Agent: {u}"));
    lines.extend(referer.as_deref());
    lines.extend(ua.as_deref());
    lines
        .into_iter()
        .map(|l| {
            l.split_once(':')
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
                .ok_or_else(|| fail(format!("bad header {l}")))
        })
        .collect()
}

fn find<'a>(q: &'a Queue, gid: &str) -> Result<&'a Task, RpcError> {
    q.tasks
        .get(gid)
        .ok_or_else(|| fail(format!("GID {gid} is not found")))
}

/// aria2's offset/num paging: a negative offset counts from the end and
/// returns entries in reverse order.
fn window(list: &[String], offset: i64, num: i64) -> Vec<&String> {
    let num = num.max(0) as usize;
    if offset >= 0 {
        list.iter().skip(offset as usize).take(num).collect()
    } else {
        let start = list.len() as i64 + offset;
        if start < 0 {
            return Vec::new();
        }
        list[..=start as usize].iter().rev().take(num).collect()
    }
}

fn add_uri(d: &Daemon, p: &Params) -> RpcResult {
    let uris = p
        .get(0)
        .and_then(Value::as_array)
        .ok_or_else(|| fail("parameter 1 must be an array of URIs"))?;
    let uri = uris
        .iter()
        .filter_map(Value::as_str)
        .next()
        .ok_or_else(|| fail("No URI to download."))?
        .to_string();
    if !(uri.starts_with("http://") || uri.starts_with("https://")) {
        return Err(fail(format!("unsupported URI: {uri}")));
    }
    let empty = Map::new();
    let opts = match p.get(1) {
        Some(Value::Object(o)) => o,
        None | Some(Value::Null) => &empty,
        Some(_) => return Err(fail("parameter 2 must be a struct")),
    };
    check_supported(opts)?;
    let position = match p.get(2) {
        Some(_) => Some(p.int(2)?.max(0) as usize),
        None => None,
    };
    let headers = opt_headers(opts)?;

    let name = match opt_path(opts, "out")? {
        Some(out) => {
            let out = out.as_path();
            // `out` is relative to `dir`; never let it climb out.
            if out.is_absolute()
                || out
                    .components()
                    .any(|c| matches!(c, std::path::Component::ParentDir))
                || out.file_name().is_none()
            {
                return Err(fail("out must be a relative file path inside dir"));
            }
            out.to_path_buf()
        }
        None => PathBuf::from(file_name_from_url(&uri)),
    };
    let base = match opt_path(opts, "dir")? {
        Some(dir) => dir,
        None => d.queue.lock().unwrap().globals.dir.clone(),
    };
    // Canonicalize the target directory so two spellings of one place
    // (`.`, an absolute path, a symlink) are recognised as the same file.
    let full = base.join(&name);
    let file_name = full.file_name().unwrap().to_owned();
    let parent = full.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)
        .and_then(|_| std::fs::canonicalize(parent))
        .map_err(|e| fail(format!("cannot use directory {}: {e}", parent.display())))
        .map(|p| (p, file_name))
        .and_then(|(dir, file_name)| {
            add_task(
                d,
                &uri,
                &dir,
                &file_name.to_string_lossy(),
                opts,
                position,
                headers,
            )
        })
}

/// Every file a download at `path` may create, truncate or delete.
fn touched(path: &Path) -> [PathBuf; 3] {
    [path.to_path_buf(), part_path(path), state_path(path)]
}

fn add_task(
    d: &Daemon,
    uri: &str,
    dir: &Path,
    name: &str,
    opts: &Map<String, Value>,
    position: Option<usize>,
    headers: Vec<(String, String)>,
) -> RpcResult {
    let uri = uri.to_string();
    let gid = {
        let mut q = d.queue.lock().unwrap();
        let in_use: Vec<PathBuf> = q
            .tasks
            .values()
            .filter(|t| !matches!(t.status, Status::Complete | Status::Error | Status::Removed))
            .flat_map(|t| touched(&t.path))
            .collect();
        let taken = |p: &Path| {
            touched(p).iter().any(|f| in_use.contains(f))
                || p.exists()
                // A `.part` without a checkpoint is someone's real file. With a
                // checkpoint it is a leftover partial download, reused so that
                // adding the same URL again resumes it (the engine verifies).
                || (part_path(p).exists() && !state_path(p).exists())
        };
        let path = unique_path(dir, name, taken);
        let connections = match opt_usize(opts, "max-connection-per-server")? {
            Some(n) => n,
            None => opt_usize(opts, "split")?.unwrap_or(q.globals.connections),
        };
        let min_split = opt_size(opts, "min-split-size")?.unwrap_or(q.globals.min_split);
        let max_download_limit =
            opt_size(opts, "max-download-limit")?.unwrap_or(q.globals.max_download_limit);
        let lowest_speed_limit =
            opt_size(opts, "lowest-speed-limit")?.unwrap_or(q.globals.lowest_speed_limit);
        // aria2's "add paused" (AriaNg, browser extensions).
        let paused = matches!(opts.get("pause"), Some(Value::String(v)) if v == "true")
            || matches!(opts.get("pause"), Some(Value::Bool(true)));
        q.next_gid = q.next_gid.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let gid = format!("{:016x}", q.next_gid);
        q.tasks.insert(
            gid.clone(),
            Task {
                gid: gid.clone(),
                uri,
                path,
                connections: connections.clamp(1, MAX_CONNECTIONS),
                min_split,
                headers,
                status: if paused {
                    Status::Paused
                } else {
                    Status::Waiting
                },
                total: 0,
                completed: 0,
                speed: 0,
                conns: 0,
                error: None,
                cancel: None,
                stop_as: None,
                auto_paused: false,
                limiter: Arc::new(RateLimiter::new(max_download_limit)),
                lowest_speed_limit,
            },
        );
        let pos = position.unwrap_or(q.waiting.len()).min(q.waiting.len());
        q.waiting.insert(pos, gid.clone());
        gid
    };
    mark_dirty(d);
    schedule(d);
    Ok(json!(gid))
}

fn pause(d: &Daemon, gid: &str) -> Result<(), RpcError> {
    let mut q = d.queue.lock().unwrap();
    let t = q
        .tasks
        .get_mut(gid)
        .ok_or_else(|| fail(format!("GID {gid} is not found")))?;
    match t.status {
        Status::Active if t.stop_as == Some(Status::Removed) => {
            Err(fail(format!("GID#{gid} is being removed")))
        }
        Status::Active => {
            t.stop_as = Some(Status::Paused);
            if let Some(c) = &t.cancel {
                c.cancel();
            }
            Ok(())
        }
        Status::Waiting => {
            t.status = Status::Paused;
            let info = HookInfo::of(t);
            drop(q);
            notify(d, "aria2.onDownloadPause", gid, info);
            mark_dirty(d);
            Ok(())
        }
        _ => Err(fail(format!("GID#{gid} cannot be paused now"))),
    }
}

fn unpause(d: &Daemon, gid: &str) -> Result<(), RpcError> {
    {
        let mut q = d.queue.lock().unwrap();
        let t = q
            .tasks
            .get_mut(gid)
            .ok_or_else(|| fail(format!("GID {gid} is not found")))?;
        if t.status != Status::Paused {
            return Err(fail(format!("GID#{gid} cannot be unpaused now")));
        }
        t.status = Status::Waiting;
    }
    mark_dirty(d);
    schedule(d);
    Ok(())
}

fn remove(d: &Daemon, gid: &str) -> RpcResult {
    let info = {
        let mut q = d.queue.lock().unwrap();
        let t = q
            .tasks
            .get_mut(gid)
            .ok_or_else(|| fail(format!("GID {gid} is not found")))?;
        let info = match t.status {
            Status::Active => {
                t.stop_as = Some(Status::Removed);
                if let Some(c) = &t.cancel {
                    c.cancel();
                }
                return Ok(json!(gid));
            }
            Status::Waiting | Status::Paused => {
                t.status = Status::Removed;
                let info = HookInfo::of(t);
                q.waiting.retain(|g| g != gid);
                push_stopped(&mut q, gid.to_string());
                info
            }
            _ => return Err(fail(format!("Active Download not found for GID#{gid}"))),
        };
        // Still under the lock, so no new task can claim this path meanwhile.
        remove_partial(&info.path);
        info
    };
    notify(d, "aria2.onDownloadStop", gid, info);
    mark_dirty(d);
    Ok(json!(gid))
}

fn change_position(d: &Daemon, gid: &str, pos: i64, how: &str) -> RpcResult {
    let mut q = d.queue.lock().unwrap();
    let cur = q
        .waiting
        .iter()
        .position(|g| g == gid)
        .ok_or_else(|| fail(format!("GID#{gid} not found in the waiting queue.")))?;
    let len = q.waiting.len() as i64;
    let target = match how {
        "POS_SET" => pos,
        "POS_CUR" => (cur as i64).saturating_add(pos),
        "POS_END" => (len - 1).saturating_add(pos),
        _ => return Err(fail(format!("unknown how: {how}"))),
    }
    .clamp(0, len - 1) as usize;
    let g = q.waiting.remove(cur);
    q.waiting.insert(target, g);
    drop(q);
    mark_dirty(d);
    Ok(json!(target))
}

fn task_options(t: &Task) -> Value {
    let mut o = json!({
        "dir": t.path.parent().map(|p| p.to_string_lossy().into_owned()).unwrap_or_default(),
        "out": t.path.file_name().map(|p| p.to_string_lossy().into_owned()).unwrap_or_default(),
        "max-connection-per-server": t.connections.to_string(),
        "split": t.connections.to_string(),
        "min-split-size": t.min_split.to_string(),
        "max-download-limit": t.limiter.rate().to_string(),
        "lowest-speed-limit": t.lowest_speed_limit.to_string(),
    });
    if !t.headers.is_empty() {
        o["header"] = json!(
            t.headers
                .iter()
                .map(|(k, v)| format!("{k}: {v}"))
                .collect::<Vec<_>>()
        );
    }
    o
}

fn uris_json(t: &Task) -> Value {
    json!([{"uri": t.uri, "status": "used"}])
}

fn files_json(t: &Task) -> Value {
    json!([{
        "index": "1",
        "path": t.path.to_string_lossy(),
        "length": t.total.to_string(),
        "completedLength": t.completed.to_string(),
        "selected": "true",
        "uris": uris_json(t),
    }])
}

fn status_json(_q: &Queue, t: &Task, keys: Option<Vec<String>>) -> Value {
    let mut s = json!({
        "gid": t.gid,
        "status": t.status.as_str(),
        "totalLength": t.total.to_string(),
        "completedLength": t.completed.to_string(),
        "uploadLength": "0",
        "downloadSpeed": t.speed.to_string(),
        "uploadSpeed": "0",
        "connections": t.conns.to_string(),
        "numPieces": "0",
        "pieceLength": "1048576",
        "dir": t.path.parent().map(|p| p.to_string_lossy().into_owned()).unwrap_or_default(),
        "files": files_json(t),
    });
    match (&t.error, t.status) {
        (Some(e), _) => {
            s["errorCode"] = json!("1");
            s["errorMessage"] = json!(e);
        }
        (None, Status::Complete) => {
            s["errorCode"] = json!("0");
            s["errorMessage"] = json!("");
        }
        _ => {}
    }
    match keys {
        Some(keys) => {
            let obj = s.as_object().unwrap();
            Value::Object(
                keys.iter()
                    .filter_map(|k| obj.get(k).map(|v| (k.clone(), v.clone())))
                    .collect(),
            )
        }
        None => s,
    }
}
