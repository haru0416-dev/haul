use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand};
use haul::util::{file_name_from_url, parse_size};
use haul::{Event, Options, daemon, download};
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(name = "haul", version, about = "Segmented parallel HTTP downloader")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Download a URL.
    Get {
        url: String,
        /// Output path (default: last URL path segment).
        #[arg(short, long)]
        output: Option<PathBuf>,
        /// Maximum concurrent connections (1-64). haul starts with one and adds
        /// more only while they raise throughput.
        #[arg(short = 'x', long, default_value_t = 16, value_parser = conns_range)]
        connections: usize,
        /// Before speeds are known, do not split ranges smaller than twice
        /// this size (e.g. 1M, 512K). Later splits follow measured speeds.
        #[arg(short = 'k', long, default_value = "1M", value_parser = parse_size)]
        min_split: u64,
        /// Extra request header, "Name: value". Repeatable.
        #[arg(short = 'H', long = "header")]
        headers: Vec<String>,
        /// Cap this download's speed, e.g. 500K or 2M (bytes per second).
        #[arg(long, value_parser = parse_size)]
        max_download_limit: Option<u64>,
        /// Reconnect any connection slower than this (bytes per second) over
        /// 10 seconds, e.g. 50K.
        #[arg(long, value_parser = parse_size, default_value = "0")]
        lowest_speed_limit: u64,
        /// Emit newline-delimited JSON events on stdout instead of a progress line.
        #[arg(long)]
        json: bool,
    },
    /// Run headless, controlled over aria2-compatible JSON-RPC
    /// (HTTP POST and WebSocket at /jsonrpc).
    Daemon {
        /// Address to listen on. Keep it on loopback unless a secret is set.
        #[arg(long, default_value = "127.0.0.1:6800")]
        listen: std::net::SocketAddr,
        /// RPC secret; clients pass "token:<secret>" as the first parameter.
        /// Defaults to $HAUL_RPC_SECRET.
        #[arg(long, env = "HAUL_RPC_SECRET", hide_env_values = true)]
        secret: Option<String>,
        /// Default download directory.
        #[arg(short, long, default_value = ".")]
        dir: PathBuf,
        /// Downloads that run at the same time.
        #[arg(short = 'j', long, default_value_t = 5)]
        max_concurrent: usize,
        /// Default maximum connections per download (1-64); each download
        /// adds connections only while they raise its throughput.
        #[arg(short = 'x', long, default_value_t = 16, value_parser = conns_range)]
        connections: usize,
        #[arg(short = 'k', long, default_value = "1M", value_parser = parse_size)]
        min_split: u64,
        /// Allow a web origin, e.g. http://127.0.0.1:8080, to call the RPC
        /// (for browser-based front ends like AriaNg). Repeatable.
        /// Browser extensions and non-browser clients need no entry.
        #[arg(long = "allow-origin")]
        allowed_origins: Vec<String>,
        /// Allow every web origin. Requires --secret, since otherwise any page
        /// the user visits could add downloads anywhere on disk.
        #[arg(long)]
        allow_origin_all: bool,
        /// Keep unfinished downloads in this file and restore them on start.
        #[arg(long)]
        session: Option<PathBuf>,
        /// Daemon-wide speed cap, e.g. 10M (bytes per second).
        #[arg(long, value_parser = parse_size, default_value = "0")]
        max_overall_download_limit: u64,
        /// Default speed cap for each new download.
        #[arg(long, value_parser = parse_size, default_value = "0")]
        max_download_limit: u64,
        /// Default lowest per-connection speed before reconnecting, e.g. 50K.
        #[arg(long, value_parser = parse_size, default_value = "0")]
        lowest_speed_limit: u64,
        /// Run COMMAND <gid> 1 <path> when a download starts (aria2-compatible).
        #[arg(long, value_name = "COMMAND")]
        on_download_start: Option<PathBuf>,
        /// ... when a download is paused.
        #[arg(long, value_name = "COMMAND")]
        on_download_pause: Option<PathBuf>,
        /// ... when a download is removed.
        #[arg(long, value_name = "COMMAND")]
        on_download_stop: Option<PathBuf>,
        /// ... when a download completes.
        #[arg(long, value_name = "COMMAND")]
        on_download_complete: Option<PathBuf>,
        /// ... when a download fails.
        #[arg(long, value_name = "COMMAND")]
        on_download_error: Option<PathBuf>,
    },
}

fn conns_range(s: &str) -> Result<usize, String> {
    match s.parse::<usize>() {
        Ok(n) if (1..=daemon::MAX_CONNECTIONS).contains(&n) => Ok(n),
        _ => Err(format!("must be 1-{}", daemon::MAX_CONNECTIONS)),
    }
}

fn human(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    format!("{v:.1}{}", UNITS[u])
}

/// Two runtime threads keep up with a saturated local link while costing less
/// CPU and memory than one per core (fewer idle wakeups, fewer malloc arenas).
/// `TOKIO_WORKER_THREADS` overrides it.
const DEFAULT_WORKER_THREADS: usize = 2;

fn main() -> anyhow::Result<()> {
    let threads = std::env::var("TOKIO_WORKER_THREADS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n: &usize| n > 0)
        .unwrap_or(DEFAULT_WORKER_THREADS);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()?
        .block_on(run(Cli::parse()))
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    match cli.cmd {
        Cmd::Get {
            url,
            output,
            connections,
            min_split,
            headers,
            max_download_limit,
            lowest_speed_limit,
            json,
        } => {
            let mut opts = Options::new(
                &url,
                output.unwrap_or_else(|| PathBuf::from(file_name_from_url(&url))),
            );
            opts.max_conns = connections;
            opts.min_split = min_split;
            opts.lowest_speed_limit = lowest_speed_limit;
            if let Some(rate) = max_download_limit {
                opts.limiters
                    .push(std::sync::Arc::new(haul::ratelimit::RateLimiter::new(rate)));
            }
            for h in headers {
                let (k, v) = h
                    .split_once(':')
                    .with_context(|| format!("header must be \"Name: value\": {h}"))?;
                opts.headers
                    .push((k.trim().to_string(), v.trim().to_string()));
            }
            get(opts, json).await
        }
        Cmd::Daemon {
            listen,
            secret,
            dir,
            max_concurrent,
            connections,
            min_split,
            allowed_origins,
            allow_origin_all,
            session,
            max_overall_download_limit,
            max_download_limit,
            lowest_speed_limit,
            on_download_start,
            on_download_pause,
            on_download_stop,
            on_download_complete,
            on_download_error,
        } => {
            if secret.is_none() && !listen.ip().is_loopback() {
                anyhow::bail!("refusing to listen on {listen} without --secret");
            }
            if secret.is_none() && allow_origin_all {
                anyhow::bail!("--allow-origin-all requires --secret");
            }
            let handle = daemon::spawn(daemon::Config {
                listen,
                secret,
                dir,
                max_concurrent,
                connections,
                min_split,
                allow_origin_all,
                allowed_origins,
                hooks: daemon::Hooks {
                    on_start: on_download_start,
                    on_pause: on_download_pause,
                    on_stop: on_download_stop,
                    on_complete: on_download_complete,
                    on_error: on_download_error,
                },
                max_overall_download_limit,
                max_download_limit,
                lowest_speed_limit,
                session,
            })
            .await?;
            eprintln!("haul daemon listening on {}", handle.url());
            tokio::select! {
                _ = handle_shutdown_signal() => {}
                _ = handle.shutdown_requested() => {}
            }
            eprintln!("shutting down; pausing active downloads");
            handle.shutdown();
            handle.wait().await;
            Ok(())
        }
    }
}

async fn handle_shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

async fn get(opts: Options, json: bool) -> anyhow::Result<()> {
    let cancel = CancellationToken::new();
    {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                cancel.cancel();
            }
        });
    }

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let printer = tokio::spawn(async move {
        let mut stderr = std::io::stderr();
        while let Some(ev) = rx.recv().await {
            if json {
                // A consumer that went away must not take the download with it.
                let mut out = std::io::stdout().lock();
                let _ = writeln!(out, "{}", serde_json::to_string(&ev).unwrap());
                let _ = out.flush();
                continue;
            }
            match ev {
                Event::Started {
                    output,
                    size,
                    ranged,
                    resumed_bytes,
                    ..
                } => {
                    let size = size.map(human).unwrap_or_else(|| "unknown size".into());
                    let mode = if ranged { "segmented" } else { "single stream" };
                    eprint!("{} ({size}, {mode}", output.display());
                    if resumed_bytes > 0 {
                        eprint!(", resuming at {}", human(resumed_bytes));
                    }
                    eprintln!(")");
                }
                Event::Progress {
                    downloaded,
                    size,
                    speed,
                    connections,
                } => {
                    let pct = size
                        .filter(|&s| s > 0)
                        .map(|s| format!("{:5.1}% ", downloaded as f64 * 100.0 / s as f64))
                        .unwrap_or_default();
                    eprint!(
                        "\r\x1b[K{pct}{} {}/s conns={connections}",
                        human(downloaded),
                        human(speed)
                    );
                    let _ = stderr.flush();
                }
                Event::Warning { message } => eprintln!("\r\x1b[Kwarning: {message}"),
                Event::Finished {
                    size, elapsed_ms, ..
                } => {
                    let secs = Duration::from_millis(elapsed_ms).as_secs_f64();
                    eprintln!(
                        "\r\x1b[Kdone: {} in {secs:.2}s ({}/s)",
                        human(size),
                        human((size as f64 / secs.max(1e-3)) as u64)
                    );
                }
            }
        }
    });

    let result = download(opts, Some(tx), cancel).await;
    let _ = printer.await;
    if let Err(e) = result {
        if json {
            let _ = writeln!(
                std::io::stdout(),
                "{}",
                serde_json::json!({"event": "error", "message": format!("{e:#}")})
            );
        }
        eprintln!("\rerror: {e:#}");
        std::process::exit(1);
    }
    Ok(())
}
