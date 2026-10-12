use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use clap::{Parser, Subcommand};
use pdl_testserver::{Config, first_mismatch, spawn};

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Serve the test file.
    Serve {
        #[arg(long, default_value = "127.0.0.1:8090")]
        addr: SocketAddr,
        /// File size in MiB.
        #[arg(long, default_value_t = 64)]
        size_mib: u64,
        /// Per-connection limit in KiB/s.
        #[arg(long)]
        rate_kib: Option<u64>,
        #[arg(long)]
        no_range: bool,
        #[arg(long)]
        max_conns: Option<usize>,
        #[arg(long)]
        drop_after: Option<u64>,
        /// Redirect to signed URLs that expire after this many requests.
        #[arg(long)]
        signed_redirect: Option<u64>,
        /// Throttle the N-th body response to --slow-rate-kib. Repeatable.
        #[arg(long = "slow-request")]
        slow_requests: Vec<u64>,
        #[arg(long, default_value_t = 16)]
        slow_rate_kib: u64,
        /// Per-connection speeds in KiB/s, cycled over connections: "4096,512".
        #[arg(long, value_delimiter = ',')]
        conn_rates_kib: Vec<u64>,
        /// Delay before each response, in milliseconds.
        #[arg(long, default_value_t = 0)]
        latency_ms: u64,
        /// Simulated round trip for handshakes and slow start, milliseconds.
        #[arg(long, default_value_t = 0)]
        rtt_ms: u64,
        /// Shared bottleneck for all connections, KiB/s.
        #[arg(long)]
        link_kib: Option<u64>,
    },
    /// Check that a downloaded file matches the served contents.
    Verify { file: PathBuf, size_mib: u64 },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Cli::parse().cmd {
        Cmd::Serve {
            addr,
            size_mib,
            rate_kib,
            no_range,
            max_conns,
            drop_after,
            signed_redirect,
            slow_requests,
            slow_rate_kib,
            conn_rates_kib,
            latency_ms,
            rtt_ms,
            link_kib,
        } => {
            let config = Config {
                size: size_mib << 20,
                rate_per_conn: rate_kib.map(|k| k << 10),
                range: !no_range,
                max_conns,
                drop_after,
                signed_redirect,
                slow_requests,
                slow_rate: slow_rate_kib << 10,
                conn_rates: conn_rates_kib.iter().map(|k| k << 10).collect(),
                latency: std::time::Duration::from_millis(latency_ms),
                rtt: std::time::Duration::from_millis(rtt_ms),
                link: link_kib.map(|k| k << 10),
                ..Config::default()
            };
            let server = spawn(config, addr).await?;
            eprintln!("serving {}", server.url());
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                // Final counters on Ctrl-C / SIGINT, for benchmarks.
                let last = tokio::select! {
                    _ = tick.tick() => false,
                    _ = tokio::signal::ctrl_c() => true,
                };
                let s = &server.stats;
                eprintln!(
                    "requests={} active={} max_active={} rejected={} sent={}MiB",
                    s.requests.load(Ordering::Relaxed),
                    s.active.load(Ordering::Relaxed),
                    s.max_active.load(Ordering::Relaxed),
                    s.rejected.load(Ordering::Relaxed),
                    s.bytes_sent.load(Ordering::Relaxed) >> 20,
                );
                if last {
                    println!(
                        "requests={} sent={}",
                        s.requests.load(Ordering::Relaxed),
                        s.bytes_sent.load(Ordering::Relaxed)
                    );
                    return Ok(());
                }
            }
        }
        Cmd::Verify { file, size_mib } => {
            let data = std::fs::read(&file)?;
            anyhow::ensure!(
                data.len() as u64 == size_mib << 20,
                "size {} != {}",
                data.len(),
                size_mib << 20
            );
            match first_mismatch(&data) {
                None => println!("ok"),
                Some(off) => anyhow::bail!("mismatch at offset {off}"),
            }
            Ok(())
        }
    }
}
