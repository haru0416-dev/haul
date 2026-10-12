# haul

A segmented parallel HTTP downloader. Use it as a command (`haul get`) or as
a headless daemon that speaks aria2's JSON-RPC, so existing aria2 front ends
such as AriaNg and browser extensions can drive it.

```console
$ haul get https://go.dev/dl/go1.27.2.linux-amd64.tar.gz
go1.27.2.linux-amd64.tar.gz (67.3MiB, segmented)
done: 67.3MiB in 0.99s (68.0MiB/s)
```

## What it does differently

- **It opens only as many connections as help.** A download starts on one
  connection and multiplies the count by four while throughput rises by at
  least 25%. If a step does not pay off, haul closes the extra connections
  again. `-x` sets the ceiling, not the number used. On a far-away server
  (RTT 226 ms), sixteen connections were slower than one; against a server
  that throttles each connection, more connections are worth it.
- **Splits aim to finish every range together.** When a connection runs out
  of work, it takes the share of the range predicted to finish last that makes
  both end at the same moment. The prediction uses each connection's measured
  speed and latency, and for a new connection, the handshake and TCP slow start.
- **Stalled connections are rescued quickly.** A connection that stops
  delivering loses its remaining range to the others within a second or two,
  instead of holding it until the 30 s read timeout.
- **Downloads resume and are verified.** Progress is checkpointed next to the
  output (`file.part`, `file.part.state`). A resume sends `If-Range` with the
  saved validator, so a changed file starts over instead of being stitched
  together from two versions. A size mismatch is an error, never a short file.
- **It is careful with what it sends and writes.** Credentials (`Authorization`,
  `Proxy-Authorization`, `Cookie`) are not forwarded when a redirect leaves the origin. A partial file
  is locked, so two runs cannot write the same download. Expired signed
  redirect targets (as on many CDNs) are re-resolved from the original URL.

## Install

Requires Rust (edition 2024). Developed and tested on Linux.

```sh
cargo install --path .
```

The crate is named `haul-dl` because `haul` is taken on crates.io; the
command and the library are both `haul`.

## Downloading

```sh
haul get URL                       # saves to the last path segment of URL
haul get URL -o out.iso            # choose the output path
haul get URL -x 4                  # use at most 4 connections (1-64, default 16)
haul get URL -H 'Authorization: Bearer …'   # extra header, repeatable
haul get URL --max-download-limit 2M        # cap the speed (bytes/s)
haul get URL --lowest-speed-limit 50K       # reconnect connections slower than this over 10 s
haul get URL --json                # newline-delimited JSON events on stdout
```

`-k`/`--min-split` (default `1M`) only matters before any speed is known:
ranges smaller than twice this size are not split. Later splits follow the
measured speeds.

haul refuses to overwrite an existing file. Interrupt a download with Ctrl-C
and run the same command again to resume it:

```console
$ haul get -o file.bin URL
^C
error: cancelled
$ haul get -o file.bin URL
file.bin (8.0MiB, segmented, resuming at 2.2MiB)
done: 8.0MiB in 4.02s (2.0MiB/s)
```

With `--json`, each line is an event:

```json
{"event":"started","url":"http://…/file.bin","output":"file.bin","size":8388608,"ranged":true,"resumed_bytes":0}
{"event":"progress","downloaded":4538368,"size":8388608,"speed":9086457,"connections":4}
{"event":"warning","message":"…"}
{"event":"finished","output":"file.bin","size":8388608,"elapsed_ms":876}
{"event":"error","message":"…"}
```

The exit status is 0 on success and 1 on failure. `TOKIO_WORKER_THREADS`
changes the number of runtime threads (default 2).

## The daemon

```sh
HAUL_RPC_SECRET=… haul daemon -d ~/Downloads --session ~/.local/state/haul.session
```

This listens on `http://127.0.0.1:6800/jsonrpc`, the address aria2 uses.
Requests are accepted as HTTP `POST` and over WebSocket on the same path;
WebSocket clients also receive the `aria2.onDownload*` notifications. Point
an aria2 front end at it with the same secret.

```console
$ curl -s http://127.0.0.1:6800/jsonrpc -d '{"jsonrpc":"2.0","id":1,"method":"aria2.addUri",
    "params":["token:SECRET",["https://example.com/file.iso"],{"out":"file.iso"}]}'
{"id":1,"jsonrpc":"2.0","result":"e56b65e695c774e3"}
```

| Option | Default | Meaning |
|---|---|---|
| `--listen ADDR` | `127.0.0.1:6800` | Address to listen on. A non-loopback address requires a secret. |
| `--secret S` / `HAUL_RPC_SECRET` | none | Clients pass `token:S` as the first parameter. |
| `-d, --dir DIR` | `.` | Default download directory. |
| `-j, --max-concurrent N` | 5 | Downloads that run at the same time. |
| `-x, --connections N` | 16 | Default maximum connections per download. |
| `-k, --min-split SIZE` | `1M` | As for `haul get`. |
| `--session FILE` | none | Keep unfinished downloads here and restore them on start. |
| `--max-overall-download-limit SIZE` | 0 (off) | Speed cap for the whole daemon. |
| `--max-download-limit SIZE` | 0 (off) | Default speed cap for each download. |
| `--lowest-speed-limit SIZE` | 0 (off) | Default reconnect threshold per connection. |
| `--allow-origin ORIGIN` | none | Let a web page at ORIGIN (e.g. a self-hosted AriaNg) call the RPC. Repeatable. |
| `--allow-origin-all` | off | Let every web origin call the RPC. Requires a secret. |
| `--on-download-{start,pause,stop,complete,error} CMD` | none | Hooks, see below. |

Requests without an `Origin` header (curl, native apps) and requests from
browser extensions are always accepted; web pages need `--allow-origin`. The
daemon pauses every active download on SIGINT, SIGTERM or `aria2.shutdown`,
and with `--session` resumes them on the next start.

### Hooks

As in aria2, a hook is run as `CMD <gid> 1 <path>`. It also gets
`HAUL_EVENT` (`start`, `pause`, `stop`, `complete` or `error`), `HAUL_GID`,
`HAUL_PATH`, `HAUL_URI` and `HAUL_ERROR` in its environment.
`HAUL_RPC_SECRET` is removed from that environment. A pause caused by
shutting the daemon down does not run the pause hook.

### Compatibility with aria2

Implemented methods: `addUri`, `remove`, `forceRemove`, `pause`, `pauseAll`,
`forcePause`, `forcePauseAll`, `unpause`, `unpauseAll`, `tellStatus`,
`getUris`, `getFiles`, `tellActive`, `tellWaiting`, `tellStopped`,
`changePosition`, `getOption`, `changeOption`, `getGlobalOption`,
`changeGlobalOption`, `getGlobalStat`, `purgeDownloadResult`,
`removeDownloadResult`, `getVersion`, `getSessionInfo`, `shutdown`,
`forceShutdown`, `saveSession` (all `aria2.`-prefixed), and
`system.multicall`, `system.listMethods`, `system.listNotifications`.

`addUri` understands `dir`, `out`, `header`, `referer`, `user-agent`,
`max-connection-per-server` (or `split`), `min-split-size`,
`max-download-limit`, `lowest-speed-limit` and `pause`. Other options that
front ends send are ignored, except `checksum`, which is refused. Accepting it
would report a download as complete without the checksum ever being verified.

Differences to expect:

- HTTP and HTTPS only: no FTP, BitTorrent or Metalink. `addUri` uses the
  first URI and ignores the others.
- `getVersion` reports aria2 version `1.37.0` so front ends enable their
  features, plus `haulVersion`.
- A name that is already taken gets a numbered variant (`file.1.iso`) instead
  of an error. Adding a URL whose partial download is still on disk resumes it.

## How it works

The file is a list of ranges, each owned by one worker that streams it with a
single `Range` request over HTTP/1.1. HTTP/2 is not offered: its flow-control
window held one connection to about 60 MB/s on Google's CDN, where HTTP/1.1
reached 100–140 MB/s. Received chunks are written with `pwritev` without
copying, writeback starts every 8 MiB, and a checkpoint never claims bytes
that are not yet on disk. The module documentation in `src/engine.rs`
describes the scheduler in detail.

## Development

```sh
cargo test --release                       # unit and integration tests
CHAOS_N=300 cargo test --release --test chaos -- --ignored --nocapture
```

The tests run against `testserver/`, a local origin that misbehaves on
demand: per-connection and shared bandwidth limits, latency, simulated round
trips with slow start, dropped connections, stalls, expiring signed redirects,
ignored `Range` and `If-Range` headers. It also runs on its own for
benchmarks:

```sh
cargo run --release -p haul-testserver -- serve --size-mib 256 --rtt-ms 100 --link-kib 20480
cargo run --release -p haul-testserver -- verify file.bin 256
```

The chaos test downloads under randomly combined faults and fails on any
result that is not either the exact file or an error.
