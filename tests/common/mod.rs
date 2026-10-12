//! Helpers shared by the integration tests (each test binary uses a subset).
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use haul::Options;
use haul::engine::{part_path, state_path};
use haul_testserver::first_mismatch;

/// A fresh directory for one test, on disk under target/ (not a RAM-backed
/// /tmp), separate per test binary.
pub fn tmpdir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("haul-{}-{name}", env!("CARGO_CRATE_NAME")));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Download options with small splits and frequent checkpoints.
pub fn opts(url: String, out: &Path) -> Options {
    let mut o = Options::new(url, out);
    o.min_split = 256 << 10;
    o.checkpoint_interval = Duration::from_millis(100);
    o
}

/// `path` holds exactly the test server's file of `size` bytes, and no
/// partial download is left next to it.
pub fn assert_file(path: &Path, size: u64) {
    let data = std::fs::read(path).unwrap();
    assert_eq!(data.len() as u64, size);
    assert_eq!(first_mismatch(&data), None);
    assert!(!part_path(path).exists());
    assert!(!state_path(path).exists());
}
