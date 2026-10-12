//! Token-bucket rate limiting shared by any number of connections.
//!
//! Callers take bytes first and then wait off any debt, so a limiter never
//! stalls a chunk that has already arrived; holding back the next read is
//! what slows the sender (TCP flow control does the rest).

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Burst allowance, as a fraction of a second's worth of the rate.
const BURST: f64 = 0.2;
/// Smallest burst, so small limits do not stall on a single chunk.
const MIN_BURST: f64 = 64.0 * 1024.0;

#[derive(Debug)]
pub struct RateLimiter {
    /// Bytes per second; 0 means unlimited.
    rate: AtomicU64,
    bucket: Mutex<Bucket>,
    /// Wakes callers waiting off debt when the rate changes; their debt was
    /// computed under the old rate.
    changed: tokio::sync::Notify,
}

#[derive(Debug)]
struct Bucket {
    tokens: f64,
    last: Instant,
}

impl RateLimiter {
    pub fn new(rate: u64) -> Self {
        RateLimiter {
            rate: AtomicU64::new(rate),
            bucket: Mutex::new(Bucket {
                tokens: 0.0,
                last: Instant::now(),
            }),
            changed: tokio::sync::Notify::new(),
        }
    }

    pub fn rate(&self) -> u64 {
        self.rate.load(Ordering::Relaxed)
    }

    /// Changes the limit; takes effect for the next bytes taken.
    pub fn set_rate(&self, rate: u64) {
        self.rate.store(rate, Ordering::Relaxed);
        let mut b = self.bucket.lock().unwrap();
        // Debt accrued under the old rate must not carry over.
        b.tokens = 0.0;
        b.last = Instant::now();
        drop(b);
        self.changed.notify_waiters();
    }

    /// Takes `n` bytes and returns how long the caller must wait.
    fn reserve(&self, n: u64) -> Duration {
        let rate = self.rate();
        if rate == 0 {
            return Duration::ZERO;
        }
        let rate = rate as f64;
        let mut b = self.bucket.lock().unwrap();
        let now = Instant::now();
        let refill = now.duration_since(b.last).as_secs_f64() * rate;
        b.tokens = (b.tokens + refill).min((rate * BURST).max(MIN_BURST));
        b.last = now;
        b.tokens -= n as f64;
        if b.tokens >= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(-b.tokens / rate)
        }
    }

    pub async fn acquire(&self, n: u64) {
        // Register before reserving so a concurrent set_rate is not missed.
        let changed = self.changed.notified();
        let wait = self.reserve(n);
        if !wait.is_zero() {
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = changed => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unlimited_never_waits() {
        let l = RateLimiter::new(0);
        assert_eq!(l.reserve(1 << 30), Duration::ZERO);
    }

    #[test]
    fn debt_turns_into_wait() {
        let l = RateLimiter::new(1 << 20);
        // Far more than the burst: about one second of debt.
        let w = l.reserve(1 << 20);
        assert!(
            w > Duration::from_millis(900) && w <= Duration::from_secs(1),
            "{w:?}"
        );
        l.set_rate(0);
        assert_eq!(l.reserve(1 << 20), Duration::ZERO);
    }
}
