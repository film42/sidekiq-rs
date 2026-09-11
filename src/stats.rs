use crate::RedisPool;
use rand::RngCore;
use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone)]
pub struct Counter {
    count: Arc<AtomicUsize>,
}

impl Counter {
    #[must_use]
    pub fn new(n: usize) -> Self {
        Self {
            count: Arc::new(AtomicUsize::new(n)),
        }
    }

    #[must_use]
    pub fn value(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    pub fn decrby(&self, n: usize) {
        self.count.fetch_sub(n, Ordering::SeqCst);
    }

    pub fn incrby(&self, n: usize) {
        self.count.fetch_add(n, Ordering::SeqCst);
    }
}

struct ProcessStats {
    rtt_us: String,
    quiet: String,
    busy: usize,
    beat: f64,
    concurrency: usize,
    info: ProcessInfo,
    rss: String,
}

#[derive(Serialize)]
struct ProcessInfo {
    hostname: String,
    identity: String,
    started_at: f64,
    pid: u32,
    tag: String,
    concurrency: usize,
    queues: Vec<String>,
    labels: Vec<String>,
    version: String,
    embedded: bool,
}

pub struct StatsPublisher {
    hostname: String,
    identity: String,
    queues: Vec<String>,
    started_at: chrono::DateTime<chrono::Utc>,
    busy_jobs: Counter,
    concurrency: usize,
}

fn generate_identity(hostname: &String) -> String {
    let pid = std::process::id();
    let mut bytes = [0u8; 12];
    rand::rng().fill_bytes(&mut bytes);
    let nonce = hex::encode(bytes);

    format!("{hostname}:{pid}:{nonce}")
}

/// A per-worker "thread id", used as the field key in the `<identity>:work`
/// hash that backs the Sidekiq web "Busy" page. Mirrors Ruby's `Sidekiq.tid`
/// (a short, process-unique token); generated once per worker so the same
/// slot is reused as that worker churns through jobs.
pub(crate) fn generate_tid() -> String {
    let mut bytes = [0u8; 6];
    rand::rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

impl StatsPublisher {
    #[must_use]
    pub fn new(
        hostname: String,
        queues: Vec<String>,
        busy_jobs: Counter,
        concurrency: usize,
    ) -> Self {
        let identity = generate_identity(&hostname);
        let started_at = chrono::Utc::now();

        Self {
            hostname,
            identity,
            queues,
            started_at,
            busy_jobs,
            concurrency,
        }
    }

    // 127.0.0.1:6379> hkeys "yolo_app:DESKTOP-UMSV21A:107068:5075431aeb06"
    // 1) "rtt_us"
    // 2) "quiet"
    // 3) "busy"
    // 4) "beat"
    // 5) "info"
    // 6) "rss"
    // 127.0.0.1:6379> hget "yolo_app:DESKTOP-UMSV21A:107068:5075431aeb06" info
    // "{\"hostname\":\"DESKTOP-UMSV21A\",\"started_at\":1658082501.5606177,\"pid\":107068,\"tag\":\"\",\"concurrency\":10,\"queues\":[\"ruby:v1_statistics\",\"ruby:v2_statistics\"],\"labels\":[],\"identity\":\"DESKTOP-UMSV21A:107068:5075431aeb06\"}"
    // 127.0.0.1:6379> hget "yolo_app:DESKTOP-UMSV21A:107068:5075431aeb06" irss
    // (nil)
    pub async fn publish_stats(&self, redis: RedisPool) -> Result<(), Box<dyn std::error::Error>> {
        let stats = self.create_process_stats().await?;
        let mut conn = redis.get().await?;
        let _: () = conn
            .cmd_with_key("HSET", self.identity.clone())
            .arg("info")
            .arg(serde_json::to_string(&stats.info)?)
            .arg("concurrency")
            .arg(stats.concurrency)
            .arg("busy")
            .arg(stats.busy)
            .arg("beat")
            .arg(stats.beat)
            .arg("rtt_us")
            .arg(stats.rtt_us)
            .arg("quiet")
            .arg(stats.quiet)
            .arg("rss")
            .arg(stats.rss)
            .query_async::<()>(conn.unnamespaced_borrow_mut())
            .await?;

        conn.expire(self.identity.clone(), 60).await?;

        conn.sadd("processes".to_string(), self.identity.clone())
            .await?;

        // Keep the WorkSet hash (written per-job in `Processor::process_one_tick_once`)
        // on the same 60s heartbeat TTL, so a crashed process's in-flight entries
        // self-expire and long-running jobs stay visible while the process is alive.
        // EXPIRE on a missing key (no jobs in flight) is a harmless no-op.
        conn.expire(format!("{}:work", self.identity), 60).await?;

        Ok(())
    }

    /// Remove this process from the `processes` set and delete the heartbeat hash
    /// and the WorkSet (`<identity>:work`).
    ///
    /// Mirrors Ruby Sidekiq's `Launcher#clear_heartbeat`, which pipelines:
    ///   `SREM processes [identity]`
    ///   `UNLINK identity:work`
    ///
    /// Without this, stale entries accumulate in the `processes` set until the
    /// heartbeat hash's 60s TTL expires — but the set membership has no TTL and
    /// never self-cleans.
    pub(crate) async fn deregister(&self, redis: RedisPool) -> crate::Result<()> {
        let mut conn = redis.get().await?;
        conn.srem_and_unlink(
            "processes".to_string(),
            self.identity.clone(),
            self.identity.clone(),
        )
        .await?;
        conn.unlink(format!("{}:work", self.identity)).await?;
        Ok(())
    }

    pub(crate) fn identity(&self) -> &str {
        &self.identity
    }

    async fn create_process_stats(&self) -> Result<ProcessStats, Box<dyn std::error::Error>> {
        let rss_in_kb = format!("{}", get_rss_kb());

        Ok(ProcessStats {
            rtt_us: "0".into(),
            busy: self.busy_jobs.value(),
            quiet: "false".into(),
            rss: rss_in_kb,
            concurrency: self.concurrency,
            beat: chrono::Utc::now().timestamp_millis() as f64 / 1000.0,
            info: ProcessInfo {
                concurrency: self.concurrency,
                hostname: self.hostname.clone(),
                identity: self.identity.clone(),
                queues: self.queues.clone(),
                started_at: self.started_at.timestamp_millis() as f64 / 1000.0,
                pid: std::process::id(),
                labels: vec![],
                tag: String::new(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                embedded: false,
            },
        })
    }
}

/// Get RSS (resident set size) in kilobytes for the current process.
#[cfg(target_os = "macos")]
#[allow(deprecated)] // mach_task_self is deprecated in libc, but works fine
fn get_rss_kb() -> u64 {
    use std::mem;
    unsafe {
        let mut info: libc::mach_task_basic_info_data_t = mem::zeroed();
        let mut count = (mem::size_of::<libc::mach_task_basic_info_data_t>()
            / mem::size_of::<libc::natural_t>())
            as libc::mach_msg_type_number_t;
        let ret = libc::task_info(
            libc::mach_task_self(),
            libc::MACH_TASK_BASIC_INFO,
            &mut info as *mut _ as libc::task_info_t,
            &mut count,
        );
        if ret == libc::KERN_SUCCESS {
            info.resident_size as u64 / 1024
        } else {
            0
        }
    }
}

#[cfg(target_os = "linux")]
fn get_rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map(|pages| pages * 4) // page size is typically 4KB
        .unwrap_or(0)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn get_rss_kb() -> u64 {
    0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RedisConnectionManager;
    use bb8::Pool;

    async fn test_pool() -> crate::RedisPool {
        let manager = RedisConnectionManager::new("redis://127.0.0.1/").unwrap();
        Pool::builder().build(manager).await.unwrap()
    }

    async fn sismember(redis: &RedisPool, set: &str, member: &str) -> bool {
        let mut conn = redis.get().await.unwrap();
        redis::cmd("SISMEMBER")
            .arg(set)
            .arg(member)
            .query_async::<i64>(conn.unnamespaced_borrow_mut())
            .await
            .unwrap_or(0)
            == 1
    }

    async fn exists(redis: &RedisPool, key: &str) -> bool {
        let mut conn = redis.get().await.unwrap();
        redis::cmd("EXISTS")
            .arg(key)
            .query_async::<i64>(conn.unnamespaced_borrow_mut())
            .await
            .unwrap_or(0)
            > 0
    }

    fn new_publisher() -> StatsPublisher {
        StatsPublisher::new(
            "testhost".to_string(),
            vec!["default".to_string()],
            Counter::new(0),
            1,
        )
    }

    #[tokio::test]
    async fn deregister_removes_identity_from_processes_set() {
        let redis = test_pool().await;
        let p = new_publisher();

        p.publish_stats(redis.clone()).await.unwrap();
        assert!(
            sismember(&redis, "processes", p.identity()).await,
            "should be in set after publish_stats"
        );

        p.deregister(redis.clone()).await.unwrap();
        assert!(
            !sismember(&redis, "processes", p.identity()).await,
            "should be removed from set after deregister"
        );
    }

    #[tokio::test]
    async fn deregister_deletes_heartbeat_hash() {
        let redis = test_pool().await;
        let p = new_publisher();

        p.publish_stats(redis.clone()).await.unwrap();
        assert!(
            exists(&redis, p.identity()).await,
            "heartbeat hash should exist"
        );

        p.deregister(redis.clone()).await.unwrap();
        assert!(
            !exists(&redis, p.identity()).await,
            "heartbeat hash should be deleted after deregister"
        );
    }

    #[tokio::test]
    async fn deregister_does_not_affect_sibling_process() {
        let redis = test_pool().await;
        let p1 = new_publisher();
        let p2 = new_publisher(); // distinct nonce → distinct identity

        p1.publish_stats(redis.clone()).await.unwrap();
        p2.publish_stats(redis.clone()).await.unwrap();

        p1.deregister(redis.clone()).await.unwrap();

        assert!(
            sismember(&redis, "processes", p2.identity()).await,
            "sibling process must remain registered after p1 deregisters"
        );

        p2.deregister(redis.clone()).await.unwrap();
    }

    #[tokio::test]
    async fn deregister_is_idempotent() {
        let redis = test_pool().await;
        let p = new_publisher();

        p.publish_stats(redis.clone()).await.unwrap();
        p.deregister(redis.clone()).await.unwrap();
        // Second call must not error (SREM on missing member is a no-op in Redis)
        p.deregister(redis.clone()).await.unwrap();
    }
}
