use crate::RedisPool;
use rand::RngCore;
use serde::Serialize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

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
    quiet: bool,
    busy: usize,
    beat: chrono::DateTime<chrono::Utc>,
    info: ProcessInfo,
    rss: String,
}

#[derive(Serialize)]
struct ProcessInfo {
    hostname: String,
    identity: String,
    started_at: f64,
    pid: u32,
    tag: Option<String>,
    concurrency: usize,
    queues: Vec<String>,
    labels: Vec<String>,
}

pub struct StatsPublisher {
    hostname: String,
    identity: String,
    queues: Vec<String>,
    started_at: chrono::DateTime<chrono::Utc>,
    busy_jobs: Counter,
}

fn generate_identity(hostname: &String) -> String {
    let pid = std::process::id();
    let mut bytes = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut bytes);
    let nonce = hex::encode(bytes);

    format!("{hostname}:{pid}:{nonce}")
}

impl StatsPublisher {
    #[must_use]
    pub fn new(hostname: String, queues: Vec<String>, busy_jobs: Counter) -> Self {
        let identity = generate_identity(&hostname);
        let started_at = chrono::Utc::now();

        Self {
            hostname,
            identity,
            queues,
            started_at,
            busy_jobs,
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
        let _ : () = conn.cmd_with_key("HSET", self.identity.clone())
            .arg("rss")
            .arg(stats.rss)
            .arg("rtt_us")
            .arg(stats.rtt_us)
            .arg("busy")
            .arg(stats.busy)
            .arg("quiet")
            .arg(stats.quiet)
            .arg("beat")
            .arg(stats.beat.timestamp())
            .arg("info")
            .arg(serde_json::to_string(&stats.info)?)
            .query_async(conn.unnamespaced_borrow_mut())
            .await?;

        conn.expire(self.identity.clone(), 30).await?;

        conn.sadd("processes".to_string(), self.identity.clone())
            .await?;

        Ok(())
    }

    async fn create_process_stats(&self) -> Result<ProcessStats, Box<dyn std::error::Error>> {
        #[cfg(feature = "rss-stats")]
        let rss_in_kb = format!(
            "{}",
            simple_process_stats::ProcessStats::get()
                .await?
                .memory_usage_bytes
                / 1024
        );

        #[cfg(not(feature = "rss-stats"))]
        let rss_in_kb = "0".to_string();

        Ok(ProcessStats {
            rtt_us: "0".into(),
            busy: self.busy_jobs.value(),
            quiet: false,
            rss: rss_in_kb,

            beat: chrono::Utc::now(),
            info: ProcessInfo {
                concurrency: num_cpus::get(),
                hostname: self.hostname.clone(),
                identity: self.identity.clone(),
                queues: self.queues.clone(),
                started_at: self.started_at.clone().timestamp() as f64,
                pid: std::process::id(),

                // TODO: Fill out labels and tags.
                labels: vec![],
                tag: None,
            },
        })
    }
}
