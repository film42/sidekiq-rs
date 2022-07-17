use rand::RngCore;

struct ProcessStats {
    rtt_us: String,
    quiet: bool,
    busy: usize,
    beat: chrono::DateTime<chrono::Utc>,
    info: ProcessInfo,
    rss: String,
}

struct ProcessInfo {
    hostname: String,
    identity: String,
    started_at: chrono::DateTime<chrono::Utc>,
    pid: u32,
    tag: Option<String>,
    concurrency: usize,
    queues: Vec<String>,
    labels: Vec<String>,
}

pub struct StatPoller {
    hostname: String,
    identity: String,
    queues: Vec<String>,
    started_at: chrono::DateTime<chrono::Utc>,
}

fn generate_identity(hostname: &String) -> String {
    let pid = std::process::id();
    let mut bytes = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut bytes);
    let nonce = hex::encode(bytes).to_string();

    format!("{hostname}:{pid}:{nonce}")
}

impl StatPublisher {
    fn new(hostname: String, queues: Vec<String>) -> Self {
        let identity = generate_identity(&hostname);
        let started_at = chrono::Utc::now();

        Self {
            hostname,
            identity,
            queues,
            started_at,
        }
    }

    pub async fn publish_stats(&self, redis: RedisPool) -> Result<(), Box<dyn std::error::Error>> {
        let stats = self.create_process_stats();
        let mut conn = redis.get().await?;
        conn.hset(self.identity.clone(),
    }

    fn create_process_stats(&self) -> ProcessStats {
        ProcessStats {
            // TODO: Individual metrics.
            rtt_us: "0".into(),
            rss: "0".into(),
            busy: 0,
            quiet: false,

            beat: chrono::Utc::now(),
            info: ProcessInfo {
                concurrency: num_cpus::get(),
                hostname: self.hostname.clone(),
                identity: self.identity.clone(),
                queues: self.queues.clone(),
                started_at: self.started_at.clone(),
                pid: std::process::id(),

                // TODO: Fill out labels and tags.
                labels: vec![],
                tag: None,
            },
        }
    }
}
