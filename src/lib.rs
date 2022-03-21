use async_trait::async_trait;
use bb8_redis::{bb8::Pool, redis::AsyncCommands, RedisConnectionManager};
use dyn_clone::DynClone;
use middleware::Chain;
use rand::{Rng, RngCore};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

mod middleware;
mod processor;
mod scheduled;

// Re-export
pub use middleware::{ChainIter, ServerMiddleware, ServerResult};
pub use processor::Processor;
pub use scheduled::Scheduled;

pub fn opts() -> EnqueueOpts {
    EnqueueOpts {
        queue: "default".into(),
    }
}

pub struct EnqueueOpts {
    queue: String,
}

impl EnqueueOpts {
    pub fn queue<S: Into<String>>(self, queue: S) -> Self {
        EnqueueOpts {
            queue: queue.into(),
        }
    }

    pub async fn perform_async(
        self,
        redis: &mut Pool<RedisConnectionManager>,
        class: String,
        args: impl serde::Serialize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(perform_async(redis, class, self.queue, args).await?)
    }
}

/// Helper function for enqueueing a worker into sidekiq.
/// This can be used to enqueue a job for a ruby sidekiq worker to process.
pub async fn perform_async(
    redis: &mut Pool<RedisConnectionManager>,
    class: String,
    queue: String,
    args: impl serde::Serialize,
) -> Result<(), Box<dyn std::error::Error>> {
    let args = serde_json::to_value(args)?;

    // Ensure args are always wrapped in an array.
    let args = if args.is_array() {
        args
    } else {
        JsonValue::Array(vec![args])
    };

    let job = Job {
        queue: queue,
        class: class,
        jid: new_jid(),
        created_at: chrono::Utc::now().timestamp() as f64,
        enqueued_at: chrono::Utc::now().timestamp() as f64,
        retry: true,
        args: args,

        // Make default eventually...
        error_message: None,
        failed_at: None,
        retry_count: None,
        retried_at: None,
    };

    UnitOfWork::from_job(job).enqueue(redis).await?;

    Ok(())
}

fn new_jid() -> String {
    let mut bytes = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

#[async_trait]
pub trait Worker: Send + Sync + DynClone {
    fn default_opts() -> EnqueueOpts
    where
        Self: Sized,
    {
        crate::opts()
    }

    /// Derive a class_name from the Worker type to be used with sidekiq. By default
    /// this method will
    fn class_name() -> String
    where
        Self: Sized,
    {
        use heck::ToUpperCamelCase;
        let type_name = std::any::type_name::<Self>();
        let name = type_name.split("::").last().unwrap_or(type_name);
        name.to_upper_camel_case()
    }

    async fn perform_async(
        redis: &mut Pool<RedisConnectionManager>,
        args: impl serde::Serialize + Send + 'static,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        Self: Sized,
    {
        let opts = Self::default_opts();
        crate::perform_async(redis, Self::class_name(), opts.queue, args).await
    }

    async fn perform(&self, args: JsonValue) -> Result<(), Box<dyn std::error::Error>>;
}
dyn_clone::clone_trait_object!(Worker);

//
// {
//   "retry": true,
//   "queue": "yolo",
//   "class": "YoloWorker",
//   "args": [
//     {
//       "yolo": "hiiii"
//     }
//   ],
//   "jid": "f33f7063c6d7a4db0869289a",
//   "created_at": 1647119929.3788748,
//   "enqueued_at": 1647119929.378998
// }
//
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Job {
    pub queue: String,
    pub args: JsonValue,
    pub retry: bool,
    pub class: String,
    pub jid: String,
    pub created_at: f64,
    pub enqueued_at: f64,
    pub failed_at: Option<f64>,
    pub error_message: Option<String>,
    pub retry_count: Option<usize>,
    pub retried_at: Option<f64>,
}

#[derive(Debug)]
pub struct UnitOfWork {
    queue: String,
    job: Job,
}

impl UnitOfWork {
    pub fn from_job(job: Job) -> Self {
        UnitOfWork {
            queue: format!("queue:{}", &job.queue),
            job,
        }
    }

    pub fn from_job_string(job_str: String) -> Result<Self, Box<dyn std::error::Error>> {
        let job: Job = serde_json::from_str(&job_str)?;
        Ok(Self::from_job(job))
    }

    pub async fn enqueue(
        &self,
        redis: &mut Pool<RedisConnectionManager>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut redis = redis.get().await?;
        self.enqueue_direct(&mut redis).await
    }

    async fn enqueue_direct(
        &self,
        redis: &mut redis::aio::Connection,
    ) -> Result<(), Box<dyn std::error::Error>> {
        redis
            .rpush(&self.queue, serde_json::to_string(&self.job)?)
            .await?;
        Ok(())
    }

    pub async fn reenqueue(
        &mut self,
        redis: &mut Pool<RedisConnectionManager>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(retry_count) = self.job.retry_count {
            redis
                .get()
                .await?
                .zadd(
                    "retry",
                    serde_json::to_string(&self.job)?,
                    Self::retry_job_at(retry_count).timestamp(),
                )
                .await?;
        }

        Ok(())
    }

    fn retry_job_at(count: usize) -> chrono::DateTime<chrono::Utc> {
        let seconds_to_delay =
            count.pow(4) + 15 + (rand::thread_rng().gen_range(0..30) * (count + 1));

        chrono::Utc::now() + chrono::Duration::seconds(seconds_to_delay as i64)
    }
}
