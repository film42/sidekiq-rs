use async_trait::async_trait;
use bb8_redis::{bb8::Pool, redis::AsyncCommands, RedisConnectionManager};
use dyn_clone::DynClone;
use middleware::Chain;
use rand::{Rng, RngCore};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::marker::PhantomData;

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
        retry: true,
    }
}

pub struct EnqueueOpts {
    queue: String,
    retry: bool,
}

impl EnqueueOpts {
    pub fn queue<S: Into<String>>(self, queue: S) -> Self {
        Self {
            queue: queue.into(),
            ..self
        }
    }

    pub fn retry(self, retry: bool) -> Self {
        Self { retry, ..self }
    }

    fn create_job(
        &self,
        class: String,
        args: impl serde::Serialize,
    ) -> Result<Job, Box<dyn std::error::Error>> {
        let args = serde_json::to_value(args)?;

        // Ensure args are always wrapped in an array.
        let args = if args.is_array() {
            args
        } else {
            JsonValue::Array(vec![args])
        };

        Ok(Job {
            queue: self.queue.clone(),
            class: class,
            jid: new_jid(),
            created_at: chrono::Utc::now().timestamp() as f64,
            enqueued_at: None,
            retry: self.retry,
            args: args,

            // Make default eventually...
            error_message: None,
            failed_at: None,
            retry_count: None,
            retried_at: None,
        })
    }

    pub async fn perform_async(
        self,
        redis: &mut Pool<RedisConnectionManager>,
        class: String,
        args: impl serde::Serialize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let job = self.create_job(class, args)?;
        UnitOfWork::from_job(job).enqueue(redis).await?;
        Ok(())
    }

    pub async fn perform_in(
        &self,
        redis: &mut Pool<RedisConnectionManager>,
        class: String,
        duration: std::time::Duration,
        args: impl serde::Serialize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let job = self.create_job(class, args)?;
        UnitOfWork::from_job(job).schedule(redis, duration).await?;
        Ok(())
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
    opts().queue(queue).perform_async(redis, class, args).await
}

/// Helper function for enqueueing a worker into sidekiq.
/// This can be used to enqueue a job for a ruby sidekiq worker to process.
pub async fn perform_in(
    redis: &mut Pool<RedisConnectionManager>,
    duration: std::time::Duration,
    class: String,
    queue: String,
    args: impl serde::Serialize,
) -> Result<(), Box<dyn std::error::Error>> {
    opts()
        .queue(queue)
        .perform_in(redis, class, duration, args)
        .await
}

fn new_jid() -> String {
    let mut bytes = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

pub struct WorkerOpts<W: ?Sized>
where
    W: Worker,
{
    queue: String,
    retry: bool,
    worker: PhantomData<W>,
}

impl<W> WorkerOpts<W>
where
    W: Worker,
{
    pub fn new() -> Self {
        Self {
            queue: "default".into(),
            retry: true,
            worker: PhantomData,
        }
    }

    pub fn retry(self, retry: bool) -> Self {
        Self { retry, ..self }
    }

    pub fn queue<S: Into<String>>(self, queue: S) -> Self {
        Self {
            queue: queue.into(),
            ..self
        }
    }

    fn into_opts(&self) -> EnqueueOpts {
        self.into()
    }

    pub async fn perform_async(
        &self,
        redis: &mut Pool<RedisConnectionManager>,
        args: impl serde::Serialize + Send + 'static,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.into_opts()
            .perform_async(redis, W::class_name(), args)
            .await
    }

    pub async fn perform_in(
        &self,
        redis: &mut Pool<RedisConnectionManager>,
        duration: std::time::Duration,
        args: impl serde::Serialize + Send + 'static,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.into_opts()
            .perform_in(redis, W::class_name(), duration, args)
            .await
    }
}

impl<W> From<&WorkerOpts<W>> for EnqueueOpts
where
    W: Worker,
{
    fn from(opts: &WorkerOpts<W>) -> Self {
        Self {
            retry: opts.retry,
            queue: opts.queue.clone(),
        }
    }
}

#[async_trait]
pub trait Worker: Send + Sync + DynClone {
    fn opts() -> WorkerOpts<Self>
    where
        Self: Sized,
    {
        WorkerOpts::new()
    }

    // TODO: Make configurable through opts and make opts accessible to the
    // retry middleware through a Box<dyn Worker>.
    fn max_retries(&self) -> usize {
        25
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
        Self::opts().perform_async(redis, args).await
    }

    async fn perform_in(
        redis: &mut Pool<RedisConnectionManager>,
        duration: std::time::Duration,
        args: impl serde::Serialize + Send + 'static,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        Self: Sized,
    {
        Self::opts().perform_in(redis, duration, args).await
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
    pub enqueued_at: Option<f64>,
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
        let mut job = self.job.clone();
        job.enqueued_at = Some(chrono::Utc::now().timestamp() as f64);

        redis
            .rpush(&self.queue, serde_json::to_string(&job)?)
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

    pub async fn schedule(
        &mut self,
        redis: &mut Pool<RedisConnectionManager>,
        duration: std::time::Duration,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let enqueue_at = chrono::Utc::now() + chrono::Duration::from_std(duration)?;

        Ok(redis
            .get()
            .await?
            .zadd(
                "schedule",
                serde_json::to_string(&self.job)?,
                enqueue_at.timestamp(),
            )
            .await?)
    }
}
