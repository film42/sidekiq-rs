use async_trait::async_trait;
use bb8_redis::{bb8::Pool, redis::AsyncCommands, RedisConnectionManager};
use middleware::Chain;
use rand::{Rng, RngCore};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

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

pub struct WorkerOpts<Args, W: WorkerGeneric<Args> + ?Sized> {
    queue: String,
    retry: bool,
    args: PhantomData<Args>,
    worker: PhantomData<W>,
}

impl<Args, W> WorkerOpts<Args, W>
where
    W: WorkerGeneric<Args>,
{
    pub fn new() -> Self {
        Self {
            queue: "default".into(),
            retry: true,
            args: PhantomData,
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

impl<Args, W: WorkerGeneric<Args>> From<&WorkerOpts<Args, W>> for EnqueueOpts {
    fn from(opts: &WorkerOpts<Args, W>) -> Self {
        Self {
            retry: opts.retry,
            queue: opts.queue.clone(),
        }
    }
}

// CONSTRUCTION

#[async_trait]
pub trait WorkerGeneric<Args>: Send + Sync {
    fn opts() -> WorkerOpts<Args, Self>
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
        Args: Send + Sync,
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
        Args: Send + Sync,
    {
        Self::opts().perform_in(redis, duration, args).await
    }

    async fn perform(&self, args: Args) -> Result<(), Box<dyn std::error::Error>>;
}
// dyn_clone::clone_trait_object!(WorkerGeneric);

// CONSTRUCTION

// We can't store a Vec<Box<dyn Worker<Args>>>, because that will only work
// for a single arg type, but since any worker is JsonValue in and Result out,
// we can wrap that generic work in a callback that shares the same type.
// I'm sure this has a fancy name, but I don't know what it is.
#[derive(Clone)]
pub struct WorkerCaller {
    work_fn: Arc<
        Box<
            dyn Fn(
                    JsonValue,
                )
                    -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send>>
                + Send
                + Sync,
        >,
    >,
    max_retries: usize,
}

async fn call_worker<Args, W>(args: JsonValue, worker: Arc<W>) -> ServerResult
where
    Args: Send + Sync + 'static,
    W: WorkerGeneric<Args> + 'static,
    for<'de> Args: Deserialize<'de>,
{
    // If the value contains a single item Vec then
    // you can probably be sure that this is a single value item.
    // Otherwise, the caller can impl a tuple type.
    let args = match args {
        JsonValue::Array(mut arr) if arr.len() == 1 => arr.pop().unwrap(),
        _ => args,
    };

    let args: Args = serde_json::from_value(args)?;
    Ok(worker.perform(args).await?)
}

impl WorkerCaller {
    pub(crate) fn wrap<Args, W>(worker: Arc<W>) -> Self
    where
        Args: Send + Sync + 'static,
        W: WorkerGeneric<Args> + 'static,
        for<'de> Args: Deserialize<'de>,
    {
        Self {
            work_fn: Arc::new(Box::new({
                let worker = worker.clone();
                move |args: JsonValue| {
                    let worker = worker.clone();
                    Box::pin(async move { Ok(call_worker(args, worker).await?) })
                    //let args: Args = serde_json::from_value(args).unwrap();

                    //Box::pin(async move { Ok(worker.perform(args).await?) })
                }
            })),
            max_retries: worker.max_retries(),
        }
    }

    pub fn max_retries(&self) -> usize {
        self.max_retries
    }

    pub async fn call(&self, args: JsonValue) -> Result<(), Box<dyn std::error::Error>> {
        (Arc::clone(&self.work_fn))(args).await
    }
}

#[derive(Deserialize, Serialize)]
struct TestArg {
    name: String,
    age: i32,
}

struct TestGenericWorker;

#[async_trait]
impl WorkerGeneric<TestArg> for TestGenericWorker {
    async fn perform(&self, args: TestArg) -> ServerResult {
        Ok(())
    }
}

#[tokio::test]
async fn testing_this_crap() {
    let worker = Arc::new(TestGenericWorker);
    let wrap = Arc::new(WorkerCaller::wrap(worker));

    for _ in 0..1 {
        let wrap = wrap.clone();
        let arg = serde_json::to_value(TestArg {
            name: "test".into(),
            age: 1337,
        })
        .unwrap();

        let x = wrap.call(arg).await.unwrap();
        println!("Res: {:?}", x);
    }
}

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
