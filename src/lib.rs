use async_trait::async_trait;
use bb8_redis::{bb8::Pool, redis::AsyncCommands, RedisConnectionManager};
use dyn_clone::DynClone;
use rand::{Rng, RngCore};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use slog::{debug, error, info};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

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
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// A pseudo iterator used to know which middleware should be called next.
/// This is created by the Chain type.
#[derive(Clone)]
pub struct ChainIter {
    stack: Arc<RwLock<Vec<Box<dyn ServerMiddleware + Send + Sync>>>>,
    index: usize,
}

impl ChainIter {
    pub async fn next(
        &self,
        job: Job,
        worker: Box<dyn Worker>,
        redis: Pool<RedisConnectionManager>,
    ) -> ServerResult {
        let stack = self.stack.read().await;

        if let Some(ref middleware) = stack.get(self.index) {
            middleware
                .call(
                    ChainIter {
                        stack: self.stack.clone(),
                        index: self.index + 1,
                    },
                    job.clone(),
                    worker.clone(),
                    redis,
                )
                .await?;
        }

        Ok(())
    }
}

/// A chain of middlewares that will be called in order by different server middlewares.
#[derive(Clone)]
struct Chain {
    stack: Arc<RwLock<Vec<Box<dyn ServerMiddleware + Send + Sync>>>>,
}

impl Chain {
    fn new(logger: slog::Logger) -> Self {
        Self {
            stack: Arc::new(RwLock::new(vec![
                Box::new(RetryMiddleware::new(logger)),
                Box::new(HandlerMiddleware),
            ])),
        }
    }

    async fn using(&mut self, middleware: Box<dyn ServerMiddleware + Send + Sync>) {
        let mut stack = self.stack.write().await;
        // HACK: Insert after retry middleware but before the handler middleware.
        let index = stack.len() - 1;
        stack.insert(index, middleware);
    }

    fn iter(&self) -> ChainIter {
        ChainIter {
            stack: self.stack.clone(),
            index: 0,
        }
    }

    async fn call(
        &mut self,
        job: Job,
        worker: Box<dyn Worker>,
        redis: Pool<RedisConnectionManager>,
    ) -> ServerResult {
        // The middleware must call bottom of the stack to the top.
        // Each middleware should receive a lambda to the next middleware
        // up the stack. Each middleware can short-circuit the stack by
        // not calling the "next" middleware.
        self.iter().next(job, worker, redis).await
    }
}

pub type ServerResult = Result<(), Box<dyn std::error::Error>>;

#[async_trait]
pub trait ServerMiddleware {
    async fn call(
        &self,
        iter: ChainIter,
        job: Job,
        worker: Box<dyn Worker>,
        redis: Pool<RedisConnectionManager>,
    ) -> ServerResult;
}

struct HandlerMiddleware;

#[async_trait]
impl ServerMiddleware for HandlerMiddleware {
    async fn call(
        &self,
        _chain: ChainIter,
        job: Job,
        worker: Box<dyn Worker>,
        _redis: Pool<RedisConnectionManager>,
    ) -> ServerResult {
        worker.perform(job.args).await
    }
}

struct RetryMiddleware {
    logger: slog::Logger,
}

impl RetryMiddleware {
    fn new(logger: slog::Logger) -> Self {
        Self { logger }
    }

    fn max_retries(&self) -> usize {
        // TODO: Make configurable at the worker level
        25
    }
}

#[async_trait]
impl ServerMiddleware for RetryMiddleware {
    async fn call(
        &self,
        chain: ChainIter,
        mut job: Job,
        worker: Box<dyn Worker>,
        mut redis: Pool<RedisConnectionManager>,
    ) -> ServerResult {
        let err = {
            match chain.next(job.clone(), worker, redis.clone()).await {
                Ok(()) => return Ok(()),
                Err(err) => format!("{err:?}"),
            }
        };

        // Update error fields on the job.
        job.error_message = Some(err);
        if job.retry_count.is_some() {
            job.retried_at = Some(chrono::Utc::now().timestamp() as f64);
        } else {
            job.failed_at = Some(chrono::Utc::now().timestamp() as f64);
        }
        let retry_count = job.retry_count.unwrap_or(0) + 1;
        job.retry_count = Some(retry_count);

        // Attempt the retry.
        if retry_count < self.max_retries() {
            error!(self.logger,
                "Scheduling job for retry in the future";
                "status" => "fail",
                "class" => &job.class,
                "jid" => &job.jid,
                "queue" => &job.queue,
                "err" => &job.error_message,
            );

            UnitOfWork::from_job(job.clone())
                .reenqueue(&mut redis)
                .await?;
        }

        Ok(())
    }
}

#[async_trait]
pub trait Worker: Send + Sync + DynClone {
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

#[derive(Clone)]
pub struct Processor {
    redis: Pool<RedisConnectionManager>,
    queues: Vec<String>,
    workers: BTreeMap<String, Box<dyn Worker>>,
    logger: slog::Logger,
    chain: Chain,
}

impl Processor {
    pub fn new(
        redis: Pool<RedisConnectionManager>,
        logger: slog::Logger,
        queues: Vec<String>,
    ) -> Self {
        Self {
            chain: Chain::new(logger.clone()),
            workers: BTreeMap::new(),

            redis,
            logger,
            queues: queues,
        }
    }

    async fn fetch(&mut self) -> Result<UnitOfWork, Box<dyn std::error::Error>> {
        let (queue, job_raw): (String, String) =
            self.redis.get().await?.brpop(&self.queues, 0).await?;
        let job: Job = serde_json::from_str(&job_raw)?;
        // println!("{:?}", (&queue, &args));
        Ok(UnitOfWork { queue, job })
    }

    pub async fn process_one(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut work = self.fetch().await?;

        info!(self.logger, "sidekiq";
            "status" => "start",
            "class" => &work.job.class,
            "jid" => &work.job.jid
        );

        if let Some(worker) = self.workers.get_mut(&work.job.class) {
            self.chain
                .call(work.job.clone(), worker.clone(), self.redis.clone())
                .await?;
        } else {
            error!(
                self.logger,
                "!!! Worker not found !!!";
                "staus" => "fail",
                "class" => &work.job.class,
                "jid" => &work.job.jid,
            );
            work.reenqueue(&mut self.redis).await?;
        }

        // TODO: Make this only say "done" when the job is successful.
        // We might need to change the ChainIter to return the final job and
        // detect any retries?
        info!(self.logger, "sidekiq";
            "status" => "done",
            "class" => &work.job.class,
            "jid" => &work.job.jid,
        );

        Ok(())
    }

    pub fn register<S: Into<String>>(&mut self, name: S, worker: Box<dyn Worker>) {
        self.workers.insert(name.into(), worker);
    }

    /// Takes self to consume the processor. This is for life-cycle management, not
    /// memory safety because you can clone processor pretty easily.
    pub async fn run(self) {
        let cpu_count = num_cpus::get();

        // Start worker routines.
        for _ in 0..cpu_count {
            tokio::spawn({
                let mut processor = self.clone();
                let logger = self.logger.clone();

                async move {
                    loop {
                        if let Err(err) = processor.process_one().await {
                            error!(logger, "Error leaked out the bottom: {:?}", err);
                        }
                    }
                }
            });
        }

        // Start retry and scheduled routines.
        tokio::spawn({
            let logger = self.logger.clone();
            let redis = self.redis.clone();
            async move {
                let sched = Scheduled::new(redis, logger.clone());
                let sorted_sets = vec!["retry".to_string(), "schedule".to_string()];

                loop {
                    // TODO: Use process count to meet a 5 second avg.
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

                    if let Err(err) = sched.enqueue_jobs(chrono::Utc::now(), &sorted_sets).await {
                        error!(logger, "Error in scheduled poller routine: {:?}", err);
                    }
                }
            }
        });

        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await
        }
    }

    pub async fn using(&mut self, middleware: Box<dyn ServerMiddleware + Send + Sync>) {
        self.chain.using(middleware).await
    }
}

pub struct Scheduled {
    redis: Pool<RedisConnectionManager>,
    logger: slog::Logger,
}

impl Scheduled {
    fn new(redis: Pool<RedisConnectionManager>, logger: slog::Logger) -> Self {
        Self { redis, logger }
    }

    async fn enqueue_jobs(
        &self,
        now: chrono::DateTime<chrono::Utc>,
        sorted_sets: &Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for sorted_set in sorted_sets {
            let mut redis = self.redis.get().await?;

            let jobs: Vec<String> = redis
                .zrangebyscore_limit(&sorted_set, "-inf", now.timestamp(), 0, 100)
                .await?;

            for job in jobs {
                if redis.zrem(&sorted_set, job.clone()).await? {
                    let work = UnitOfWork::from_job_string(job)?;

                    debug!(self.logger, "Enqueueing job";
                        "class" => &work.job.class,
                        "queue" => &work.queue
                    );

                    work.enqueue_direct(&mut redis).await?;
                }
            }
        }

        Ok(())
    }
}
