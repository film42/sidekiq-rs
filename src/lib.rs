use async_trait::async_trait;
use bb8_redis::{bb8::Pool, redis::AsyncCommands, RedisConnectionManager};
use dyn_clone::DynClone;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use slog::{error, info};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// A pseudo iterator used to know which middleware should be called next.
/// This is created by the Chain type.
#[derive(Clone)]
struct ChainIter {
    stack: Arc<RwLock<Vec<Box<dyn ServerMiddleware + Send + Sync>>>>,
    index: usize,
}

impl ChainIter {
    async fn next(
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
    fn new() -> Self {
        Self {
            stack: Arc::new(RwLock::new(vec![
                Box::new(RetryMiddleware),
                Box::new(HandlerMiddleware),
            ])),
        }
    }

    async fn using(&mut self, middleware: Box<dyn ServerMiddleware + Send + Sync>) {
        let mut stack = self.stack.write().await;
        stack.push(middleware);
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

type ServerResult = Result<(), Box<dyn std::error::Error>>;

#[async_trait]
trait ServerMiddleware {
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
        println!("BEFORE Calling worker...");
        let r = worker.perform(job.args).await;
        println!("AFTER Calling worker...");
        r
    }
}

struct RetryMiddleware;

#[async_trait]
impl ServerMiddleware for RetryMiddleware {
    async fn call(
        &self,
        chain: ChainIter,
        job: Job,
        worker: Box<dyn Worker>,
        mut redis: Pool<RedisConnectionManager>,
    ) -> ServerResult {
        println!("BEFORE: retry middleware");
        if chain
            .next(job.clone(), worker, redis.clone())
            .await
            .is_err()
        {
            UnitOfWork {
                job: job.clone(),
                queue: format!("queue:{}", &job.queue),
            }
            .reenqueue(&mut redis)
            .await?;
        }
        println!("AFTER: retry middleware");
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
struct Job {
    queue: String,
    args: JsonValue,
    retry: bool,
    class: String,
    jid: String,
    created_at: f64,
    enqueued_at: f64,
}

#[derive(Debug)]
struct UnitOfWork {
    queue: String,
    job: Job,
}

impl UnitOfWork {
    pub async fn reenqueue(
        &self,
        redis: &mut Pool<RedisConnectionManager>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        redis
            .get()
            .await?
            .rpush(&self.queue, serde_json::to_string(&self.job)?)
            .await?;

        Ok(())
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
            redis,
            logger,
            queues: queues,
            workers: BTreeMap::new(),
            chain: Chain::new(),
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
        let work = self.fetch().await?;

        info!(self.logger, "sidekiq";
            "status" => "start",
            "class" => &work.job.class,
            "jid" => &work.job.jid
        );

        if let Some(worker) = self.workers.get_mut(&work.job.class) {
            // not reached, testing...

            self.chain
                .call(
                    work.job.clone(),
                    worker.clone(),
                    // async move { handler.call(work.job.clone(), worker.clone()).await },
                    // &mut self.redis,
                    self.redis.clone(),
                )
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

        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await
        }
    }
}
