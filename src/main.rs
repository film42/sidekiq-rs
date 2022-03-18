use async_trait::async_trait;
use dyn_clone::DynClone;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use slog::{error, info, o, Drain};
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
        redis: Option<redis::aio::Connection>,
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
struct Chain {
    stack: Arc<RwLock<Vec<Box<dyn ServerMiddleware + Send + Sync>>>>,
}

impl Chain {
    fn new() -> Self {
        Self {
            stack: Arc::new(RwLock::new(vec![])),
        }
    }

    async fn using(&mut self, middleware: Box<dyn ServerMiddleware + Send + Sync>) {
        let mut stack = self.stack.write().await;
        stack.push(middleware);
    }

    async fn call(
        &mut self,
        job: Job,
        worker: Box<dyn Worker>,
        redis: Option<redis::aio::Connection>,
    ) -> ServerResult {
        // The middleware must call bottom of the stack to the top.
        // Each middleware should receive a lambda to the next middleware
        // up the stack. Each middleware can short-circuit the stack by
        // not calling the "next" middleware.

        ChainIter {
            stack: self.stack.clone(),
            index: 0,
        }
        .next(job, worker, redis)
        .await
    }
}

type ServerResult = Result<(), Box<dyn std::error::Error>>;

#[async_trait]
trait ServerMiddleware {
    async fn call(
        &self,
        _iter: ChainIter,
        _job: Job,
        _worker: Box<dyn Worker>,
        redis: Option<redis::aio::Connection>,
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
        _redis: Option<redis::aio::Connection>,
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
        redis: Option<redis::aio::Connection>,
    ) -> ServerResult {
        println!("BEFORE: retry middleware");
        if chain.next(job.clone(), worker, None).await.is_err() {
            if let Some(mut redis) = redis {
                UnitOfWork {
                    job: job.clone(),
                    queue: format!("queue:{}", &job.queue),
                }
                .reenqueue(&mut redis)
                .await?;
            }
        }
        println!("AFTER: retry middleware");
        Ok(())
    }
}

#[async_trait]
trait Worker: Send + Sync + DynClone {
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
        client: &mut redis::aio::Connection,
    ) -> Result<(), Box<dyn std::error::Error>> {
        client
            .rpush(&self.queue, serde_json::to_string(&self.job)?)
            .await?;

        Ok(())
    }
}

struct Processor {
    redis: redis::aio::Connection,
    queues: Vec<String>,
    workers: BTreeMap<String, Box<dyn Worker>>,
    logger: slog::Logger,
    chain: Chain,
}

impl Processor {
    pub async fn fetch(&mut self) -> Result<UnitOfWork, Box<dyn std::error::Error>> {
        let (queue, job_raw): (String, String) = self.redis.brpop(&self.queues, 0).await?;
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
                    None,
                )
                .await?;

        //             // let mut args: Vec<JsonValue> = serde_json::from_value(work.job.args)?;
        //             // How do we control this variadic argument problem? If it's an array with one object we
        //             // can do this, but if not we'll likely need the caller to specify a tuple type.
        //             // let args = args.pop().unwrap();
        //             if let Err(err) = worker.perform(work.job.args.clone()).await {
        //                 error!(self.logger, "sidekiq";
        //                     "staus" => "fail",
        //                     "class" => &work.job.class,
        //                     "jid" => &work.job.jid,
        //                     "reason" => format!("{err:?}")
        //                 );
        //                 work.reenqueue(&mut self.redis).await?;
        //                 return Err(err);
        //             }
        } else {
            // Handle missing worker.
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let decorator = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let logger = slog::Logger::root(drain, o!());

    let client = redis::Client::open("redis://127.0.0.1/")?;
    let con = client.get_tokio_connection().await?;
    // let res = con.brpop("some:queue", 1).await?;
    // println!("Hello, world! {:?}", res);

    let mut p = Processor {
        redis: con,
        queues: vec!["queue:yolo".to_string()],
        workers: BTreeMap::new(),
        logger,
        chain: Chain::new(),
    };

    p.chain.using(Box::new(RetryMiddleware)).await;
    p.chain.using(Box::new(HandlerMiddleware)).await;

    // Add known workers
    p.register("HelloWorker", Box::new(HelloWorker));
    p.register("YoloWorker", Box::new(YoloWorker));

    loop {
        p.process_one().await?;
    }
}

#[derive(Clone)]
struct HelloWorker;

#[async_trait]
impl Worker for HelloWorker {
    async fn perform(&self, _args: JsonValue) -> Result<(), Box<dyn std::error::Error>> {
        println!("Hello, I'm worker!");

        Ok(())
    }
}

#[derive(Clone)]
struct YoloWorker;

#[derive(Deserialize, Debug)]
struct YoloArgs {
    yolo: String,
}

#[async_trait]
impl Worker for YoloWorker {
    async fn perform(&self, args: JsonValue) -> Result<(), Box<dyn std::error::Error>> {
        let (args,): (YoloArgs,) = serde_json::from_value(args)?;

        println!("YOLO: {}", &args.yolo);

        Ok(())
    }
}
