use async_trait::async_trait;
use bb8_redis::{bb8::Pool, redis::AsyncCommands, RedisConnectionManager};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sidekiq::{Processor, Worker};
use slog::{error, info, o, Drain};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Logger
    let decorator = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let logger = slog::Logger::root(drain, o!());

    // Redis
    let manager = RedisConnectionManager::new("redis://127.0.0.1/").unwrap();
    let redis = Pool::builder().build(manager).await.unwrap();

    // Sidekiq
    let mut p = Processor::new(redis, logger.clone(), vec!["queue:yolo".to_string()]);

    // Add known workers
    p.register("HelloWorker", Box::new(HelloWorker::new(logger.clone())));
    p.register("YoloWorker", Box::new(YoloWorker::new(logger.clone())));

    p.run().await;
    Ok(())
}

#[derive(Clone)]
struct HelloWorker {
    logger: slog::Logger,
}

impl HelloWorker {
    fn new(logger: slog::Logger) -> Self {
        Self { logger }
    }
}

#[async_trait]
impl Worker for HelloWorker {
    async fn perform(&self, _args: JsonValue) -> Result<(), Box<dyn std::error::Error>> {
        println!("Hello, I'm worker!");

        Ok(())
    }
}

#[derive(Clone)]
struct YoloWorker {
    logger: slog::Logger,
}

impl YoloWorker {
    fn new(logger: slog::Logger) -> Self {
        Self { logger }
    }
}

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
