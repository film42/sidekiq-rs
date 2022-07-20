use async_trait::async_trait;
use bb8::Pool;
use serde::{Deserialize, Serialize};
use sidekiq::{
    periodic, ChainIter, Job, Processor, RedisConnectionManager, RedisPool, ServerMiddleware,
    ServerResult, Worker, WorkerRef,
};
use slog::{debug, error, info, o, Drain};
use std::sync::Arc;

mod v2 {
    use super::*;

    pub struct StatisticsWorker {
        pub logger: slog::Logger,
    }

    #[async_trait]
    impl Worker<Stats> for StatisticsWorker {
        async fn perform(&self, args: Stats) -> Result<(), Box<dyn std::error::Error>> {
            info!(self.logger, "Got a metric (v2)"; "metric" => format!("{:?}", args));

            Ok(())
        }

        // Set the default queue
        fn opts() -> sidekiq::WorkerOpts<Stats, Self>
        where
            Self: Sized,
        {
            sidekiq::WorkerOpts::new().queue("ruby:v2_statistics")
        }

        // Set the default class name
        fn class_name() -> String
        where
            Self: Sized,
        {
            "V2::StatisticsWorker".to_string()
        }
    }
}

#[derive(Clone)]
struct V1StatisticsWorker {
    logger: slog::Logger,
}

#[derive(Debug, Serialize, Deserialize)]
struct Stats {
    metric: String,
    value: f64,
}

#[async_trait]
impl Worker<Stats> for V1StatisticsWorker {
    async fn perform(&self, args: Stats) -> Result<(), Box<dyn std::error::Error>> {
        info!(self.logger, "Got a metric (v1)"; "metric" => format!("{:?}", args));

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let decorator = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let logger = slog::Logger::root(drain, o!());

    let manager = RedisConnectionManager::new("redis://127.0.0.1/")?;
    let mut redis = Pool::builder()
        .connection_customizer(sidekiq::with_custom_namespace("yolo_app".to_string()))
        .build(manager)
        .await?;

    tokio::spawn({
        let mut redis = redis.clone();

        async move {
            loop {
                V1StatisticsWorker::opts()
                    .queue("ruby:v1_statistics")
                    .perform_async(
                        &mut redis,
                        Stats {
                            metric: "temp.house.basement".into(),
                            value: 12.2,
                        },
                    )
                    .await
                    .unwrap();

                v2::StatisticsWorker::opts()
                    .perform_async(
                        &mut redis,
                        Stats {
                            metric: "temp.house.garage".into(),
                            value: 13.37,
                        },
                    )
                    .await
                    .unwrap();

                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    });

    let mut p = Processor::new(
        redis.clone(),
        logger.clone(),
        vec![
            "rust:v1_statistics".to_string(),
            "rust:v2_statistics".to_string(),
        ],
    );

    p.register(V1StatisticsWorker {
        logger: logger.clone(),
    });

    p.register(v2::StatisticsWorker {
        logger: logger.clone(),
    });

    p.run().await;
    Ok(())
}
