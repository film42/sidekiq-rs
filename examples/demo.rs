use async_trait::async_trait;
use bb8_redis::{bb8::Pool, RedisConnectionManager};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sidekiq::{
    periodic, ChainIter, Job, Processor, ServerMiddleware, ServerResult, Worker, WorkerRef,
};
use slog::{error, info, o, Drain};
use std::sync::Arc;

#[derive(Clone)]
struct HelloWorker;

#[async_trait]
impl Worker<()> for HelloWorker {
    async fn perform(&self, _args: ()) -> Result<(), Box<dyn std::error::Error>> {
        // I don't use any args. I do my own work.
        Ok(())
    }
}

#[derive(Clone)]
struct PaymentReportWorker {
    logger: slog::Logger,
}

impl PaymentReportWorker {
    fn new(logger: slog::Logger) -> Self {
        Self { logger }
    }

    async fn send_report(&self, user_guid: String) -> Result<(), Box<dyn std::error::Error>> {
        // TODO: Some actual work goes here...
        info!(self.logger, "Sending payment report to user"; "user_guid" => user_guid, "class_name" => Self::class_name());

        Ok(())
    }
}

#[derive(Deserialize, Debug, Serialize)]
struct PaymentReportArgs {
    user_guid: String,
}

#[async_trait]
impl Worker<PaymentReportArgs> for PaymentReportWorker {
    fn opts() -> sidekiq::WorkerOpts<PaymentReportArgs, Self> {
        sidekiq::WorkerOpts::new().queue("yolo")
    }

    async fn perform(&self, args: PaymentReportArgs) -> Result<(), Box<dyn std::error::Error>> {
        self.send_report(args.user_guid).await
    }
}

struct FilterExpiredUsersMiddleware {
    logger: slog::Logger,
}

impl FilterExpiredUsersMiddleware {
    fn new(logger: slog::Logger) -> Self {
        Self { logger }
    }
}

#[derive(Deserialize)]
struct FiltereExpiredUsersArgs {
    user_guid: String,
}

impl FiltereExpiredUsersArgs {
    fn is_expired(&self) -> bool {
        self.user_guid == "USR-123-EXPIRED"
    }
}

#[async_trait]
impl ServerMiddleware for FilterExpiredUsersMiddleware {
    async fn call(
        &self,
        chain: ChainIter,
        job: &Job,
        worker: Arc<WorkerRef>,
        redis: Pool<RedisConnectionManager>,
    ) -> ServerResult {
        let args: Result<(FiltereExpiredUsersArgs,), serde_json::Error> =
            serde_json::from_value(job.args.clone());

        // If we can safely deserialize then attempt to filter based on user guid.
        if let Ok((filter,)) = args {
            if filter.is_expired() {
                error!(
                    self.logger,
                    "Detected an expired user, skipping this job";
                    "class" => &job.class,
                    "jid" => &job.jid,
                    "user_guid" => filter.user_guid,
                );
                return Ok(());
            }
        }

        chain.next(job, worker, redis).await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Logger
    let decorator = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let logger = slog::Logger::root(drain, o!());

    // Redis
    let manager = RedisConnectionManager::new("redis://127.0.0.1/")?;
    let mut redis = Pool::builder().build(manager).await?;

    tokio::spawn({
        let mut redis = redis.clone();

        async move {
            loop {
                PaymentReportWorker::perform_async(
                    &mut redis,
                    PaymentReportArgs {
                        user_guid: "USR-123".into(),
                    },
                )
                .await
                .unwrap();

                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    });

    // Enqueue a job with the worker! There are many ways to do this.
    PaymentReportWorker::perform_async(
        &mut redis,
        PaymentReportArgs {
            user_guid: "USR-123".into(),
        },
    )
    .await?;

    PaymentReportWorker::perform_in(
        &mut redis,
        std::time::Duration::from_secs(10),
        PaymentReportArgs {
            user_guid: "USR-123".into(),
        },
    )
    .await?;

    PaymentReportWorker::opts()
        .queue("brolo")
        .perform_async(
            &mut redis,
            PaymentReportArgs {
                user_guid: "USR-123-EXPIRED".into(),
            },
        )
        .await?;

    sidekiq::perform_async(
        &mut redis,
        "PaymentReportWorker".into(),
        "yolo".into(),
        PaymentReportArgs {
            user_guid: "USR-123".to_string(),
        },
    )
    .await?;

    // Enqueue a job
    sidekiq::perform_async(
        &mut redis,
        "PaymentReportWorker".into(),
        "yolo".into(),
        PaymentReportArgs {
            user_guid: "USR-123".to_string(),
        },
    )
    .await?;

    // Enqueue a job with options
    sidekiq::opts()
        .queue("yolo".to_string())
        .perform_async(
            &mut redis,
            "PaymentReportWorker".into(),
            PaymentReportArgs {
                user_guid: "USR-123".to_string(),
            },
        )
        .await?;

    // Sidekiq server
    let mut p = Processor::new(
        redis.clone(),
        logger.clone(),
        vec!["yolo".to_string(), "brolo".to_string()],
    );

    // Add known workers
    p.register(HelloWorker);
    p.register(PaymentReportWorker::new(logger.clone()));

    // Custom Middlewares
    p.using(FilterExpiredUsersMiddleware::new(logger.clone()))
        .await;

    // Reset cron jobs
    periodic::destroy_all(redis.clone()).await?;

    // Cron jobs
    periodic::builder("0 * * * * *")?
        .name("Payment report processing for a user using json args")
        .queue("yolo")
        .args(json!({ "user_guid": "USR-123-PERIODIC-FROM-JSON-ARGS" }))?
        .register(&mut p, PaymentReportWorker::new(logger.clone()))
        .await?;

    periodic::builder("0 * * * * *")?
        .name("Payment report processing for a user using typed args")
        .queue("yolo")
        .args(PaymentReportArgs {
            user_guid: "USR-123-PERIODIC-FROM-TYPED-ARGS".to_string(),
        })?
        .register(&mut p, PaymentReportWorker::new(logger.clone()))
        .await?;

    p.run().await;
    Ok(())
}
