use async_trait::async_trait;
use bb8::Pool;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sidekiq::{
    periodic, ChainIter, Job, Processor, RedisConnectionManager, RedisPool, Result,
    ServerMiddleware, Worker, WorkerRef,
};
use std::sync::Arc;
use tracing::{debug, error, info};

#[derive(Clone)]
struct HelloWorker;

#[async_trait]
impl Worker<()> for HelloWorker {
    async fn perform(&self, _args: ()) -> Result<()> {
        // I don't use any args. I do my own work.
        Ok(())
    }
}

#[derive(Clone)]
struct PaymentReportWorker {
    redis: RedisPool,
}

impl PaymentReportWorker {
    fn new(redis: RedisPool) -> Self {
        Self { redis }
    }

    async fn send_report(&self, user_guid: String) -> Result<()> {
        // TODO: Some actual work goes here...
        info!({
            "user_guid" = user_guid,
            "class_name" = Self::class_name()
        }, "Sending payment report to user");

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

    async fn perform(&self, args: PaymentReportArgs) -> Result<()> {
        use redis::AsyncCommands;

        let times_called: usize = self
            .redis
            .get()
            .await?
            .unnamespaced_borrow_mut()
            .incr("example_of_accessing_the_raw_redis_connection", 1)
            .await?;

        debug!({ "times_called" = times_called }, "Called this worker");

        self.send_report(args.user_guid).await
    }
}

struct FilterExpiredUsersMiddleware;

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
        redis: RedisPool,
    ) -> Result<()> {
        let args: std::result::Result<(FiltereExpiredUsersArgs,), serde_json::Error> =
            serde_json::from_value(job.args.clone());

        // If we can safely deserialize then attempt to filter based on user guid.
        if let Ok((filter,)) = args {
            if filter.is_expired() {
                error!({
                    "class" = &job.class,
                    "jid" = &job.jid,
                    "user_guid" = filter.user_guid
                }, "Detected an expired user, skipping this job");
                return Ok(());
            }
        }

        chain.next(job, worker, redis).await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

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
    let mut p = Processor::new(redis.clone(), vec!["yolo".to_string(), "brolo".to_string()]);

    // Add known workers
    p.register(HelloWorker);
    p.register(PaymentReportWorker::new(redis.clone()));

    // Custom Middlewares
    p.using(FilterExpiredUsersMiddleware).await;

    // Reset cron jobs
    periodic::destroy_all(redis.clone()).await?;

    // Cron jobs
    periodic::builder("0 * * * * *")?
        .name("Payment report processing for a user using json args")
        .queue("yolo")
        .args(json!({ "user_guid": "USR-123-PERIODIC-FROM-JSON-ARGS" }))?
        .register(&mut p, PaymentReportWorker::new(redis.clone()))
        .await?;

    periodic::builder("0 * * * * *")?
        .name("Payment report processing for a user using typed args")
        .queue("yolo")
        .args(PaymentReportArgs {
            user_guid: "USR-123-PERIODIC-FROM-TYPED-ARGS".to_string(),
        })?
        .register(&mut p, PaymentReportWorker::new(redis.clone()))
        .await?;

    p.run().await;
    Ok(())
}
