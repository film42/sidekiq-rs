use async_trait::async_trait;
use bb8::Pool;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sidekiq::{Processor, RedisConnectionManager, Worker};
use slog::{o, Drain};

#[derive(Clone)]
struct CustomerNotificationWorker;

#[async_trait]
impl Worker<CustomerNotification> for CustomerNotificationWorker {
    fn opts() -> sidekiq::WorkerOpts<CustomerNotification, Self> {
        // Use default options to set the unique_for option by default.
        sidekiq::WorkerOpts::new()
            .queue("customers")
            .unique_for(std::time::Duration::from_secs(30))
    }

    async fn perform(&self, _args: CustomerNotification) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}

#[derive(Deserialize, Debug, Serialize)]
struct CustomerNotification {
    customer_guid: String,
    date_string: String,
}

#[derive(Clone)]
struct CustomerSuperUniqueNotificationWorker;

#[async_trait]
impl Worker<CustomerNotification> for CustomerSuperUniqueNotificationWorker {
    fn opts() -> sidekiq::WorkerOpts<CustomerNotification, Self> {
        // Use default options to set the unique_for option by default.
        sidekiq::WorkerOpts::new()
            .queue("customers")
            .unique_for(std::time::Duration::from_secs(30))
    }

    fn unique_hash_for_args(
        args: &CustomerNotification,
    ) -> Result<String, Box<dyn std::error::Error>> {
        Ok(format!("{:x}", Sha256::digest(&args.customer_guid)))
    }

    async fn perform(&self, _args: CustomerNotification) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
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

    // Sidekiq server
    let mut p = Processor::new(redis.clone(), logger.clone(), vec!["customers".to_string()]);

    // Add known workers
    p.register(CustomerNotificationWorker);
    p.register(CustomerSuperUniqueNotificationWorker);

    // Create a bunch of jobs with the default uniqueness options. Only
    // one of these should be created within a 30 second period.
    for n in 1..10 {
        for _ in 1..10 {
            let date_string = format!("{n}/{n}/2022");

            CustomerNotificationWorker::perform_async(
                &mut redis,
                CustomerNotification {
                    customer_guid: "CST-123".to_string(),
                    date_string: date_string.clone(),
                },
            )
            .await?;

            CustomerSuperUniqueNotificationWorker::perform_async(
                &mut redis,
                CustomerNotification {
                    customer_guid: "CST-123".to_string(),
                    date_string: date_string.clone(),
                },
            )
            .await?;
        }
    }

    // Override the unique_for option. Note: Because the code above
    // uses the default unique_for value of 30, this code is essentially
    // a no-op.
    CustomerNotificationWorker::opts()
        .unique_for(std::time::Duration::from_secs(90))
        .perform_async(
            &mut redis,
            CustomerNotification {
                customer_guid: "CST-123".to_string(),
                date_string: "01-01-1970".to_string(),
            },
        )
        .await?;

    p.run().await;
    Ok(())
}
