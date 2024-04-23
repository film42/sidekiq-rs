use async_trait::async_trait;
use bb8::Pool;
use serde::{Deserialize, Serialize};
use sidekiq::{Processor, RedisConnectionManager, Result, Worker};

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

    async fn perform(&self, _args: CustomerNotification) -> Result<()> {
        Ok(())
    }
}

#[derive(Deserialize, Debug, Serialize)]
struct CustomerNotification {
    customer_guid: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // Redis
    let manager = RedisConnectionManager::new("redis://127.0.0.1/")?;
    let redis = Pool::builder().build(manager).await?;

    // Sidekiq server
    let mut p = Processor::new(redis.clone(), vec!["customers".to_string()]);

    // Add known workers
    p.register(CustomerNotificationWorker);

    // Create a bunch of jobs with the default uniqueness options. Only
    // one of these should be created within a 30 second period.
    for _ in 1..10 {
        CustomerNotificationWorker::perform_async(
            &redis,
            CustomerNotification {
                customer_guid: "CST-123".to_string(),
            },
        )
        .await?;
    }

    // Override the unique_for option. Note: Because the code above
    // uses the default unique_for value of 30, this code is essentially
    // a no-op.
    CustomerNotificationWorker::opts()
        .unique_for(std::time::Duration::from_secs(90))
        .perform_async(
            &redis,
            CustomerNotification {
                customer_guid: "CST-123".to_string(),
            },
        )
        .await?;

    p.run().await;
    Ok(())
}
