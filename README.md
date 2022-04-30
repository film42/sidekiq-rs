Sidekiq.rs (aka `rusty-sidekiq`)
================================

[![crates.io](https://img.shields.io/crates/v/rusty-sidekiq.svg)](https://crates.io/crates/rusty-sidekiq/)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE.md)
[![Documentation](https://docs.rs/rusty-sidekiq/badge.svg)](https://docs.rs/rusty-sidekiq/)

This is a reimplementation of sidekiq in rust. It is compatible with sidekiq.rb for both submitting and processing jobs.
Sidekiq.rb is obviously much more mature than this repo, but I hope you enjoy using it. This library is built using tokio
so it is async by default.


## The Worker

This library uses serde to make worker arguments strongly typed as needed. Below is an example of a worker with strongly
typed arguments. It also has custom options that will be used whenever a job is submitted. These can be overridden at 
enqueue time making it easy to change the queue name, for example, should you need to.

```rust
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
        info!(self.logger, "Sending payment report to user"; "user_guid" => user_guid);

        Ok(())
    }
}

#[derive(Deserialize, Debug, Serialize)]
struct PaymentReportArgs {
    user_guid: String,
}

#[async_trait]
impl Worker<PaymentReportArgs> for PaymentReportWorker {
    // Default worker options
    fn opts() -> sidekiq::WorkerOpts<Self> {
        sidekiq::WorkerOpts::new().queue("yolo")
    }

    // Worker implementation
    async fn perform(&self, args: PaymentReportArgs) -> Result<(), Box<dyn std::error::Error>> {
        self.send_report(args.user_guid).await
    }
}
```


## Creating a Job

There are several ways to insert a job, but for this example, we'll keep it simple. Given some worker, insert using strongly
typed arguments.

```rust
PaymentReportWorker::perform_async(
    &mut redis,
    PaymentReportArgs {
        user_guid: "USR-123".into(),
    },
)
.await?;
```

You can make custom overrides at enqueue time.

```rust
PaymentReportWorker::opts()
    .queue("brolo")
    .perform_async(
        &mut redis,
        PaymentReportArgs {
            user_guid: "USR-123".into(),
        },
    )
    .await?;
```

Or you can have more control by using the crate level method.

```rust
sidekiq::perform_async(
    &mut redis,
    "PaymentReportWorker".into(),
    "yolo".into(),
    PaymentReportArgs {
        user_guid: "USR-123".to_string(),
    },
)
.await?;
```

See more examples in `examples/demo.rs`.


## Starting the Server

Below is an example of how you should create a `Processor`, register workers, include any
custom middlewares, and start the server.

```rust
// Redis
let manager = RedisConnectionManager::new("redis://127.0.0.1/").unwrap();
let mut redis = Pool::builder().build(manager).await.unwrap();

// Sidekiq server
let mut p = Processor::new(
    redis,
    logger.clone(),
    vec!["yolo".to_string(), "brolo".to_string()],
);

// Add known workers
p.register(PaymentReportWorker::new(logger.clone()));

// Custom Middlewares
p.using(FilterExpiredUsersMiddleware::new(logger.clone()))
    .await;

// Start the server
p.run().await;
```


## Periodic Jobs

Periodic cron jobs are supported out of the box. All you need to specify is a valid
cron string and a worker instance. You can optionally supply arguments, a queue, a
retry flag, and a name that will be logged when a worker is submitted.

Example:

```rust
// Clear out all periodic jobs and their schedules
periodic::destroy_all(redis).await?;

// Add a new periodic job
periodic::builder("0 0 8 * * *")?
    .name("Email clients with an oustanding balance daily at 8am UTC")
    .queue("reminders")
    .args(EmailReminderArgs {
        report_type: "outstanding_balance",
    })?
    .register(&mut p, EmailReminderWorker)
    .await?;
```

Periodic jobs are not removed automatically. If your project adds a periodic job and
then later removes the `periodic::builder` call, the periodic job will still exist in
redis. You can call `periodic::destroy_all(redis).await?` at the start of your program
to ensure only the periodic jobs added by the latest version of your program will be 
executed.

The implementation relies on a sorted set in redis. It stores a json payload of the
periodic job with a score equal to the next scheduled UTC time of the cron string. All
clients will periodically poll for changes and atomically update the score to the new
next scheduled UTC time for the cron string. The worker that successfully changes the
score atomically will enqueue a new job. Processes that don't successfully update the 
score will move on. This implementation detail means periodic jobs never leave redis.
Another detail is that json when decoded and then encoded might not produce the same
value as the original string. Ex: `{"a":"b","c":"d"}` might become `{"c":"d","a":b"}`.
To keep the json representation consistent, when updating a periodic job with its new
score in redis, the original json string will be used again to keep things consistent.


## Server Middleware

One great feature of sidekiq is its middleware patternoriginal jsonif. This
removes the fear of using json as a unique value.
sidekiq server middleware pattern in rust. In the example below supposes you have an
app that performs work only for paying customers. The middleware below will hault jobs
from being executed if the customers have expired. One thing kind of interesting about
the implementation is that we can rely on serde to conditionally type-check workers.
For example, suppose I only care about user-centric workers, and I identify those by their
`user_guid` as a parameter. With serde it's easy to validate your paramters.

```rust
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
        // Use serde to check if a user_guid is part of the job args.
        let args: Result<(FiltereExpiredUsersArgs,), serde_json::Error> =
            serde_json::from_value(job.args.clone());

        // If we can safely deserialize then attempt to filter based on user guid.
        if let Ok((filter,)) = args {
            if filter.is_expired() {
                error!(
                    self.logger,
                    "Detected an expired user, skipping this job";
                    "class" => job.class,
                    "jid" => job.jid,
                    "user_guid" => filter.user_guid,
                );
                return Ok(());
            }
        }

        // This customer is not expired, so we may continue.
        chain.next(job, worker, redis).await
    }
}
```


## License

MIT
