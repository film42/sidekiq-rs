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
use tracing::info;

#[derive(Clone)]
struct PaymentReportWorker {}

impl PaymentReportWorker {
    fn new() -> Self {
        Self { }
    }

    async fn send_report(&self, user_guid: String) -> Result<(), Box<dyn std::error::Error>> {
        // TODO: Some actual work goes here...
        info!({"user_guid" = user_guid}, "Sending payment report to user");

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

#### Unique jobs

Unique jobs are supported via the `unique_for` option which can be defined by default on the
worker or via `SomeWorker::opts().unique_for(duration)`. See the `examples/unique.rs` example
to only enqueue a job that is unique via (worker_name, queue_name, sha256_hash_of_job_args) for
some defined `ttl`. Note: This is using `SET key value NX EX duration` under the hood as a "good
enough" lock on the job.


## Starting the Server

Below is an example of how you should create a `Processor`, register workers, include any
custom middlewares, and start the server.

```rust
// Redis
let manager = sidekiq::RedisConnectionManager::new("redis://127.0.0.1/").unwrap();
let mut redis = bb8::Pool::builder().build(manager).await.unwrap();

// Sidekiq server
let mut p = Processor::new(
    redis,
    vec!["yolo".to_string(), "brolo".to_string()],
);

// Add known workers
p.register(PaymentReportWorker::new());

// Custom Middlewares
p.using(FilterExpiredUsersMiddleware::new())
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
processes will periodically poll for changes and atomically update the score to the new
next scheduled UTC time for the cron string. The worker that successfully changes the
score atomically will enqueue a new job. Processes that don't successfully update the
score will move on. This implementation detail means periodic jobs never leave redis.
Another detail is that json when decoded and then encoded might not produce the same
value as the original string. Ex: `{"a":"b","c":"d"}` might become `{"c":"d","a":b"}`.
To keep the json representation consistent, when updating a periodic job with its new
score in redis, the original json string will be used again to keep things consistent.


## Server Middleware

One great feature of sidekiq is its middleware pattern. This library reimplements the
sidekiq server middleware pattern in rust. In the example below supposes you have an
app that performs work only for paying customers. The middleware below will hault jobs
from being executed if the customers have expired. One thing kind of interesting about
the implementation is that we can rely on serde to conditionally type-check workers.
For example, suppose I only care about user-centric workers, and I identify those by their
`user_guid` as a parameter. With serde it's easy to validate your paramters.

```rust
use tracing::info;

struct FilterExpiredUsersMiddleware {}

impl FilterExpiredUsersMiddleware {
    fn new() -> Self {
        Self { }
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
        redis: RedisPool,
    ) -> ServerResult {
        // Use serde to check if a user_guid is part of the job args.
        let args: Result<(FiltereExpiredUsersArgs,), serde_json::Error> =
            serde_json::from_value(job.args.clone());

        // If we can safely deserialize then attempt to filter based on user guid.
        if let Ok((filter,)) = args {
            if filter.is_expired() {
                error!({
                    "class" = job.class,
                    "jid" = job.jid,
                    "user_guid" = filter.user_guid },
                    "Detected an expired user, skipping this job"
                );
                return Ok(());
            }
        }

        // This customer is not expired, so we may continue.
        chain.next(job, worker, redis).await
    }
}
```

## Customization Details

### Namespacing the workers

It's still very common to use the `redis-namespace` gem with ruby sidekiq workers. This library
supports namespacing redis commands by using a connection customizer when you build the connection
pool.

```rust
let manager = sidekiq::RedisConnectionManager::new("redis://127.0.0.1/")?;
let redis = bb8::Pool::builder()
    .connection_customizer(sidekiq::with_custom_namespace("my_cool_app".to_string()))
    .build(manager)
    .await?;
```

Now all commands used by this library will be prefixed with `my_cool_app:`, example: `ZDEL my_cool_app:scheduled {...}`.

### Passing database connections into the workers

Workers will often need access to other software components like database connections, http clients,
etc. You can define these on your worker struct so long as they implement `Clone`. Example:

```rust
use tracing::debug;

#[derive(Clone)]
struct ExampleWorker {
    redis: RedisPool,
}


#[async_trait]
impl Worker<()> for ExampleWorker {
    async fn perform(&self, args: PaymentReportArgs) -> Result<(), Box<dyn std::error::Error>> {
        use redis::AsyncCommands;

        // And then they are available here...
        let times_called: usize = self
            .redis
            .get()
            .await?
            .unnamespaced_borrow_mut()
            .incr("example_of_accessing_the_raw_redis_connection", 1)
            .await?;

        debug!({"times_called" = times_called}, "Called this worker");
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
// ...
    let mut p = Processor::new(
        redis.clone(),
        vec!["low_priority".to_string()],
    );

    p.register(ExampleWorker{ redis: redis.clone() });
}
```

### Customizing the worker name for workers under a nested ruby module

You mind find that your worker under a module does not match with a ruby worker under a module.
A nested rusty-sidekiq worker `workers::MyWorker` will only keep the final type name `MyWorker` when
registering the worker for some "class name". Meaning, if a ruby worker is enqueued with the class
`Workers::MyWorker`, the `workers::MyWorker` type will not process that work. This is because by default
the class name is generated at compile time based on the worker struct name. To override this, redefine one
of the default trait methods:

```rust
pub struct MyWorker;

#[async_trait]
impl Worker<()> for MyWorker {
    async fn perform(&self, _args: ()) -> ServerResult {
        Ok(())
    }

    fn class_name() -> String
    where
        Self: Sized,
    {
        "Workers::MyWorker".to_string()
    }
}
```

And now when ruby enqueues a `Workers::MyWorker` job, it will be picked up by rust-sidekiq.


## License

MIT
