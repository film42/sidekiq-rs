use super::Result;
use crate::{
    periodic::PeriodicJob, Chain, Counter, Job, RedisPool, Scheduled, ServerMiddleware,
    StatsPublisher, UnitOfWork, Worker, WorkerRef,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::select;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum WorkFetcher {
    NoWorkFound,
    Done,
}

#[derive(Clone)]
pub struct Processor {
    redis: RedisPool,
    queues: Vec<String>,
    human_readable_queues: Vec<String>,
    periodic_jobs: Vec<PeriodicJob>,
    workers: BTreeMap<String, Arc<WorkerRef>>,
    chain: Chain,
    busy_jobs: Counter,
    cancellation_token: CancellationToken,
    config: ProcessorConfig,
}

#[derive(Clone)]
#[allow(clippy::manual_non_exhaustive)]
pub struct ProcessorConfig {
    /// The number of Sidekiq workers that can run at the same time. Adjust as needed based on
    /// your workload and resource (cpu/memory/etc) usage.
    ///
    /// If your workload is largely CPU-bound (computationally expensive), this should probably
    /// match your CPU count. This is the default.
    ///
    /// If your workload is largely IO-bound (e.g. reading from a DB, making web requests and
    /// waiting for responses, etc), this can probably be quite a bit higher than your CPU count.
    pub num_workers: usize,
    pub enqueue_periodic_jobs: bool,
    pub enqueue_retry_jobs: bool,
    pub enqueue_scheduled_jobs: bool,
    // Disallow consumers from directly creating a ProcessorConfig object.
    _private: (),
}

impl ProcessorConfig {
    #[must_use]
    pub fn num_workers(mut self, num_workers: usize) -> Self {
        self.num_workers = num_workers;
        self
    }

    #[must_use]
    pub fn enqueue_periodic_jobs(mut self, enqueue_periodic_jobs: bool) -> Self {
        self.enqueue_periodic_jobs = enqueue_periodic_jobs;
        self
    }

    #[must_use]
    pub fn enqueue_retry_jobs(mut self, enqueue_retry_jobs: bool) -> Self {
        self.enqueue_retry_jobs = enqueue_retry_jobs;
        self
    }

    #[must_use]
    pub fn enqueue_scheduled_jobs(mut self, enqueue_scheduled_jobs: bool) -> Self {
        self.enqueue_scheduled_jobs = enqueue_scheduled_jobs;
        self
    }
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            num_workers: num_cpus::get(),
            enqueue_periodic_jobs: true,
            enqueue_retry_jobs: true,
            enqueue_scheduled_jobs: true,
            _private: Default::default(),
        }
    }
}

impl Processor {
    #[must_use]
    pub fn new(redis: RedisPool, queues: Vec<String>) -> Self {
        let busy_jobs = Counter::new(0);

        Self {
            chain: Chain::new_with_stats(busy_jobs.clone()),
            workers: BTreeMap::new(),
            periodic_jobs: vec![],
            busy_jobs,

            redis,
            queues: queues
                .iter()
                .map(|queue| format!("queue:{queue}"))
                .collect(),
            human_readable_queues: queues,
            cancellation_token: CancellationToken::new(),
            config: Default::default(),
        }
    }

    pub fn with_config(mut self, config: ProcessorConfig) -> Self {
        self.config = config;
        self
    }

    async fn fetch(&mut self) -> Result<Option<UnitOfWork>> {
        let response: Option<(String, String)> = self
            .redis
            .get()
            .await?
            .brpop(self.queues.clone(), 2)
            .await?;

        if let Some((queue, job_raw)) = response {
            let job: Job = serde_json::from_str(&job_raw)?;
            return Ok(Some(UnitOfWork { queue, job }));
        }

        Ok(None)
    }

    pub async fn process_one(&mut self) -> Result<()> {
        loop {
            if self.cancellation_token.is_cancelled() {
                return Ok(());
            }

            if let WorkFetcher::NoWorkFound = self.process_one_tick_once().await? {
                continue;
            }

            return Ok(());
        }
    }

    pub async fn process_one_tick_once(&mut self) -> Result<WorkFetcher> {
        let work = self.fetch().await?;

        if work.is_none() {
            // If there is no job to handle, we need to add a `yield_now` in order to allow tokio's
            // scheduler to wake up another task that may be waiting to acquire a connection from
            // the Redis connection pool. See the following issue for more details:
            // https://github.com/film42/sidekiq-rs/issues/43
            tokio::task::yield_now().await;
            return Ok(WorkFetcher::NoWorkFound);
        }
        let mut work = work.expect("polled and found some work");

        let started = std::time::Instant::now();

        info!({
            "status" = "start",
            "class" = &work.job.class,
            "queue" = &work.job.queue,
            "jid" = &work.job.jid
        }, "sidekiq");

        if let Some(worker) = self.workers.get_mut(&work.job.class) {
            self.chain
                .call(&work.job, worker.clone(), self.redis.clone())
                .await?;
        } else {
            error!({
                "staus" = "fail",
                "class" = &work.job.class,
                "queue" = &work.job.queue,
                "jid" = &work.job.jid
            },"!!! Worker not found !!!");
            work.reenqueue(&self.redis).await?;
        }

        // TODO: Make this only say "done" when the job is successful.
        // We might need to change the ChainIter to return the final job and
        // detect any retries?
        info!({
            "elapsed" = format!("{:?}", started.elapsed()),
            "status" = "done",
            "class" = &work.job.class,
            "queue" = &work.job.queue,
            "jid" = &work.job.jid}, "sidekiq");

        Ok(WorkFetcher::Done)
    }

    pub fn register<
        Args: Sync + Send + for<'de> serde::Deserialize<'de> + 'static,
        W: Worker<Args> + 'static,
    >(
        &mut self,
        worker: W,
    ) {
        self.workers
            .insert(W::class_name(), Arc::new(WorkerRef::wrap(Arc::new(worker))));
    }

    pub fn get_cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    pub(crate) async fn register_periodic(&mut self, periodic_job: PeriodicJob) -> Result<()> {
        self.periodic_jobs.push(periodic_job.clone());

        let mut conn = self.redis.get().await?;
        periodic_job.insert(&mut conn).await?;

        info!({
            "args" = &periodic_job.args,
            "class" = &periodic_job.class,
            "queue" = &periodic_job.queue,
            "name" = &periodic_job.name,
            "cron" = &periodic_job.cron,
        },"Inserting periodic job");

        Ok(())
    }

    /// Takes self to consume the processor. This is for life-cycle management, not
    /// memory safety because you can clone processor pretty easily.
    pub async fn run(self) {
        let mut join_set: JoinSet<()> = JoinSet::new();

        // Start worker routines.
        for i in 0..self.config.num_workers {
            join_set.spawn({
                let mut processor = self.clone();
                let cancellation_token = self.cancellation_token.clone();

                async move {
                    loop {
                        if let Err(err) = processor.process_one().await {
                            error!("Error leaked out the bottom: {:?}", err);
                        }

                        if cancellation_token.is_cancelled() {
                            break;
                        }
                    }

                    debug!("Broke out of loop for worker {}", i);
                }
            });
        }

        // Start sidekiq-web metrics publisher.
        join_set.spawn({
            let redis = self.redis.clone();
            let queues = self.human_readable_queues.clone();
            let busy_jobs = self.busy_jobs.clone();
            let cancellation_token = self.cancellation_token.clone();
            async move {
                let hostname = if let Some(host) = gethostname::gethostname().to_str() {
                    host.to_string()
                } else {
                    "UNKNOWN_HOSTNAME".to_string()
                };

                let stats_publisher = StatsPublisher::new(hostname, queues, busy_jobs);

                loop {
                    // TODO: Use process count to meet a 5 second avg.
                    select! {
                        _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
                        _ = cancellation_token.cancelled() => {
                            break;
                        }
                    }

                    if let Err(err) = stats_publisher.publish_stats(redis.clone()).await {
                        error!("Error publishing processor stats: {:?}", err);
                    }
                }

                debug!("Broke out of loop web metrics");
            }
        });

        if self.config.enqueue_retry_jobs || self.config.enqueue_scheduled_jobs {
            // Start retry and scheduled routines.
            join_set.spawn({
                let redis = self.redis.clone();
                let cancellation_token = self.cancellation_token.clone();
                async move {
                    let sched = Scheduled::new(redis);
                    let mut sorted_sets = vec![];
                    if self.config.enqueue_retry_jobs {
                        sorted_sets.push("retry".to_string());
                    }
                    if self.config.enqueue_scheduled_jobs {
                        sorted_sets.push("schedule".to_string());
                    }

                    loop {
                        // TODO: Use process count to meet a 5 second avg.
                        select! {
                            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
                            _ = cancellation_token.cancelled() => {
                                break;
                            }
                        }

                        if let Err(err) = sched.enqueue_jobs(chrono::Utc::now(), &sorted_sets).await {
                            error!("Error in scheduled poller routine: {:?}", err);
                        }
                    }

                    debug!("Broke out of loop for retry and scheduled");
                }
            });
        }

        if self.config.enqueue_periodic_jobs {
            // Watch for periodic jobs and enqueue jobs.
            join_set.spawn({
                let redis = self.redis.clone();
                let cancellation_token = self.cancellation_token.clone();
                async move {
                    let sched = Scheduled::new(redis);

                    loop {
                        // TODO: Use process count to meet a 30 second avg.
                        select! {
                            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {}
                            _ = cancellation_token.cancelled() => {
                                break;
                            }
                        }

                        if let Err(err) = sched.enqueue_periodic_jobs(chrono::Utc::now()).await {
                            error!("Error in periodic job poller routine: {}", err);
                        }
                    }

                    debug!("Broke out of loop for periodic");
                }
            });
        }

        while let Some(result) = join_set.join_next().await {
            if let Err(err) = result {
                error!("Processor had a spawned task return an error: {}", err);
            }
        }
    }

    pub async fn using<M>(&mut self, middleware: M)
    where
        M: ServerMiddleware + Send + Sync + 'static,
    {
        self.chain.using(Box::new(middleware)).await;
    }
}
