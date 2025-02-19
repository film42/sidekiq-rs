use super::Result;
use crate::{
    periodic::PeriodicJob, Chain, Counter, Job, RedisPool, Scheduled, ServerMiddleware,
    StatsPublisher, UnitOfWork, Worker, WorkerRef,
};
use std::collections::{BTreeMap, VecDeque};
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
    queues: VecDeque<String>,
    human_readable_queues: Vec<String>,
    periodic_jobs: Vec<PeriodicJob>,
    workers: BTreeMap<String, Arc<WorkerRef>>,
    chain: Chain,
    busy_jobs: Counter,
    cancellation_token: CancellationToken,
    config: ProcessorConfig,
}

#[derive(Clone)]
#[non_exhaustive]
pub struct ProcessorConfig {
    /// The number of Sidekiq workers that can run at the same time. Adjust as needed based on
    /// your workload and resource (cpu/memory/etc) usage.
    ///
    /// This config value controls how many workers are spawned to handle the queues provided
    /// to [`Processor::new`]. These workers will be shared across all of these queues.
    ///
    /// If your workload is largely CPU-bound (computationally expensive), this should probably
    /// match your CPU count. This is the default.
    ///
    /// If your workload is largely IO-bound (e.g. reading from a DB, making web requests and
    /// waiting for responses, etc), this can probably be quite a bit higher than your CPU count.
    pub num_workers: usize,

    /// The strategy for balancing the priority of fetching queues' jobs from Redis. Defaults
    /// to [`BalanceStrategy::RoundRobin`].
    ///
    /// The Redis API used to fetch jobs ([brpop](https://redis.io/docs/latest/commands/brpop/))
    /// checks queues for jobs in the order the queues are provided. This means that if the first
    /// queue in the list provided to [`Processor::new`] always has an item, the other queues
    /// will never have their jobs run. To mitigate this, a [`BalanceStrategy`] can be provided
    /// to allow ensuring that no queue is starved indefinitely.
    pub balance_strategy: BalanceStrategy,

    /// Queue-specific configurations. The queues specified in this field do not need to match
    /// the list of queues provided to [`Processor::new`].
    pub queue_configs: BTreeMap<String, QueueConfig>,
}

#[derive(Default, Clone)]
#[non_exhaustive]
pub enum BalanceStrategy {
    /// Rotate the list of queues by 1 every time jobs are fetched from Redis. This allows each
    /// queue in the list to have an equal opportunity to have its jobs run.
    #[default]
    RoundRobin,
    /// Do not modify the list of queues. Warning: This can lead to queue starvation! For example,
    /// if the first queue in the list provided to [`Processor::new`] is heavily used and always
    /// has a job available to run, then the jobs in the other queues will never run.
    None,
}

#[derive(Default, Clone)]
#[non_exhaustive]
pub struct QueueConfig {
    /// Similar to `ProcessorConfig#num_workers`, except allows configuring the number of
    /// additional workers to dedicate to a specific queue. If provided, `num_workers` additional
    /// workers will be created for this specific queue.
    pub num_workers: usize,
}

impl ProcessorConfig {
    #[must_use]
    pub fn num_workers(mut self, num_workers: usize) -> Self {
        self.num_workers = num_workers;
        self
    }

    #[must_use]
    pub fn balance_strategy(mut self, balance_strategy: BalanceStrategy) -> Self {
        self.balance_strategy = balance_strategy;
        self
    }

    #[must_use]
    pub fn queue_config(mut self, queue: String, config: QueueConfig) -> Self {
        self.queue_configs.insert(queue, config);
        self
    }
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            num_workers: num_cpus::get(),
            balance_strategy: Default::default(),
            queue_configs: Default::default(),
        }
    }
}

impl QueueConfig {
    #[must_use]
    pub fn num_workers(mut self, num_workers: usize) -> Self {
        self.num_workers = num_workers;
        self
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

    pub async fn fetch(&mut self) -> Result<Option<UnitOfWork>> {
        self.run_balance_strategy();

        let response: Option<(String, String)> = self
            .redis
            .get()
            .await?
            .brpop(self.queues.clone().into(), 2)
            .await?;

        if let Some((queue, job_raw)) = response {
            let job: Job = serde_json::from_str(&job_raw)?;
            return Ok(Some(UnitOfWork { queue, job }));
        }

        Ok(None)
    }

    /// Re-order the `Processor#queues` based on the `ProcessorConfig#balance_strategy`.
    fn run_balance_strategy(&mut self) {
        if self.queues.is_empty() {
            return;
        }

        match self.config.balance_strategy {
            BalanceStrategy::RoundRobin => self.queues.rotate_right(1),
            BalanceStrategy::None => {}
        }
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

        // Logic for spawning shared workers (workers that handles multiple queues) and dedicated
        // workers (workers that handle a single queue).
        let spawn_worker = |mut processor: Processor,
                            cancellation_token: CancellationToken,
                            num: usize,
                            dedicated_queue_name: Option<String>| {
            async move {
                loop {
                    if let Err(err) = processor.process_one().await {
                        error!("Error leaked out the bottom: {:?}", err);
                    }

                    if cancellation_token.is_cancelled() {
                        break;
                    }
                }

                let dedicated_queue_str = dedicated_queue_name
                    .map(|name| format!(" dedicated to queue '{name}'"))
                    .unwrap_or_default();
                debug!("Broke out of loop for worker {num}{dedicated_queue_str}");
            }
        };

        // Start worker routines.
        for i in 0..self.config.num_workers {
            join_set.spawn(spawn_worker(
                self.clone(),
                self.cancellation_token.clone(),
                i,
                None,
            ));
        }

        // Start dedicated worker routines.
        for (queue, config) in &self.config.queue_configs {
            for i in 0..config.num_workers {
                join_set.spawn({
                    let mut processor = self.clone();
                    processor.queues = [queue.clone()].into();
                    spawn_worker(
                        processor,
                        self.cancellation_token.clone(),
                        i,
                        Some(queue.clone()),
                    )
                });
            }
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

                let stats_publisher =
                    StatsPublisher::new(hostname, queues, busy_jobs, self.config.num_workers);

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

        // Start retry and scheduled routines.
        join_set.spawn({
            let redis = self.redis.clone();
            let cancellation_token = self.cancellation_token.clone();
            async move {
                let sched = Scheduled::new(redis);
                let sorted_sets = vec!["retry".to_string(), "schedule".to_string()];

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
