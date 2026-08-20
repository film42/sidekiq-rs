use super::Result;
use crate::Error;
use crate::stats::generate_tid;
use crate::{
    Chain, Counter, Job, RedisPool, Scheduled, ServerMiddleware, StatsPublisher, UnitOfWork,
    Worker, WorkerRef, periodic::PeriodicJob,
};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
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
    // Sidekiq-web WorkSet bookkeeping. Both are assigned per-worker by `run()`;
    // when unset (e.g. a bare `process_one()` call), WorkSet publishing is
    // skipped. `identity` is the shared process identity (the same one the
    // heartbeat registers in `processes`); `tid` is this worker's thread id.
    identity: Option<String>,
    tid: Option<String>,
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

    /// When `true` (the default), [`Processor::run`] polls Redis `retry` and `schedule`
    /// sorted sets and re-enqueues due jobs. Disable when another process owns scheduling.
    pub enable_scheduled: bool,

    /// When `true` (the default), [`Processor::run`] polls and enqueues periodic/cron jobs.
    pub enable_periodic: bool,

    /// When `true` (the default), [`Processor::run`] publishes Sidekiq-web heartbeats
    /// (`processes` set + WorkSet). Workers still get a process identity for WorkSet
    /// bookkeeping either way.
    pub enable_stats: bool,
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

    #[must_use]
    pub fn enable_scheduled(mut self, enable_scheduled: bool) -> Self {
        self.enable_scheduled = enable_scheduled;
        self
    }

    #[must_use]
    pub fn enable_periodic(mut self, enable_periodic: bool) -> Self {
        self.enable_periodic = enable_periodic;
        self
    }

    #[must_use]
    pub fn enable_stats(mut self, enable_stats: bool) -> Self {
        self.enable_stats = enable_stats;
        self
    }
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            num_workers: num_cpus::get(),
            balance_strategy: Default::default(),
            queue_configs: Default::default(),
            enable_scheduled: true,
            enable_periodic: true,
            enable_stats: true,
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
            identity: None,
            tid: None,
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
        let work = work.expect("polled and found some work");

        let started = std::time::Instant::now();

        info!({
            "status" = "start",
            "class" = &work.job.class,
            "queue" = &work.job.queue,
            "jid" = &work.job.jid
        }, "sidekiq");

        let worker = if let Some(worker) = self.workers.get(&work.job.class) {
            worker.clone()
        } else {
            Arc::new(WorkerRef::not_found(work.job.class.clone()))
        };

        // Publish this job to the Sidekiq WorkSet (`<identity>:work`) so it shows
        // on the web "Busy" page, then clear it whether the job succeeds or fails.
        self.set_work(&work).await;
        let result = self.chain.call(&work.job, worker, self.redis.clone()).await;
        self.clear_work().await;
        result?;

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

    /// Record an in-flight job in this process's Sidekiq WorkSet
    /// (`<identity>:work`) so it shows on the web UI "Busy" page. Best-effort:
    /// any Redis error is logged and never interrupts job processing. A no-op
    /// unless an `identity` + `tid` were assigned (i.e. running under `run()`).
    async fn set_work(&self, work: &UnitOfWork) {
        let (Some(identity), Some(tid)) = (self.identity.as_deref(), self.tid.as_deref()) else {
            return;
        };

        let result: Result<()> = async {
            let key = format!("{identity}:work");
            let mut conn = self.redis.get().await?;
            conn.hset(key.clone(), tid.to_string(), work_record(&work.job)?)
                .await?;
            conn.expire(key, 60).await?;
            Ok(())
        }
        .await;

        if let Err(err) = result {
            error!("Error recording sidekiq work state: {:?}", err);
        }
    }

    /// Clear this worker's WorkSet entry once the job finishes (success or fail).
    async fn clear_work(&self) {
        let (Some(identity), Some(tid)) = (self.identity.as_deref(), self.tid.as_deref()) else {
            return;
        };

        let result: Result<()> = async {
            let mut conn = self.redis.get().await?;
            conn.hdel(format!("{identity}:work"), tid.to_string())
                .await?;
            Ok(())
        }
        .await;

        if let Err(err) = result {
            error!("Error clearing sidekiq work state: {:?}", err);
        }
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
    ///
    /// Returns `Ok(())` after a graceful cancellation (via [`Self::get_cancellation_token`]).
    /// If a spawned task panics or a worker loop exits without cancellation, remaining
    /// tasks are cancelled and this returns `Err`.
    pub async fn run(self) -> Result<()> {
        let mut join_set: JoinSet<()> = JoinSet::new();

        // Build the stats publisher up front so its process identity can be shared
        // with the workers: each worker records its in-flight job under that
        // identity's WorkSet (`<identity>:work`) — the same identity the heartbeat
        // registers in the `processes` set — so the web "Busy" page lists running
        // jobs against this process.
        let hostname = if let Some(host) = gethostname::gethostname().to_str() {
            host.to_string()
        } else {
            "UNKNOWN_HOSTNAME".to_string()
        };
        let stats_publisher = StatsPublisher::new(
            hostname,
            self.human_readable_queues.clone(),
            self.busy_jobs.clone(),
            self.config.num_workers,
        );
        let identity = stats_publisher.identity().to_string();
        let mut task_names: HashMap<tokio::task::Id, String> = HashMap::new();

        // Logic for spawning shared workers (workers that handles multiple queues) and dedicated
        // workers (workers that handle a single queue).
        let spawn_worker = |mut processor: Processor,
                            cancellation_token: CancellationToken,
                            task_name: String| {
            async move {
                loop {
                    if let Err(error) = processor.process_one().await {
                        error!(%task_name, ?error, "Error leaked out the bottom");
                    }

                    if cancellation_token.is_cancelled() {
                        break;
                    }
                }

                debug!(
                    %task_name,
                    "Broke out of worker loop"
                );
            }
        };

        // Start worker routines.
        for i in 0..self.config.num_workers {
            let mut processor = self.clone();
            processor.identity = Some(identity.clone());
            processor.tid = Some(generate_tid());
            let name = format!("shared-worker-{i}");
            spawn_named(
                &mut join_set,
                &mut task_names,
                name.clone(),
                spawn_worker(processor, self.cancellation_token.clone(), name),
            );
        }

        // Start dedicated worker routines.
        // Queue config keys are human-readable names (same as `Processor::new`);
        // Redis list keys use the `queue:` prefix.
        for (queue, config) in &self.config.queue_configs {
            for i in 0..config.num_workers {
                let mut processor = self.clone();
                processor.queues = [format!("queue:{queue}")].into();
                processor.human_readable_queues = [queue.clone()].into();
                processor.identity = Some(identity.clone());
                processor.tid = Some(generate_tid());
                let name = format!("worker-{i}-queue-{queue}");
                spawn_named(
                    &mut join_set,
                    &mut task_names,
                    name.clone(),
                    spawn_worker(processor, self.cancellation_token.clone(), name),
                );
            }
        }

        if self.config.enable_stats {
            // Start sidekiq-web metrics publisher. Consumes the `stats_publisher` built
            // above (whose identity the workers share for the WorkSet).
            spawn_named(&mut join_set, &mut task_names, "stats", {
                let redis = self.redis.clone();
                let cancellation_token = self.cancellation_token.clone();
                async move {
                    loop {
                        // TODO: Use process count to meet a 5 second avg.
                        select! {
                            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
                            _ = cancellation_token.cancelled() => {
                                break;
                            }
                        }

                        if let Err(error) = stats_publisher.publish_stats(redis.clone()).await {
                            error!(?error, "Error publishing processor stats");
                        }
                    }

                    // On graceful shutdown, remove the process from the `processes` set and
                    // delete the heartbeat hash. This mirrors Ruby Sidekiq's clear_heartbeat():
                    //   pipeline.srem("processes", [identity])
                    //   pipeline.unlink("#{identity}:work")
                    // Without this, stale entries accumulate in the `processes` set until the
                    // heartbeat hash's 60-second TTL expires — but the set membership has no TTL
                    // and never self-cleans.
                    let identity = stats_publisher.identity().to_string();
                    if let Err(error) = stats_publisher.deregister(redis.clone()).await {
                        error!(
                            ?error,
                            "Error deregistering processor from Redis on shutdown",
                        );
                    }

                    debug!(identity = %identity, "Deregistered processor from Redis");
                }
            });
        }

        if self.config.enable_scheduled {
            // Start retry and scheduled routines.
            spawn_named(&mut join_set, &mut task_names, "retry-and-scheduled", {
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

                        if let Err(error) =
                            sched.enqueue_jobs(chrono::Utc::now(), &sorted_sets).await
                        {
                            error!(?error, "Error in scheduled poller routine");
                        }
                    }

                    debug!("Broke out of loop for retry and scheduled");
                }
            });
        }

        if self.config.enable_periodic {
            // Watch for periodic jobs and enqueue jobs.
            spawn_named(&mut join_set, &mut task_names, "periodic", {
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

                        if let Err(error) = sched.enqueue_periodic_jobs(chrono::Utc::now()).await {
                            error!(?error, "Error in periodic job poller routine");
                        }
                    }

                    debug!("Broke out of loop for periodic");
                }
            });
        }

        let mut fatal: Option<Error> = None;
        while let Some(result) = join_set.join_next_with_id().await {
            match result {
                Ok((id, ())) if self.cancellation_token.is_cancelled() => {
                    // Task exited gracefully when the cancellation token was cancelled.
                    task_names.remove(&id);
                }
                Ok((id, ())) => {
                    // Task exited unexpectedly.
                    let task_name = take_task_name(&mut task_names, id);
                    error!(
                        %task_name,
                        "Processor worker exited unexpectedly. Cancelling remaining tasks."
                    );
                    self.cancellation_token.cancel();
                    fatal.get_or_insert_with(|| {
                        Error::Message(format!(
                            "Processor worker '{task_name}' exited unexpectedly"
                        ))
                    });
                }
                Err(join_err) => {
                    // Task panicked.
                    let task_name = take_task_name(&mut task_names, join_err.id());
                    error!(
                        %task_name,
                        error = ?join_err,
                        "Processor had a spawned task return an error. Cancelling remaining tasks.",
                    );
                    self.cancellation_token.cancel();
                    fatal.get_or_insert_with(|| {
                        Error::Message(format!(
                            "Processor worker '{task_name}' panicked: {join_err}"
                        ))
                    });
                }
            }
        }

        // If any task panicked or exited unexpectedly, return an error.
        match fatal {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    pub async fn using<M>(&mut self, middleware: M)
    where
        M: ServerMiddleware + Send + Sync + 'static,
    {
        self.chain.using(Box::new(middleware)).await;
    }
}

fn spawn_named<F>(
    join_set: &mut JoinSet<()>,
    names: &mut HashMap<tokio::task::Id, String>,
    name: impl Into<String>,
    fut: F,
) where
    F: Future<Output = ()> + Send + 'static,
{
    let handle = join_set.spawn(fut);
    names.insert(handle.id(), name.into());
}

fn take_task_name(names: &mut HashMap<tokio::task::Id, String>, id: tokio::task::Id) -> String {
    names.remove(&id).unwrap_or_else(|| "unknown".to_string())
}

/// Build the value stored in the `<identity>:work` hash for an in-flight job,
/// matching Ruby Sidekiq's `{queue, payload, run_at}` work record. `payload` is
/// the job JSON as a *string*, exactly as Sidekiq stores it and the web UI
/// expects it (`Sidekiq.load_json(work.payload)`).
fn work_record(job: &Job) -> Result<String> {
    let record = serde_json::json!({
        "queue": job.queue,
        "payload": serde_json::to_string(job)?,
        "run_at": chrono::Utc::now().timestamp(),
    });

    Ok(record.to_string())
}

#[cfg(test)]
mod work_set_tests {
    use super::*;

    #[test]
    fn work_record_matches_sidekiq_shape() {
        let job: Job = serde_json::from_str(
            r#"{"queue":"default","args":[1,"x"],"retry":true,"class":"HardWorker","jid":"abc123","created_at":1700000000.0}"#,
        )
        .expect("parse job");

        let record: serde_json::Value =
            serde_json::from_str(&work_record(&job).expect("build record")).expect("parse record");

        assert_eq!(record["queue"], "default");
        assert!(record["run_at"].is_number());

        // `payload` must be a JSON *string* (the job JSON), not a nested object.
        let payload = record["payload"].as_str().expect("payload is a string");
        let payload: serde_json::Value = serde_json::from_str(payload).expect("parse payload");
        assert_eq!(payload["class"], "HardWorker");
        assert_eq!(payload["jid"], "abc123");
        assert_eq!(payload["args"][1], "x");
        assert!(payload["args"][0].is_number());
    }
}
