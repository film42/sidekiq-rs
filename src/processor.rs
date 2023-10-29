use crate::{
    periodic::PeriodicJob, Chain, Counter, Job, RedisPool, Scheduled, ServerMiddleware,
    StatsPublisher, UnitOfWork, Worker, WorkerRef,
};
use slog::{error, info};
use std::collections::BTreeMap;
use std::sync::Arc;

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
    logger: slog::Logger,
    chain: Chain,
    busy_jobs: Counter,
}

impl Processor {
    #[must_use]
    pub fn new(redis: RedisPool, logger: slog::Logger, queues: Vec<String>) -> Self {
        let busy_jobs = Counter::new(0);

        Self {
            chain: Chain::new_with_stats(logger.clone(), busy_jobs.clone()),
            workers: BTreeMap::new(),
            periodic_jobs: vec![],
            busy_jobs,

            redis,
            logger,
            queues: queues
                .iter()
                .map(|queue| format!("queue:{queue}"))
                .collect(),
            human_readable_queues: queues,
        }
    }

    async fn fetch(&mut self) -> Result<Option<UnitOfWork>, Box<dyn std::error::Error>> {
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

    pub async fn process_one(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            if let WorkFetcher::NoWorkFound = self.process_one_tick_once().await? {
                continue;
            }

            return Ok(());
        }
    }

    pub async fn process_one_tick_once(
        &mut self,
    ) -> Result<WorkFetcher, Box<dyn std::error::Error>> {
        let work = self.fetch().await?;

        if work.is_none() {
            return Ok(WorkFetcher::NoWorkFound);
        }
        let mut work = work.expect("polled and found some work");

        let started = std::time::Instant::now();

        info!(self.logger, "sidekiq";
            "status" => "start",
            "class" => &work.job.class,
            "queue" => &work.job.queue,
            "jid" => &work.job.jid
        );

        if let Some(worker) = self.workers.get_mut(&work.job.class) {
            self.chain
                .call(&work.job, worker.clone(), self.redis.clone())
                .await?;
        } else {
            error!(
                self.logger,
                "!!! Worker not found !!!";
                "staus" => "fail",
                "class" => &work.job.class,
                "queue" => &work.job.queue,
                "jid" => &work.job.jid,
            );
            work.reenqueue(&self.redis).await?;
        }

        // TODO: Make this only say "done" when the job is successful.
        // We might need to change the ChainIter to return the final job and
        // detect any retries?
        info!(self.logger, "sidekiq";
            "elapsed" => format!("{:?}", started.elapsed()),
            "status" => "done",
            "class" => &work.job.class,
            "queue" => &work.job.queue,
            "jid" => &work.job.jid,
        );

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

    pub(crate) async fn register_periodic(
        &mut self,
        periodic_job: PeriodicJob,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.periodic_jobs.push(periodic_job.clone());

        let mut conn = self.redis.get().await?;
        periodic_job.insert(&mut conn).await?;

        info!(self.logger, "Inserting periodic job";
            "args" => &periodic_job.args,
            "class" => &periodic_job.class,
            "queue" => &periodic_job.queue,
            "name" => &periodic_job.name,
            "cron" => &periodic_job.cron,
        );

        Ok(())
    }

    /// Takes self to consume the processor. This is for life-cycle management, not
    /// memory safety because you can clone processor pretty easily.
    pub async fn run(self) {
        let cpu_count = num_cpus::get();

        // Start worker routines.
        for _ in 0..cpu_count {
            tokio::spawn({
                let mut processor = self.clone();
                let logger = self.logger.clone();

                async move {
                    loop {
                        if let Err(err) = processor.process_one().await {
                            error!(logger, "Error leaked out the bottom: {:?}", err);
                        }
                    }
                }
            });
        }

        // Start sidekiq-web metrics publisher.
        tokio::spawn({
            let logger = self.logger.clone();
            let redis = self.redis.clone();
            let queues = self.human_readable_queues.clone();
            let busy_jobs = self.busy_jobs.clone();
            async move {
                let hostname = if let Some(host) = gethostname::gethostname().to_str() {
                    host.to_string()
                } else {
                    "UNKNOWN_HOSTNAME".to_string()
                };

                let stats_publisher = StatsPublisher::new(hostname, queues, busy_jobs);

                loop {
                    // TODO: Use process count to meet a 5 second avg.
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

                    if let Err(err) = stats_publisher.publish_stats(redis.clone()).await {
                        error!(logger, "Error publishing processor stats: {:?}", err);
                    }
                }
            }
        });

        // Start retry and scheduled routines.
        tokio::spawn({
            let logger = self.logger.clone();
            let redis = self.redis.clone();
            async move {
                let sched = Scheduled::new(redis, logger.clone());
                let sorted_sets = vec!["retry".to_string(), "schedule".to_string()];

                loop {
                    // TODO: Use process count to meet a 5 second avg.
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

                    if let Err(err) = sched.enqueue_jobs(chrono::Utc::now(), &sorted_sets).await {
                        error!(logger, "Error in scheduled poller routine: {:?}", err);
                    }
                }
            }
        });

        // Watch for periodic jobs and enqueue jobs.
        tokio::spawn({
            let logger = self.logger.clone();
            let redis = self.redis.clone();
            async move {
                let sched = Scheduled::new(redis, logger.clone());

                loop {
                    // TODO: Use process count to meet a 30 second avg.
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;

                    if let Err(err) = sched.enqueue_periodic_jobs(chrono::Utc::now()).await {
                        error!(logger, "Error in periodic job poller routine: {:?}", err);
                    }
                }
            }
        });

        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }

    pub async fn using<M>(&mut self, middleware: M)
    where
        M: ServerMiddleware + Send + Sync + 'static,
    {
        self.chain.using(Box::new(middleware)).await;
    }
}
