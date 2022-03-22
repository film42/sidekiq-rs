use crate::{Chain, Job, Scheduled, ServerMiddleware, UnitOfWork, Worker};
use bb8_redis::{bb8::Pool, redis::AsyncCommands, RedisConnectionManager};
use slog::{error, info};
use std::collections::BTreeMap;

#[derive(Clone)]
pub struct Processor {
    redis: Pool<RedisConnectionManager>,
    queues: Vec<String>,
    workers: BTreeMap<String, Box<dyn Worker>>,
    logger: slog::Logger,
    chain: Chain,
}

impl Processor {
    pub fn new(
        redis: Pool<RedisConnectionManager>,
        logger: slog::Logger,
        queues: Vec<String>,
    ) -> Self {
        Self {
            chain: Chain::new(logger.clone()),
            workers: BTreeMap::new(),

            redis,
            logger,
            queues: queues
                .iter()
                .map(|queue| format!("queue:{queue}"))
                .collect(),
        }
    }

    async fn fetch(&mut self) -> Result<UnitOfWork, Box<dyn std::error::Error>> {
        let (queue, job_raw): (String, String) =
            self.redis.get().await?.brpop(&self.queues, 0).await?;
        let job: Job = serde_json::from_str(&job_raw)?;
        // println!("{:?}", (&queue, &args));
        Ok(UnitOfWork { queue, job })
    }

    pub async fn process_one(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut work = self.fetch().await?;

        info!(self.logger, "sidekiq";
            "status" => "start",
            "class" => &work.job.class,
            "queue" => &work.job.queue,
            "jid" => &work.job.jid
        );

        if let Some(worker) = self.workers.get_mut(&work.job.class) {
            self.chain
                .call(work.job.clone(), worker.clone(), self.redis.clone())
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
            work.reenqueue(&mut self.redis).await?;
        }

        // TODO: Make this only say "done" when the job is successful.
        // We might need to change the ChainIter to return the final job and
        // detect any retries?
        info!(self.logger, "sidekiq";
            "status" => "done",
            "class" => &work.job.class,
            "queue" => &work.job.queue,
            "jid" => &work.job.jid,
        );

        Ok(())
    }

    pub fn register<W: Worker + 'static>(&mut self, worker: W) {
        self.workers.insert(W::class_name(), Box::new(worker));
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

        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await
        }
    }

    pub async fn using<M>(&mut self, middleware: M)
    where
        M: ServerMiddleware + Send + Sync + 'static,
    {
        self.chain.using(Box::new(middleware)).await
    }
}
