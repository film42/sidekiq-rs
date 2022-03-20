use crate::UnitOfWork;
use bb8_redis::{bb8::Pool, redis::AsyncCommands, RedisConnectionManager};
use slog::debug;

pub struct Scheduled {
    redis: Pool<RedisConnectionManager>,
    logger: slog::Logger,
}

impl Scheduled {
    pub fn new(redis: Pool<RedisConnectionManager>, logger: slog::Logger) -> Self {
        Self { redis, logger }
    }

    pub async fn enqueue_jobs(
        &self,
        now: chrono::DateTime<chrono::Utc>,
        sorted_sets: &Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for sorted_set in sorted_sets {
            let mut redis = self.redis.get().await?;

            let jobs: Vec<String> = redis
                .zrangebyscore_limit(&sorted_set, "-inf", now.timestamp(), 0, 100)
                .await?;

            for job in jobs {
                if redis.zrem(&sorted_set, job.clone()).await? {
                    let work = UnitOfWork::from_job_string(job)?;

                    debug!(self.logger, "Enqueueing job";
                        "class" => &work.job.class,
                        "queue" => &work.queue
                    );

                    work.enqueue_direct(&mut redis).await?;
                }
            }
        }

        Ok(())
    }
}
