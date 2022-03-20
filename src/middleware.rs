use crate::{Job, UnitOfWork, Worker};
use async_trait::async_trait;
use bb8_redis::{bb8::Pool, RedisConnectionManager};
use slog::error;
use std::sync::Arc;
use tokio::sync::RwLock;

pub type ServerResult = Result<(), Box<dyn std::error::Error>>;

#[async_trait]
pub trait ServerMiddleware {
    async fn call(
        &self,
        iter: ChainIter,
        job: Job,
        worker: Box<dyn Worker>,
        redis: Pool<RedisConnectionManager>,
    ) -> ServerResult;
}

/// A pseudo iterator used to know which middleware should be called next.
/// This is created by the Chain type.
#[derive(Clone)]
pub struct ChainIter {
    stack: Arc<RwLock<Vec<Box<dyn ServerMiddleware + Send + Sync>>>>,
    index: usize,
}

impl ChainIter {
    pub async fn next(
        &self,
        job: Job,
        worker: Box<dyn Worker>,
        redis: Pool<RedisConnectionManager>,
    ) -> ServerResult {
        let stack = self.stack.read().await;

        if let Some(ref middleware) = stack.get(self.index) {
            middleware
                .call(
                    ChainIter {
                        stack: self.stack.clone(),
                        index: self.index + 1,
                    },
                    job.clone(),
                    worker.clone(),
                    redis,
                )
                .await?;
        }

        Ok(())
    }
}

/// A chain of middlewares that will be called in order by different server middlewares.
#[derive(Clone)]
pub(crate) struct Chain {
    stack: Arc<RwLock<Vec<Box<dyn ServerMiddleware + Send + Sync>>>>,
}

impl Chain {
    pub(crate) fn new(logger: slog::Logger) -> Self {
        Self {
            stack: Arc::new(RwLock::new(vec![
                Box::new(RetryMiddleware::new(logger)),
                Box::new(HandlerMiddleware),
            ])),
        }
    }

    pub(crate) async fn using(&mut self, middleware: Box<dyn ServerMiddleware + Send + Sync>) {
        let mut stack = self.stack.write().await;
        // HACK: Insert after retry middleware but before the handler middleware.
        let index = stack.len() - 1;
        stack.insert(index, middleware);
    }

    pub(crate) fn iter(&self) -> ChainIter {
        ChainIter {
            stack: self.stack.clone(),
            index: 0,
        }
    }

    pub(crate) async fn call(
        &mut self,
        job: Job,
        worker: Box<dyn Worker>,
        redis: Pool<RedisConnectionManager>,
    ) -> ServerResult {
        // The middleware must call bottom of the stack to the top.
        // Each middleware should receive a lambda to the next middleware
        // up the stack. Each middleware can short-circuit the stack by
        // not calling the "next" middleware.
        self.iter().next(job, worker, redis).await
    }
}

struct HandlerMiddleware;

#[async_trait]
impl ServerMiddleware for HandlerMiddleware {
    async fn call(
        &self,
        _chain: ChainIter,
        job: Job,
        worker: Box<dyn Worker>,
        _redis: Pool<RedisConnectionManager>,
    ) -> ServerResult {
        worker.perform(job.args).await
    }
}

struct RetryMiddleware {
    logger: slog::Logger,
}

impl RetryMiddleware {
    fn new(logger: slog::Logger) -> Self {
        Self { logger }
    }

    fn max_retries(&self) -> usize {
        // TODO: Make configurable at the worker level
        25
    }
}

#[async_trait]
impl ServerMiddleware for RetryMiddleware {
    async fn call(
        &self,
        chain: ChainIter,
        mut job: Job,
        worker: Box<dyn Worker>,
        mut redis: Pool<RedisConnectionManager>,
    ) -> ServerResult {
        let err = {
            match chain.next(job.clone(), worker, redis.clone()).await {
                Ok(()) => return Ok(()),
                Err(err) => format!("{err:?}"),
            }
        };

        // Update error fields on the job.
        job.error_message = Some(err);
        if job.retry_count.is_some() {
            job.retried_at = Some(chrono::Utc::now().timestamp() as f64);
        } else {
            job.failed_at = Some(chrono::Utc::now().timestamp() as f64);
        }
        let retry_count = job.retry_count.unwrap_or(0) + 1;
        job.retry_count = Some(retry_count);

        // Attempt the retry.
        if retry_count < self.max_retries() {
            error!(self.logger,
                "Scheduling job for retry in the future";
                "status" => "fail",
                "class" => &job.class,
                "jid" => &job.jid,
                "queue" => &job.queue,
                "err" => &job.error_message,
            );

            UnitOfWork::from_job(job.clone())
                .reenqueue(&mut redis)
                .await?;
        }

        Ok(())
    }
}
