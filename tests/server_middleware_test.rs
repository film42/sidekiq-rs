#[cfg(test)]
mod test {
    use async_trait::async_trait;
    use bb8::Pool;
    use sidekiq::{
        ChainIter, Job, Processor, RedisConnectionManager, RedisPool, ServerMiddleware,
        ServerResult, WorkFetcher, Worker, WorkerRef,
    };
    use slog::{o, Drain};
    use std::sync::{Arc, Mutex};

    #[async_trait]
    trait FlushAll {
        async fn flushall(&self);
    }

    #[async_trait]
    impl FlushAll for RedisPool {
        async fn flushall(&self) {
            let mut conn = self.get().await.unwrap();
            let _: String = redis::cmd("FLUSHALL")
                .query_async(conn.unnamespaced_borrow_mut())
                .await
                .unwrap();
        }
    }

    async fn new_base_processor(queue: String) -> (Processor, RedisPool, slog::Logger) {
        // Logger
        let decorator = slog_term::PlainSyncDecorator::new(std::io::stdout());
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let logger = slog::Logger::root(drain, o!());

        // Redis
        let manager = RedisConnectionManager::new("redis://127.0.0.1/").unwrap();
        let redis = Pool::builder().build(manager).await.unwrap();
        redis.flushall().await;

        // Sidekiq server
        let p = Processor::new(redis.clone(), logger.clone(), vec![queue]);

        (p, redis, logger)
    }

    #[derive(Clone)]
    struct TestWorker {
        did_process: Arc<Mutex<bool>>,
    }

    #[async_trait]
    impl Worker<()> for TestWorker {
        async fn perform(&self, _args: ()) -> Result<(), Box<dyn std::error::Error>> {
            let mut this = self.did_process.lock().unwrap();
            *this = true;

            Ok(())
        }
    }

    #[derive(Clone)]
    struct TestMiddleware {
        should_halt: bool,
        did_process: Arc<Mutex<bool>>,
    }

    #[async_trait]
    impl ServerMiddleware for TestMiddleware {
        async fn call(
            &self,
            chain: ChainIter,
            job: &Job,
            worker: Arc<WorkerRef>,
            redis: RedisPool,
        ) -> ServerResult {
            {
                let mut this = self.did_process.lock().unwrap();
                *this = true;
            }

            if self.should_halt {
                return Ok(());
            } else {
                return Ok(chain.next(job, worker, redis).await?);
            }
        }
    }

    #[tokio::test]
    async fn can_process_job_with_middleware() {
        let worker = TestWorker {
            did_process: Arc::new(Mutex::new(false)),
        };
        let queue = "random123".to_string();
        let (mut p, mut redis, _) = new_base_processor(queue.clone()).await;

        let middleware = TestMiddleware {
            should_halt: false,
            did_process: Arc::new(Mutex::new(false)),
        };

        p.register(worker.clone());
        p.using(middleware.clone()).await;

        TestWorker::opts()
            .queue(queue)
            .perform_async(&mut redis, ())
            .await
            .unwrap();

        assert_eq!(p.process_one_tick_once().await.unwrap(), WorkFetcher::Done);
        assert!(*worker.did_process.lock().unwrap());
        assert!(*middleware.did_process.lock().unwrap());
    }

    #[tokio::test]
    async fn can_prevent_job_from_being_processed_with_halting_middleware() {
        let worker = TestWorker {
            did_process: Arc::new(Mutex::new(false)),
        };
        let queue = "random123".to_string();
        let (mut p, mut redis, _) = new_base_processor(queue.clone()).await;

        let middleware = TestMiddleware {
            should_halt: true,
            did_process: Arc::new(Mutex::new(false)),
        };

        p.register(worker.clone());
        p.using(middleware.clone()).await;

        TestWorker::opts()
            .queue(queue)
            .perform_async(&mut redis, ())
            .await
            .unwrap();

        assert_eq!(p.process_one_tick_once().await.unwrap(), WorkFetcher::Done);
        assert!(!*worker.did_process.lock().unwrap());
        assert!(*middleware.did_process.lock().unwrap());
    }
}
