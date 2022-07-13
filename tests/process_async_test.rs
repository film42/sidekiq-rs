#[cfg(test)]
mod test {
    use async_trait::async_trait;
    use bb8_redis::{bb8::Pool, RedisConnectionManager};
    use sidekiq::{Processor, WorkFetcher, Worker};
    use slog::{o, Drain};
    use std::sync::{Arc, Mutex};

    #[async_trait]
    trait FlushAll {
        async fn flushall(&self);
    }

    #[async_trait]
    impl FlushAll for Pool<RedisConnectionManager> {
        async fn flushall(&self) {
            let mut conn = self.get().await.unwrap();
            let _: String = redis::cmd("FLUSHALL")
                .query_async(&mut *conn)
                .await
                .unwrap();
        }
    }

    async fn new_base_processor(
        queue: String,
    ) -> (Processor, Pool<RedisConnectionManager>, slog::Logger) {
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

    #[tokio::test]
    async fn can_process_an_async_job() {
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

        let worker = TestWorker {
            did_process: Arc::new(Mutex::new(false)),
        };
        let queue = "random123".to_string();
        let (mut p, mut redis, _) = new_base_processor(queue.clone()).await;

        p.register(worker.clone());

        TestWorker::opts()
            .queue(queue)
            .perform_async(&mut redis, ())
            .await
            .unwrap();

        assert_eq!(p.process_one_tick_once().await.unwrap(), WorkFetcher::Done);
        assert!(*worker.did_process.lock().unwrap());
    }
}
