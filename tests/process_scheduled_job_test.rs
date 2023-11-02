#[cfg(test)]
mod test {
    use async_trait::async_trait;
    use bb8::Pool;
    use sidekiq::{Processor, RedisConnectionManager, RedisPool, Scheduled, WorkFetcher, Worker};
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

    async fn new_base_processor(queue: String) -> (Processor, RedisPool) {
        // Redis
        let manager = RedisConnectionManager::new("redis://127.0.0.1/").unwrap();
        let redis = Pool::builder().build(manager).await.unwrap();
        redis.flushall().await;

        // Sidekiq server
        let p = Processor::new(redis.clone(), vec![queue]);

        (p, redis)
    }

    #[tokio::test]
    async fn can_process_a_scheduled_job() {
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
        let (mut p, mut redis) = new_base_processor(queue.clone()).await;

        p.register(worker.clone());

        TestWorker::opts()
            .queue(queue)
            .perform_in(&mut redis, std::time::Duration::from_secs(10), ())
            .await
            .unwrap();

        assert_eq!(
            p.process_one_tick_once().await.unwrap(),
            WorkFetcher::NoWorkFound
        );

        let sched = Scheduled::new(redis.clone());
        let sorted_sets = vec!["retry".to_string(), "schedule".to_string()];
        let n = sched
            .enqueue_jobs(
                chrono::Utc::now() + chrono::Duration::seconds(11),
                &sorted_sets,
            )
            .await
            .unwrap();

        assert_eq!(n, 1);

        assert_eq!(p.process_one_tick_once().await.unwrap(), WorkFetcher::Done);

        assert!(*worker.did_process.lock().unwrap());
    }
}
