#[cfg(test)]
mod test {
    use async_trait::async_trait;
    use bb8::Pool;
    use serial_test::serial;
    use sidekiq::{
        BalanceStrategy, Processor, ProcessorConfig, QueueConfig, RedisConnectionManager,
        RedisPool, Result, WorkFetcher, Worker,
    };
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    };
    use std::time::Duration;

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

    async fn llen(redis: &RedisPool, key: &str) -> i64 {
        let mut conn = redis.get().await.unwrap();
        redis::cmd("LLEN")
            .arg(key)
            .query_async(conn.unnamespaced_borrow_mut())
            .await
            .unwrap_or(0)
    }

    async fn new_base_processor(queue: String) -> (Processor, RedisPool) {
        // Redis
        let manager = RedisConnectionManager::new("redis://127.0.0.1/").unwrap();
        let redis = Pool::builder().build(manager).await.unwrap();
        redis.flushall().await;

        // Sidekiq server
        let p = Processor::new(redis.clone(), vec![queue]).with_config(
            ProcessorConfig::default()
                .num_workers(1)
                .balance_strategy(BalanceStrategy::RoundRobin)
                .queue_config(
                    "dedicated queue 1".to_string(),
                    QueueConfig::default().num_workers(10),
                )
                .queue_config(
                    "dedicated queue 2".to_string(),
                    QueueConfig::default().num_workers(100),
                ),
        );

        (p, redis)
    }

    #[tokio::test]
    async fn can_process_an_async_job() {
        #[derive(Clone)]
        struct TestWorker {
            did_process: Arc<Mutex<bool>>,
        }

        #[async_trait]
        impl Worker<()> for TestWorker {
            async fn perform(&self, _args: ()) -> Result<()> {
                let mut this = self.did_process.lock().unwrap();
                *this = true;

                Ok(())
            }
        }

        let worker = TestWorker {
            did_process: Arc::new(Mutex::new(false)),
        };
        let queue = "random123".to_string();
        let (mut p, redis) = new_base_processor(queue.clone()).await;

        p.register(worker.clone());

        TestWorker::opts()
            .queue(queue)
            .perform_async(&redis, ())
            .await
            .unwrap();

        assert_eq!(p.process_one_tick_once().await.unwrap(), WorkFetcher::Done);
        assert!(*worker.did_process.lock().unwrap());
    }

    /// Jobs on a `queue_config` queue are fetched and processed by dedicated
    /// workers (`run()`'s dedicated path). Shared workers poll a different queue
    /// and cannot steal the job — so a successful process implies the dedicated
    /// worker picked it up (including the `queue:` Redis key prefix).
    #[serial]
    #[tokio::test]
    async fn dedicated_workers_pick_up_and_process_jobs() {
        #[derive(Clone)]
        struct DedicatedWorker {
            did_process: Arc<AtomicBool>,
        }

        #[async_trait]
        impl Worker<()> for DedicatedWorker {
            async fn perform(&self, _args: ()) -> Result<()> {
                self.did_process.store(true, Ordering::SeqCst);
                Ok(())
            }
        }

        let manager = RedisConnectionManager::new("redis://127.0.0.1/").unwrap();
        let redis = Pool::builder().build(manager).await.unwrap();
        redis.flushall().await;

        let shared_queues = vec!["shared_queue_1".to_string(), "shared_queue_2".to_string()];
        let dedicated_queue = "dedicated_queue".to_string();
        let did_process = Arc::new(AtomicBool::new(false));

        // Shared workers are running but only poll `shared_queues`. The job is
        // enqueued on `dedicated_queue`, which only the dedicated worker fetches.
        let mut p = Processor::new(redis.clone(), shared_queues).with_config(
            ProcessorConfig::default()
                .num_workers(2)
                .queue_config(
                    dedicated_queue.clone(),
                    QueueConfig::default().num_workers(1),
                ),
        );
        p.register(DedicatedWorker {
            did_process: did_process.clone(),
        });

        DedicatedWorker::opts()
            .queue(dedicated_queue.clone())
            .perform_async(&redis, ())
            .await
            .unwrap();

        assert_eq!(
            llen(&redis, &format!("queue:{dedicated_queue}")).await,
            1,
            "job should be waiting on the dedicated queue"
        );

        let token = p.get_cancellation_token();
        let handle = tokio::spawn(p.run());

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !did_process.load(Ordering::SeqCst) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "dedicated worker should pick up and process the job within 5s"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert!(
            did_process.load(Ordering::SeqCst),
            "dedicated worker must process the job"
        );
        assert_eq!(
            llen(&redis, &format!("queue:{dedicated_queue}")).await,
            0,
            "dedicated worker must drain the dedicated queue"
        );

        token.cancel();
        handle.await.unwrap().unwrap();
    }
}
