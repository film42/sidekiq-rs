#[cfg(test)]
mod test {
    use async_trait::async_trait;
    use bb8::Pool;
    use sidekiq::{
        periodic, Processor, RedisConnectionManager, RedisPool, Scheduled, WorkFetcher, Worker,
    };
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

    async fn set_cron_scores_to_zero(redis: RedisPool) {
        let mut conn = redis.get().await.unwrap();

        let jobs = conn
            .zrange("periodic".to_string(), isize::MIN, isize::MAX)
            .await
            .unwrap();

        for job in jobs {
            let _: usize = conn
                .zadd("periodic".to_string(), job.clone(), 0)
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn can_process_a_cron_job() {
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
        let (mut p, redis) = new_base_processor(queue.clone()).await;

        p.register(worker.clone());

        // Cron jobs
        periodic::builder("0 * * * * *")
            .unwrap()
            .name("Payment report processing for a user using json args")
            .queue(queue.clone())
            .register(&mut p, worker.clone())
            .await
            .unwrap();

        assert_eq!(
            p.process_one_tick_once().await.unwrap(),
            WorkFetcher::NoWorkFound
        );

        set_cron_scores_to_zero(redis.clone()).await;

        let sched = Scheduled::new(redis.clone());
        let n = sched
            .enqueue_periodic_jobs(chrono::Utc::now())
            .await
            .unwrap();

        assert_eq!(n, 1);

        assert_eq!(p.process_one_tick_once().await.unwrap(), WorkFetcher::Done);

        assert!(*worker.did_process.lock().unwrap());
    }
}
