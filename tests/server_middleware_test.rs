#[cfg(test)]
mod test {
    use async_trait::async_trait;
    use bb8::Pool;
    use serial_test::serial;
    use sidekiq::{
        ChainIter, Error, Job, Processor, RedisConnectionManager, RedisPool, Result, RetryOpts,
        Scheduled, ServerMiddleware, UnitOfWork, WorkFetcher, Worker, WorkerRef,
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
                .arg("SYNC")
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

    #[derive(Clone)]
    struct AlwaysFailWorker;

    #[async_trait]
    impl Worker<()> for AlwaysFailWorker {
        async fn perform(&self, _args: ()) -> Result<()> {
            Err(Error::Message("big ouchie".to_string()))
        }
    }

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
        ) -> Result<()> {
            {
                let mut this = self.did_process.lock().unwrap();
                *this = true;
            }

            if self.should_halt {
                return Ok(());
            } else {
                return chain.next(job, worker, redis).await;
            }
        }
    }

    #[tokio::test]
    #[serial]
    async fn can_process_job_with_middleware() {
        let worker = TestWorker {
            did_process: Arc::new(Mutex::new(false)),
        };
        let queue = "random123".to_string();
        let (mut p, redis) = new_base_processor(queue.clone()).await;

        let middleware = TestMiddleware {
            should_halt: false,
            did_process: Arc::new(Mutex::new(false)),
        };

        p.register(worker.clone());
        p.using(middleware.clone()).await;

        TestWorker::opts()
            .queue(queue)
            .perform_async(&redis, ())
            .await
            .unwrap();

        assert_eq!(p.process_one_tick_once().await.unwrap(), WorkFetcher::Done);
        assert!(*worker.did_process.lock().unwrap());
        assert!(*middleware.did_process.lock().unwrap());
    }

    #[tokio::test]
    #[serial]
    async fn can_prevent_job_from_being_processed_with_halting_middleware() {
        let worker = TestWorker {
            did_process: Arc::new(Mutex::new(false)),
        };
        let queue = "random123".to_string();
        let (mut p, redis) = new_base_processor(queue.clone()).await;

        let middleware = TestMiddleware {
            should_halt: true,
            did_process: Arc::new(Mutex::new(false)),
        };

        p.register(worker.clone());
        p.using(middleware.clone()).await;

        TestWorker::opts()
            .queue(queue)
            .perform_async(&redis, ())
            .await
            .unwrap();

        assert_eq!(p.process_one_tick_once().await.unwrap(), WorkFetcher::Done);
        assert!(!*worker.did_process.lock().unwrap());
        assert!(*middleware.did_process.lock().unwrap());
    }

    #[tokio::test]
    #[serial]
    async fn can_retry_a_job() {
        let worker = AlwaysFailWorker;
        let queue = "failure_zone".to_string();
        let (mut p, redis) = new_base_processor(queue.clone()).await;
        p.register(worker.clone());

        AlwaysFailWorker::opts()
            .queue(queue)
            .retry(true)
            .perform_async(&redis, ())
            .await
            .unwrap();

        assert_eq!(p.process_one_tick_once().await.unwrap(), WorkFetcher::Done);
        let sets = vec!["retry".to_string()];
        let sched = Scheduled::new(redis.clone());
        let future_date = chrono::Utc::now() + chrono::Duration::days(30);

        // We should be able to reenqueue the job.
        let n_jobs_retried = sched.enqueue_jobs(future_date, &sets).await;
        assert!(n_jobs_retried.is_ok());
        let n_jobs_retried = n_jobs_retried.unwrap();
        assert_eq!(n_jobs_retried, 1, "one job in the retry queue");

        // Let's grab that job.
        let job = p.fetch().await;
        assert!(job.is_ok());
        let job = job.unwrap();
        assert!(job.is_some());
        let job = job.unwrap();

        assert_eq!(job.job.retry, RetryOpts::Yes);
        assert_eq!(job.job.retry_count, Some(1));
        assert_eq!(job.job.class, "AlwaysFailWorker");
    }

    #[tokio::test]
    #[serial]
    async fn can_retry_only_until_the_max_global_retries() {
        let worker = AlwaysFailWorker;
        let queue = "failure_zone_global".to_string();
        let (mut p, redis) = new_base_processor(queue.clone()).await;
        p.register(worker.clone());

        let mut job = AlwaysFailWorker::opts()
            .queue(queue)
            .retry(true)
            .into_opts()
            .create_job(AlwaysFailWorker::class_name(), ())
            .expect("never fails");

        // One last retry remaining.
        assert_eq!(worker.max_retries(), 25, "default is 25 retries");
        job.retry_count = Some(worker.max_retries());

        UnitOfWork::from_job(job)
            .enqueue(&redis)
            .await
            .expect("enqueues");

        assert_eq!(p.process_one_tick_once().await.unwrap(), WorkFetcher::Done);
        let sets = vec!["retry".to_string()];
        let sched = Scheduled::new(redis.clone());
        let future_date = chrono::Utc::now() + chrono::Duration::days(30);

        // We should have no jobs that need retrying.
        let n_jobs_retried = sched.enqueue_jobs(future_date, &sets).await;
        assert!(n_jobs_retried.is_ok());
        let n_jobs_retried = n_jobs_retried.unwrap();

        assert_eq!(n_jobs_retried, 0, "no jobs in the retry queue");
    }

    #[tokio::test]
    #[serial]
    async fn can_retry_based_on_job_opts_retries() {
        let worker = AlwaysFailWorker;
        let queue = "failure_zone_max_on_job".to_string();
        let (mut p, redis) = new_base_processor(queue.clone()).await;
        p.register(worker.clone());

        let mut job = AlwaysFailWorker::opts()
            .queue(queue)
            .retry(5)
            .into_opts()
            .create_job(AlwaysFailWorker::class_name(), ())
            .expect("never fails");

        // One last retry remaining from the retry(5) on the job params.
        assert_eq!(worker.max_retries(), 25, "default is 25 retries");
        job.retry_count = Some(5);

        UnitOfWork::from_job(job)
            .enqueue(&redis)
            .await
            .expect("enqueues");

        assert_eq!(p.process_one_tick_once().await.unwrap(), WorkFetcher::Done);
        let sets = vec!["retry".to_string()];
        let sched = Scheduled::new(redis.clone());
        let future_date = chrono::Utc::now() + chrono::Duration::days(30);

        // We should have no jobs that need retrying.
        let n_jobs_retried = sched.enqueue_jobs(future_date, &sets).await;
        assert!(n_jobs_retried.is_ok());
        let n_jobs_retried = n_jobs_retried.unwrap();

        assert_eq!(n_jobs_retried, 0, "no jobs in the retry queue");
    }

    #[tokio::test]
    #[serial]
    async fn can_set_retry_to_false_per_job() {
        let worker = AlwaysFailWorker;
        let queue = "failure_zone_never_retry_the_job".to_string();
        let (mut p, redis) = new_base_processor(queue.clone()).await;
        p.register(worker.clone());

        AlwaysFailWorker::opts()
            .queue(queue)
            .retry(false)
            .perform_async(&redis, ())
            .await
            .expect("never fails");

        // One last retry remaining from the retry(5) on the job params.
        assert_eq!(worker.max_retries(), 25, "default is 25 retries");

        assert_eq!(p.process_one_tick_once().await.unwrap(), WorkFetcher::Done);
        let sets = vec!["retry".to_string()];
        let sched = Scheduled::new(redis.clone());
        let future_date = chrono::Utc::now() + chrono::Duration::days(30);

        // We should have no jobs that need retrying.
        let n_jobs_retried = sched.enqueue_jobs(future_date, &sets).await;
        assert!(n_jobs_retried.is_ok());
        let n_jobs_retried = n_jobs_retried.unwrap();

        assert_eq!(n_jobs_retried, 0, "no jobs in the retry queue");
    }
}
