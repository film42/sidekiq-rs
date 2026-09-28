#[cfg(test)]
mod test {
    use async_trait::async_trait;
    use bb8::Pool;
    use serial_test::serial;
    use sidekiq::{Processor, ProcessorConfig, RedisConnectionManager, RedisPool, Result, Worker};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    async fn new_pool() -> RedisPool {
        let manager = RedisConnectionManager::new("redis://127.0.0.1/").unwrap();
        Pool::builder().build(manager).await.unwrap()
    }

    async fn flushall(redis: &RedisPool) {
        let mut conn = redis.get().await.unwrap();
        let _: String = redis::cmd("FLUSHALL")
            .query_async(conn.unnamespaced_borrow_mut())
            .await
            .unwrap();
    }

    async fn scard(redis: &RedisPool, key: &str) -> i64 {
        let mut conn = redis.get().await.unwrap();
        redis::cmd("SCARD")
            .arg(key)
            .query_async(conn.unnamespaced_borrow_mut())
            .await
            .unwrap_or(0)
    }

    async fn process_quiet(redis: &RedisPool) -> Option<String> {
        let mut conn = redis.get().await.unwrap();
        let members: Vec<String> = redis::cmd("SMEMBERS")
            .arg("processes")
            .query_async(conn.unnamespaced_borrow_mut())
            .await
            .unwrap_or_default();
        let identity = members.into_iter().next()?;
        redis::cmd("HGET")
            .arg(&identity)
            .arg("quiet")
            .query_async(conn.unnamespaced_borrow_mut())
            .await
            .ok()
    }

    /// Graceful cancellation must remove the process from the `processes` set.
    ///
    /// Waits for the 5-second stats heartbeat to fire naturally, then cancels and
    /// verifies the set is empty. This is an integration test and intentionally slow.
    #[tokio::test]
    #[serial]
    async fn graceful_shutdown_removes_process_from_processes_set() {
        let redis = new_pool().await;
        flushall(&redis).await;

        let p = Processor::new(redis.clone(), vec!["default".to_string()])
            .with_config(ProcessorConfig::default().num_workers(1));
        let token = p.get_cancellation_token();
        let handle = tokio::spawn(p.run());

        // The stats loop publishes every 5 s; wait for the first heartbeat.
        tokio::time::sleep(Duration::from_secs(6)).await;

        assert_eq!(
            scard(&redis, "processes").await,
            1,
            "process should be registered in set after first heartbeat"
        );

        token.cancel();
        handle.await.unwrap();

        assert_eq!(
            scard(&redis, "processes").await,
            0,
            "processes set must be empty after graceful shutdown"
        );
    }

    /// Cancellation before the first heartbeat fires must leave the set empty.
    /// deregister() is a no-op when nothing was published (SREM on a missing member is safe).
    #[tokio::test]
    #[serial]
    async fn early_shutdown_leaves_processes_set_empty() {
        let redis = new_pool().await;
        flushall(&redis).await;

        let p = Processor::new(redis.clone(), vec!["default".to_string()]);
        let token = p.get_cancellation_token();
        let handle = tokio::spawn(p.run());

        token.cancel();
        handle.await.unwrap();

        assert_eq!(
            scard(&redis, "processes").await,
            0,
            "processes set must be empty when cancelled before first heartbeat"
        );
    }

    #[derive(Clone)]
    struct SlowDrainWorker {
        release: Arc<AtomicBool>,
        running: Arc<AtomicBool>,
    }

    #[async_trait]
    impl Worker<()> for SlowDrainWorker {
        async fn perform(&self, _args: ()) -> Result<()> {
            self.running.store(true, Ordering::Relaxed);
            while !self.release.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            self.running.store(false, Ordering::Relaxed);
            Ok(())
        }
    }

    /// While draining an in-flight job, the process must stay in `processes` with
    /// `quiet: true` — matching Ruby Sidekiq, which only calls `clear_heartbeat`
    /// after `Manager#stop` returns.
    #[tokio::test]
    #[serial]
    async fn quiet_heartbeat_stays_registered_while_draining() {
        let redis = new_pool().await;
        flushall(&redis).await;

        let release = Arc::new(AtomicBool::new(false));
        let running = Arc::new(AtomicBool::new(false));

        let mut p = Processor::new(redis.clone(), vec!["drain_q".to_string()])
            .with_config(ProcessorConfig::default().num_workers(1));
        p.register(SlowDrainWorker {
            release: release.clone(),
            running: running.clone(),
        });

        SlowDrainWorker::opts()
            .queue("drain_q".to_string())
            .perform_async(&redis, ())
            .await
            .unwrap();

        {
            let mut conn = redis.get().await.unwrap();
            let len: i64 = redis::cmd("LLEN")
                .arg("queue:drain_q")
                .query_async(conn.unnamespaced_borrow_mut())
                .await
                .unwrap();
            assert_eq!(len, 1, "job should be waiting on drain_q");
        }

        let token = p.get_cancellation_token();
        let handle = tokio::spawn(p.run());

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !running.load(Ordering::Relaxed) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "slow drain job should start within 10s"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        token.cancel();

        // Give the stats task a moment to observe cancel and publish quiet.
        let quiet_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            if process_quiet(&redis).await.as_deref() == Some("true")
                && scard(&redis, "processes").await == 1
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < quiet_deadline,
                "expected quiet heartbeat while draining"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert!(
            running.load(Ordering::Relaxed),
            "in-flight job should still be running"
        );

        release.store(true, Ordering::Relaxed);
        handle.await.unwrap();

        assert_eq!(
            scard(&redis, "processes").await,
            0,
            "process must deregister only after drain completes"
        );
    }

    /// A scheduler-only process (`num_workers(0)`, no dedicated queues) must still
    /// return from `run()` on cancel. With no LiveWorkerGuards, drain must be
    /// signaled up front or the stats task hangs forever after quiet.
    #[tokio::test]
    #[serial]
    async fn zero_worker_config_shuts_down_without_hanging() {
        let redis = new_pool().await;
        flushall(&redis).await;

        let p = Processor::new(redis.clone(), vec!["default".to_string()])
            .with_config(ProcessorConfig::default().num_workers(0));
        let token = p.get_cancellation_token();
        let handle = tokio::spawn(p.run());

        // Allow at least one stats heartbeat so we exercise the post-quiet path.
        tokio::time::sleep(Duration::from_secs(6)).await;
        assert_eq!(
            scard(&redis, "processes").await,
            1,
            "zero-worker process should still heartbeat"
        );

        token.cancel();
        tokio::time::timeout(Duration::from_secs(3), handle)
            .await
            .expect("run() must return promptly with zero workers")
            .unwrap();

        assert_eq!(
            scard(&redis, "processes").await,
            0,
            "zero-worker process must deregister on shutdown"
        );
    }
}
