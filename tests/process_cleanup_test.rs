#[cfg(test)]
mod test {
    use bb8::Pool;
    use serial_test::serial;
    use sidekiq::{Processor, ProcessorConfig, RedisConnectionManager, RedisPool};
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

    /// Graceful cancellation must remove the process from the `processes` set.
    ///
    /// Waits for the 5-second stats heartbeat to fire naturally, then cancels and
    /// verifies the set is empty. This is an integration test and intentionally slow.
    #[serial]
    #[tokio::test]
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
        handle.await.unwrap().unwrap();

        assert_eq!(
            scard(&redis, "processes").await,
            0,
            "processes set must be empty after graceful shutdown"
        );
    }

    /// Cancellation before the first heartbeat fires must leave the set empty.
    /// deregister() is a no-op when nothing was published (SREM on a missing member is safe).
    #[serial]
    #[tokio::test]
    async fn early_shutdown_leaves_processes_set_empty() {
        let redis = new_pool().await;
        flushall(&redis).await;

        let p = Processor::new(redis.clone(), vec!["default".to_string()]);
        let token = p.get_cancellation_token();
        let handle = tokio::spawn(p.run());

        token.cancel();
        handle.await.unwrap().unwrap();

        assert_eq!(
            scard(&redis, "processes").await,
            0,
            "processes set must be empty when cancelled before first heartbeat"
        );
    }

    /// A panic in one worker must cancel siblings so `run()` can drain and return.
    ///
    /// Proof of graceful shutdown:
    /// - a long-running job is in flight on one worker when the other panics
    ///   (`PanicWorker` records that `SlowWorker` had not finished yet)
    /// - `run()` waits for that in-flight job (`slow_finished` before return)
    /// - the stats task deregisters (`processes` empty) after the panic
    ///
    /// Uses a multi-thread runtime so the intentional panic is isolated to a
    /// JoinSet worker (caught as `JoinError`) instead of unwinding on the test
    /// thread. The panic hook is silenced only for our intentional message so
    /// cargo/IDE output is not mistaken for a failed test.
    #[serial]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_shuts_down_siblings_gracefully_when_worker_panics() {
        use async_trait::async_trait;
        use sidekiq::{Result, Worker};
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        #[derive(Clone)]
        struct SlowWorker {
            started: Arc<AtomicBool>,
            finished: Arc<AtomicBool>,
        }

        #[async_trait]
        impl Worker<()> for SlowWorker {
            async fn perform(&self, _args: ()) -> Result<()> {
                self.started.store(true, Ordering::SeqCst);
                // Longer than a typical BRPOP tick so this stays in-flight when
                // the sibling panics; worker loops only check cancel after
                // process_one returns.
                tokio::time::sleep(Duration::from_secs(3)).await;
                self.finished.store(true, Ordering::SeqCst);
                Ok(())
            }
        }

        #[derive(Clone)]
        struct PanicWorker {
            slow_started: Arc<AtomicBool>,
            slow_finished: Arc<AtomicBool>,
            saw_slow_in_flight: Arc<AtomicBool>,
        }

        #[async_trait]
        impl Worker<()> for PanicWorker {
            async fn perform(&self, _args: ()) -> Result<()> {
                // Proof the panic overlapped the slow job.
                self.saw_slow_in_flight.store(
                    self.slow_started.load(Ordering::SeqCst) &&
                    !self.slow_finished.load(Ordering::SeqCst),
                    Ordering::SeqCst,
                );
                panic!("intentional test panic");
            }
        }

        let redis = new_pool().await;
        flushall(&redis).await;

        let slow_started = Arc::new(AtomicBool::new(false));
        let slow_finished = Arc::new(AtomicBool::new(false));
        let saw_slow_in_flight = Arc::new(AtomicBool::new(false));

        let mut p = Processor::new(redis.clone(), vec!["default".to_string()]).with_config(
            ProcessorConfig::default()
                .num_workers(2)
                .enable_scheduled(false)
                .enable_periodic(false)
                .enable_stats(true),
        );
        p.register(SlowWorker {
            started: slow_started.clone(),
            finished: slow_finished.clone(),
        });
        p.register(PanicWorker {
            slow_started: slow_started.clone(),
            slow_finished: slow_finished.clone(),
            saw_slow_in_flight: saw_slow_in_flight.clone(),
        });
        let token = p.get_cancellation_token();
        let handle = tokio::spawn(p.run());

        // Wait for the stats heartbeat so we can observe deregister on cancel.
        tokio::time::sleep(Duration::from_secs(6)).await;
        assert_eq!(
            scard(&redis, "processes").await,
            1,
            "process should be registered before the panic"
        );
        assert!(
            !token.is_cancelled(),
            "cancellation token should be idle before the panic"
        );

        SlowWorker::perform_async(&redis, ())
            .await
            .expect("enqueue slow job");

        // Ensure the slow job is actually running on one worker before we panic
        // the other, otherwise both workers may be idle when PanicWorker runs.
        let wait_started = tokio::time::Instant::now();
        while !slow_started.load(Ordering::SeqCst) {
            assert!(
                wait_started.elapsed() < Duration::from_secs(1),
                "slow job should start before panic is enqueued"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            !slow_finished.load(Ordering::SeqCst),
            "slow job must still be in flight when we enqueue the panic"
        );

        let _panic_hook_guard = {
            struct Guard(
                Option<Box<dyn Fn(&std::panic::PanicHookInfo<'_>) + Send + Sync + 'static>>,
            );
            impl Drop for Guard {
                fn drop(&mut self) {
                    if let Some(prev) = self.0.take() {
                        std::panic::set_hook(prev);
                    }
                }
            }

            let prev = std::panic::take_hook();
            std::panic::set_hook(Box::new(|info| {
                if info.to_string().contains("intentional test panic") {
                    return;
                }
                eprintln!("{info}");
            }));
            Guard(Some(prev))
        };

        PanicWorker::perform_async(&redis, ())
            .await
            .expect("enqueue panic job");

        let join = tokio::time::timeout(Duration::from_secs(15), handle)
            .await
            .expect("sibling tasks should finish after cancel, not hang");

        let err = join
            .expect("run task should not panic")
            .expect_err("run should fail on worker panic");

        drop(_panic_hook_guard);

        assert!(
            saw_slow_in_flight.load(Ordering::SeqCst),
            "panic must occur while the slow job is still in flight"
        );
        assert!(
            slow_finished.load(Ordering::SeqCst),
            "run() must await in-flight work to completion before returning"
        );
        assert!(
            err.to_string().contains("panicked"),
            "expected panic error, got {err}"
        );
        assert!(token.is_cancelled(), "panic must cancel remaining tasks");
        assert_eq!(
            scard(&redis, "processes").await,
            0,
            "stats task must deregister after sibling panic"
        );
    }
}
