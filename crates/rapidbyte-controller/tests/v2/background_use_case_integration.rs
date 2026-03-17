use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use rapidbyte_controller::adapters::background::lease_sweep::run_timeout_tick;
use rapidbyte_controller::adapters::background::preview_cleanup::run_preview_cleanup_tick;
use rapidbyte_controller::adapters::background::reaper::run_reconcile_tick;
use rapidbyte_controller::app::cleanup_preview::PreviewCleanupExecutor;
use rapidbyte_controller::app::reconcile_restart::ReconcileRestartExecutor;
use rapidbyte_controller::app::timeout_attempt::TimeoutAttemptExecutor;

struct CountedExecutor {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl TimeoutAttemptExecutor for CountedExecutor {
    async fn execute_timeout_tick(&self) -> anyhow::Result<()> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[async_trait]
impl ReconcileRestartExecutor for CountedExecutor {
    async fn execute_reconcile_tick(&self) -> anyhow::Result<()> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[async_trait]
impl PreviewCleanupExecutor for CountedExecutor {
    async fn execute_preview_cleanup_tick(&self) -> anyhow::Result<()> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::test]
async fn background_ticks_call_use_case_executors() {
    let timeout_calls = Arc::new(AtomicUsize::new(0));
    let reconcile_calls = Arc::new(AtomicUsize::new(0));
    let cleanup_calls = Arc::new(AtomicUsize::new(0));

    run_timeout_tick(&CountedExecutor {
        calls: timeout_calls.clone(),
    })
    .await
    .expect("timeout tick should succeed");
    run_reconcile_tick(&CountedExecutor {
        calls: reconcile_calls.clone(),
    })
    .await
    .expect("reconcile tick should succeed");
    run_preview_cleanup_tick(&CountedExecutor {
        calls: cleanup_calls.clone(),
    })
    .await
    .expect("cleanup tick should succeed");

    assert_eq!(timeout_calls.load(Ordering::Relaxed), 1);
    assert_eq!(reconcile_calls.load(Ordering::Relaxed), 1);
    assert_eq!(cleanup_calls.load(Ordering::Relaxed), 1);
}
