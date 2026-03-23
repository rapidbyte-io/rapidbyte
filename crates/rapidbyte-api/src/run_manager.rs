//! In-memory run tracking, broadcast channels for SSE, and cancellation tokens.
//!
//! [`RunManager`] is the single source of truth for in-flight and
//! recently completed pipeline runs. It is `Send + Sync` and backed
//! by a [`DashMap`] for lock-free concurrent access.

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use rapidbyte_engine::PipelineResult;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

use crate::types::{RunStatus, SseEvent};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Manages in-flight and recently completed pipeline runs.
///
/// Provides broadcast channels for SSE subscribers and cancellation
/// tokens for cooperative cancellation.
pub struct RunManager {
    runs: DashMap<String, RunState>,
}

/// Read-only snapshot of a run's state (no channels/tokens).
#[derive(Debug, Clone)]
pub struct RunSnapshot {
    pub run_id: String,
    pub pipeline: String,
    pub status: RunStatus,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub result: Option<PipelineResult>,
    pub error: Option<String>,
}

// ---------------------------------------------------------------------------
// Internal state
// ---------------------------------------------------------------------------

/// Full mutable state for a tracked run.
struct RunState {
    run_id: String,
    pipeline: String,
    status: RunStatus,
    started_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
    result: Option<PipelineResult>,
    error: Option<String>,
    event_tx: broadcast::Sender<SseEvent>,
    cancel: CancellationToken,
}

impl RunState {
    /// Create a snapshot that excludes broadcast/cancel internals.
    fn snapshot(&self) -> RunSnapshot {
        RunSnapshot {
            run_id: self.run_id.clone(),
            pipeline: self.pipeline.clone(),
            status: self.status,
            started_at: self.started_at,
            finished_at: self.finished_at,
            result: self.result.clone(),
            error: self.error.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// RunManager implementation
// ---------------------------------------------------------------------------

/// Broadcast channel capacity for SSE events per run.
const BROADCAST_CAPACITY: usize = 256;

impl RunManager {
    /// Create an empty `RunManager`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            runs: DashMap::new(),
        }
    }

    /// Register a new run for the given pipeline.
    ///
    /// Returns a `run_`-prefixed ULID string.
    #[must_use]
    pub fn create_run(&self, pipeline: String) -> String {
        let run_id = format!("run_{}", Ulid::new());
        let (event_tx, _rx) = broadcast::channel(BROADCAST_CAPACITY);
        let state = RunState {
            run_id: run_id.clone(),
            pipeline,
            status: RunStatus::Pending,
            started_at: Utc::now(),
            finished_at: None,
            result: None,
            error: None,
            event_tx,
            cancel: CancellationToken::new(),
        };
        self.runs.insert(run_id.clone(), state);
        run_id
    }

    /// Look up a run by ID and return a cloned snapshot.
    #[must_use]
    pub fn get(&self, run_id: &str) -> Option<RunSnapshot> {
        self.runs.get(run_id).map(|r| r.snapshot())
    }

    /// Update the status of an existing run.
    ///
    /// No-op if the run does not exist.
    pub fn update_status(&self, run_id: &str, status: RunStatus) {
        if let Some(mut entry) = self.runs.get_mut(run_id) {
            entry.status = status;
        }
    }

    /// Mark a run as completed, storing the pipeline result and finish time.
    pub fn complete(&self, run_id: &str, result: PipelineResult) {
        if let Some(mut entry) = self.runs.get_mut(run_id) {
            entry.status = RunStatus::Completed;
            entry.result = Some(result);
            entry.finished_at = Some(Utc::now());
        }
    }

    /// Mark a run as failed, storing the error message and finish time.
    pub fn fail(&self, run_id: &str, error: String) {
        if let Some(mut entry) = self.runs.get_mut(run_id) {
            entry.status = RunStatus::Failed;
            entry.error = Some(error);
            entry.finished_at = Some(Utc::now());
        }
    }

    /// Cancel a run by triggering its [`CancellationToken`].
    pub fn cancel(&self, run_id: &str) {
        if let Some(entry) = self.runs.get(run_id) {
            entry.cancel.cancel();
        }
    }

    /// Return a clone of the run's cancellation token.
    #[must_use]
    pub fn cancel_token(&self, run_id: &str) -> Option<CancellationToken> {
        self.runs.get(run_id).map(|r| r.cancel.clone())
    }

    /// Subscribe to SSE events for a run.
    ///
    /// Returns `None` if the run does not exist.
    #[must_use]
    pub fn subscribe(&self, run_id: &str) -> Option<broadcast::Receiver<SseEvent>> {
        self.runs.get(run_id).map(|r| r.event_tx.subscribe())
    }

    /// Broadcast an SSE event to all subscribers of the given run.
    ///
    /// Send errors (e.g. no active receivers) are silently ignored.
    pub fn send_event(&self, run_id: &str, event: SseEvent) {
        if let Some(entry) = self.runs.get(run_id) {
            let _ = entry.event_tx.send(event);
        }
    }

    /// Return snapshots for all tracked runs.
    #[must_use]
    pub fn list_runs(&self) -> Vec<RunSnapshot> {
        self.runs.iter().map(|r| r.snapshot()).collect()
    }

    /// Spawn a background task that evicts finished runs older than one hour.
    ///
    /// Runs every 5 minutes. The task stops when the [`RunManager`] is dropped
    /// (there is no explicit shutdown — the `DashMap` is `Arc`-free so the
    /// task simply holds a reference; callers should wrap `RunManager` in an
    /// `Arc` and pass a clone).
    pub fn start_eviction_task(self: &std::sync::Arc<Self>) {
        let mgr = std::sync::Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(300));
            loop {
                interval.tick().await;
                let cutoff = Utc::now() - chrono::Duration::hours(1);
                mgr.runs.retain(|_id, state| {
                    state.finished_at.is_none_or(|finished| finished > cutoff)
                });
            }
        });
    }
}

impl Default for RunManager {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_run_and_get() {
        let mgr = RunManager::new();
        let run_id = mgr.create_run("test-pipeline".into());
        assert!(run_id.starts_with("run_"));
        let state = mgr.get(&run_id).unwrap();
        assert_eq!(state.pipeline, "test-pipeline");
        assert_eq!(state.status, RunStatus::Pending);
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let mgr = RunManager::new();
        assert!(mgr.get("run_nonexistent").is_none());
    }

    #[test]
    fn update_status() {
        let mgr = RunManager::new();
        let run_id = mgr.create_run("p".into());
        mgr.update_status(&run_id, RunStatus::Running);
        assert_eq!(mgr.get(&run_id).unwrap().status, RunStatus::Running);
    }

    #[test]
    fn complete_sets_result_and_finished() {
        let mgr = RunManager::new();
        let run_id = mgr.create_run("p".into());
        let result = PipelineResult {
            counts: rapidbyte_engine::PipelineCounts::default(),
            source: rapidbyte_engine::SourceTiming::default(),
            dest: rapidbyte_engine::DestTiming::default(),
            num_transforms: 0,
            total_transform_secs: 0.0,
            transform_load_times_ms: vec![],
            duration_secs: 1.5,
            wasm_overhead_secs: 0.1,
            retry_count: 0,
            parallelism: 1,
            stream_metrics: vec![],
        };
        mgr.complete(&run_id, result);
        let snap = mgr.get(&run_id).unwrap();
        assert_eq!(snap.status, RunStatus::Completed);
        assert!(snap.finished_at.is_some());
        assert!(snap.result.is_some());
    }

    #[test]
    fn fail_sets_error_and_finished() {
        let mgr = RunManager::new();
        let run_id = mgr.create_run("p".into());
        mgr.fail(&run_id, "boom".into());
        let snap = mgr.get(&run_id).unwrap();
        assert_eq!(snap.status, RunStatus::Failed);
        assert!(snap.finished_at.is_some());
        assert_eq!(snap.error.as_deref(), Some("boom"));
    }

    #[test]
    fn cancel_sets_token() {
        let mgr = RunManager::new();
        let run_id = mgr.create_run("p".into());
        let token = mgr.cancel_token(&run_id).unwrap();
        assert!(!token.is_cancelled());
        mgr.cancel(&run_id);
        assert!(token.is_cancelled());
    }

    #[test]
    fn cancel_token_nonexistent_returns_none() {
        let mgr = RunManager::new();
        assert!(mgr.cancel_token("run_nope").is_none());
    }

    #[tokio::test]
    async fn subscribe_receives_events() {
        let mgr = RunManager::new();
        let run_id = mgr.create_run("p".into());
        let mut rx = mgr.subscribe(&run_id).unwrap();
        let event = SseEvent::Progress {
            phase: "source".into(),
            stream: "users".into(),
            records_read: Some(100),
            records_written: None,
            bytes_read: Some(4096),
        };
        mgr.send_event(&run_id, event.clone());
        let received = rx.recv().await.unwrap();
        assert!(matches!(received, SseEvent::Progress { .. }));
    }

    #[test]
    fn subscribe_nonexistent_returns_none() {
        let mgr = RunManager::new();
        assert!(mgr.subscribe("run_nope").is_none());
    }

    #[test]
    fn send_event_no_subscribers_does_not_panic() {
        let mgr = RunManager::new();
        let run_id = mgr.create_run("p".into());
        // No subscriber — should not panic.
        mgr.send_event(
            &run_id,
            SseEvent::Log {
                level: "info".into(),
                message: "hello".into(),
            },
        );
    }

    #[test]
    fn list_runs_returns_all() {
        let mgr = RunManager::new();
        let _ = mgr.create_run("a".into());
        let _ = mgr.create_run("b".into());
        let _ = mgr.create_run("c".into());
        assert_eq!(mgr.list_runs().len(), 3);
    }

    #[test]
    fn list_runs_empty() {
        let mgr = RunManager::new();
        assert!(mgr.list_runs().is_empty());
    }
}
