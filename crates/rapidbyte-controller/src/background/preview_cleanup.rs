//! Preview cleanup background task.

use std::time::Duration;

use tracing::info;

use crate::state::ControllerState;

async fn cleanup_expired_previews(state: &ControllerState) -> usize {
    let expired_run_ids = { state.previews.write().await.remove_expired() };
    let pending = {
        let mut retry_set = state.preview_delete_retries.write().await;
        retry_set.extend(expired_run_ids);
        retry_set.iter().cloned().collect::<Vec<_>>()
    };
    let mut succeeded = Vec::new();
    for run_id in &pending {
        if let Err(error) = state.delete_preview(run_id).await {
            tracing::error!(run_id, ?error, "failed to delete expired durable preview");
        } else {
            succeeded.push(run_id.clone());
        }
    }
    if !succeeded.is_empty() {
        let mut retry_set = state.preview_delete_retries.write().await;
        for run_id in &succeeded {
            retry_set.remove(run_id);
        }
    }
    succeeded.len()
}

#[must_use]
pub fn spawn_preview_cleanup(
    state: ControllerState,
    interval_duration: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(interval_duration);
        loop {
            interval.tick().await;
            let removed = cleanup_expired_previews(&state).await;
            if removed > 0 {
                info!(removed, "Removed expired preview metadata entries");
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::test_support::FailingMetadataStore;
    use std::time::Duration;

    #[tokio::test]
    async fn preview_cleanup_task_removes_expired_entries() {
        let store = FailingMetadataStore::new();
        let state = ControllerState::with_metadata_store(b"test-signing-key", store.clone());
        let preview = crate::preview::PreviewEntry {
            run_id: "run-1".into(),
            task_id: "task-1".into(),
            flight_endpoint: "localhost:9091".into(),
            ticket: bytes::Bytes::from_static(b"ticket"),
            streams: vec![],
            created_at: std::time::Instant::now()
                .checked_sub(Duration::from_secs(120))
                .unwrap(),
            ttl: Duration::from_secs(60),
        };
        {
            let mut previews = state.previews.write().await;
            previews.store(preview.clone());
        }
        state
            .persist_preview_record(&preview)
            .await
            .expect("preview persistence should succeed");

        let handle = spawn_preview_cleanup(state.clone(), Duration::from_millis(10));
        tokio::time::sleep(Duration::from_millis(25)).await;
        handle.abort();

        let mut previews = state.previews.write().await;
        assert!(previews.get("run-1").is_none());
        assert_eq!(previews.remove_expired().len(), 0);
        assert!(store.persisted_preview("run-1").is_none());
        assert!(state.preview_delete_retries.read().await.is_empty());
    }

    #[tokio::test]
    async fn preview_cleanup_retries_failed_durable_deletes() {
        let store = FailingMetadataStore::new().fail_delete_preview_on(1);
        let state = ControllerState::with_metadata_store(b"test-signing-key", store.clone());
        let preview = crate::preview::PreviewEntry {
            run_id: "run-1".into(),
            task_id: "task-1".into(),
            flight_endpoint: "localhost:9091".into(),
            ticket: bytes::Bytes::from_static(b"ticket"),
            streams: vec![],
            created_at: std::time::Instant::now()
                .checked_sub(Duration::from_secs(120))
                .unwrap(),
            ttl: Duration::from_secs(60),
        };
        {
            let mut previews = state.previews.write().await;
            previews.store(preview.clone());
        }
        state
            .persist_preview_record(&preview)
            .await
            .expect("preview persistence should succeed");

        let removed = cleanup_expired_previews(&state).await;
        assert_eq!(removed, 0);
        assert!(store.persisted_preview("run-1").is_some());
        assert!(state.preview_delete_retries.read().await.contains("run-1"));

        let removed = cleanup_expired_previews(&state).await;
        assert_eq!(removed, 1);
        assert!(store.persisted_preview("run-1").is_none());
        assert!(state.preview_delete_retries.read().await.is_empty());
    }
}
