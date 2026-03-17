use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use sqlx::PgPool;
use tokio::sync::{broadcast, RwLock};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::domain::event::DomainEvent;
use crate::domain::ports::event_bus::{EventBus, EventBusError, EventStream};

fn box_err(e: impl std::error::Error + Send + Sync + 'static) -> EventBusError {
    EventBusError(Box::new(e))
}

/// Extracts the `run_id` from a `DomainEvent`.
fn event_run_id(event: &DomainEvent) -> &str {
    match event {
        DomainEvent::RunStateChanged { run_id, .. }
        | DomainEvent::ProgressReported { run_id, .. }
        | DomainEvent::RunCompleted { run_id, .. }
        | DomainEvent::RunFailed { run_id, .. }
        | DomainEvent::RunCancelled { run_id } => run_id,
    }
}

pub struct PgEventBus {
    pool: PgPool,
    subscribers: Arc<RwLock<HashMap<String, broadcast::Sender<DomainEvent>>>>,
}

impl PgEventBus {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            subscribers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Spawn a background listener task that receives Postgres NOTIFY messages
    /// on the `run_events` channel and dispatches them to in-process subscribers.
    ///
    /// # Errors
    ///
    /// Returns an error if the initial listener connection fails.
    pub async fn start_listener(&self) -> Result<(), EventBusError> {
        let mut listener = sqlx::postgres::PgListener::connect_with(&self.pool)
            .await
            .map_err(box_err)?;

        listener.listen("run_events").await.map_err(box_err)?;

        let subscribers = Arc::clone(&self.subscribers);

        tokio::spawn(async move {
            loop {
                match listener.recv().await {
                    Ok(notification) => {
                        let payload = notification.payload();
                        let event: DomainEvent = match serde_json::from_str(payload) {
                            Ok(e) => e,
                            Err(err) => {
                                tracing::warn!(
                                    error = %err,
                                    payload = payload,
                                    "failed to deserialize domain event from NOTIFY payload"
                                );
                                continue;
                            }
                        };

                        let run_id = event_run_id(&event).to_string();
                        let subs = subscribers.read().await;
                        if let Some(tx) = subs.get(&run_id) {
                            // Ignore send errors (no active receivers).
                            let _ = tx.send(event);
                        }
                    }
                    Err(err) => {
                        tracing::error!(error = %err, "PgListener recv error, reconnecting...");
                        // sqlx PgListener automatically reconnects, so we just
                        // log and continue the loop.
                    }
                }
            }
        });

        Ok(())
    }
}

#[async_trait]
impl EventBus for PgEventBus {
    async fn publish(&self, event: DomainEvent) -> Result<(), EventBusError> {
        let payload = serde_json::to_string(&event).map_err(|e| EventBusError(Box::new(e)))?;

        // PG NOTIFY payload limit is ~8000 bytes. Truncate oversized payloads
        // by stripping the message/detail and sending just the state change.
        let payload = if payload.len() > 7500 {
            tracing::warn!(
                len = payload.len(),
                "NOTIFY payload exceeds safe limit, sending truncated event"
            );
            // Build a minimal event with just the essential fields
            match &event {
                DomainEvent::RunStateChanged {
                    run_id,
                    state,
                    attempt,
                } => serde_json::to_string(&DomainEvent::RunStateChanged {
                    run_id: run_id.clone(),
                    state: *state,
                    attempt: *attempt,
                })
                .unwrap_or(payload),
                DomainEvent::ProgressReported { run_id, .. } => {
                    serde_json::to_string(&DomainEvent::ProgressReported {
                        run_id: run_id.clone(),
                        message: "<truncated>".to_string(),
                        pct: None,
                    })
                    .unwrap_or(payload)
                }
                DomainEvent::RunCompleted { run_id, .. } => {
                    // Strip metrics, send state change instead
                    serde_json::to_string(&DomainEvent::RunStateChanged {
                        run_id: run_id.clone(),
                        state: crate::domain::run::RunState::Completed,
                        attempt: 0,
                    })
                    .unwrap_or(payload)
                }
                DomainEvent::RunFailed { run_id, .. } => {
                    // Strip error details, send state change instead
                    serde_json::to_string(&DomainEvent::RunStateChanged {
                        run_id: run_id.clone(),
                        state: crate::domain::run::RunState::Failed,
                        attempt: 0,
                    })
                    .unwrap_or(payload)
                }
                DomainEvent::RunCancelled { run_id } => {
                    serde_json::to_string(&DomainEvent::RunStateChanged {
                        run_id: run_id.clone(),
                        state: crate::domain::run::RunState::Cancelled,
                        attempt: 0,
                    })
                    .unwrap_or(payload)
                }
            }
        } else {
            payload
        };

        sqlx::query("SELECT pg_notify('run_events', $1)")
            .bind(&payload)
            .execute(&self.pool)
            .await
            .map_err(box_err)?;

        Ok(())
    }

    async fn subscribe(&self, run_id: &str) -> Result<EventStream, EventBusError> {
        let mut subs = self.subscribers.write().await;
        let tx = subs
            .entry(run_id.to_string())
            .or_insert_with(|| broadcast::channel(64).0);
        let rx = tx.subscribe();

        let stream = BroadcastStream::new(rx).filter_map(std::result::Result::ok);

        Ok(Box::pin(stream))
    }
}
