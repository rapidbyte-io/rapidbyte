use std::time::Duration;

use rapidbyte_controller::adapter::postgres::event_bus::PgEventBus;
use rapidbyte_controller::domain::event::DomainEvent;
use rapidbyte_controller::domain::ports::event_bus::EventBus;
use rapidbyte_controller::domain::run::RunState;
use tokio_stream::StreamExt;

use super::setup_db;

#[tokio::test]
async fn publish_and_receive_via_listen_notify() {
    let (pool, _container) = setup_db().await;
    let bus = PgEventBus::new(pool);
    bus.start_listener().await.unwrap();

    let mut stream = bus.subscribe("run-ev-1").await.unwrap();

    let event = DomainEvent::RunStateChanged {
        run_id: "run-ev-1".to_string(),
        state: RunState::Running,
        attempt: 1,
    };
    bus.publish(event).await.unwrap();

    let received = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timed out waiting for event")
        .expect("stream ended unexpectedly");

    match received {
        DomainEvent::RunStateChanged {
            run_id,
            state,
            attempt,
        } => {
            assert_eq!(run_id, "run-ev-1");
            assert_eq!(state, RunState::Running);
            assert_eq!(attempt, 1);
        }
        other => panic!("unexpected event variant: {other:?}"),
    }
}

#[tokio::test]
async fn subscribe_filters_by_run_id() {
    let (pool, _container) = setup_db().await;
    let bus = PgEventBus::new(pool);
    bus.start_listener().await.unwrap();

    let mut stream1 = bus.subscribe("run-filter-1").await.unwrap();

    // Publish event for a different run_id
    let wrong_event = DomainEvent::RunStateChanged {
        run_id: "run-filter-2".to_string(),
        state: RunState::Running,
        attempt: 1,
    };
    bus.publish(wrong_event).await.unwrap();

    // Should NOT receive anything for run-filter-1
    let timeout_result = tokio::time::timeout(Duration::from_millis(500), stream1.next()).await;
    assert!(timeout_result.is_err(), "should have timed out");

    // Now publish for the correct run_id
    let correct_event = DomainEvent::RunStateChanged {
        run_id: "run-filter-1".to_string(),
        state: RunState::Completed,
        attempt: 1,
    };
    bus.publish(correct_event).await.unwrap();

    let received = tokio::time::timeout(Duration::from_secs(5), stream1.next())
        .await
        .expect("timed out waiting for correct event")
        .expect("stream ended");

    match received {
        DomainEvent::RunStateChanged { run_id, state, .. } => {
            assert_eq!(run_id, "run-filter-1");
            assert_eq!(state, RunState::Completed);
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[tokio::test]
async fn cleanup_removes_empty_channel() {
    let (pool, _container) = setup_db().await;
    let bus = PgEventBus::new(pool);
    bus.start_listener().await.unwrap();

    // Subscribe then drop the stream
    let stream = bus.subscribe("run-cleanup").await.unwrap();
    drop(stream);

    // Cleanup should remove the entry
    bus.cleanup("run-cleanup").await;

    // Subscribe again -- should get a fresh channel (no error)
    let _stream2 = bus.subscribe("run-cleanup").await.unwrap();
}

#[tokio::test]
async fn publish_oversized_payload_truncated() {
    let (pool, _container) = setup_db().await;
    let bus = PgEventBus::new(pool);
    bus.start_listener().await.unwrap();

    let mut stream = bus.subscribe("run-big").await.unwrap();

    // Create a ProgressReported event with an 8000-byte message
    let big_message = "x".repeat(8000);
    let event = DomainEvent::ProgressReported {
        run_id: "run-big".to_string(),
        message: big_message,
        pct: Some(50.0),
    };

    // Should not error despite oversized payload
    bus.publish(event).await.unwrap();

    // The event should be received (possibly truncated)
    let received = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timed out waiting for truncated event")
        .expect("stream ended");

    match received {
        DomainEvent::ProgressReported { run_id, .. } => {
            assert_eq!(run_id, "run-big");
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[tokio::test]
async fn multiple_subscribers_same_run() {
    let (pool, _container) = setup_db().await;
    let bus = PgEventBus::new(pool);
    bus.start_listener().await.unwrap();

    let mut stream1 = bus.subscribe("run-multi").await.unwrap();
    let mut stream2 = bus.subscribe("run-multi").await.unwrap();

    let event = DomainEvent::RunCancelled {
        run_id: "run-multi".to_string(),
    };
    bus.publish(event).await.unwrap();

    let r1 = tokio::time::timeout(Duration::from_secs(5), stream1.next())
        .await
        .expect("stream1 timed out")
        .expect("stream1 ended");

    let r2 = tokio::time::timeout(Duration::from_secs(5), stream2.next())
        .await
        .expect("stream2 timed out")
        .expect("stream2 ended");

    // Both should receive RunCancelled
    assert!(matches!(r1, DomainEvent::RunCancelled { .. }));
    assert!(matches!(r2, DomainEvent::RunCancelled { .. }));
}
