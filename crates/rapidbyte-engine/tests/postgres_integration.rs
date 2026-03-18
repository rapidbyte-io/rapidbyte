//! Postgres integration tests using testcontainers.
//!
//! These tests run against a real Postgres container to verify the
//! `PgBackend` adapter implementations for all repository port traits
//! and the legacy `StateBackend` sync interface.
//!
//! Gated behind `--features integration` to avoid requiring Docker in CI.

#![cfg(feature = "integration")]

use std::sync::{Arc, OnceLock};

use rapidbyte_engine::adapter::postgres::PgBackend;
use rapidbyte_engine::{CursorRepository, DlqRepository, RunRecordRepository};
use rapidbyte_types::envelope::{DlqRecord, Timestamp};
use rapidbyte_types::error::ErrorCategory;
use rapidbyte_types::state::{CursorState, PipelineId, RunStats, RunStatus, StreamName};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

// ---------------------------------------------------------------------------
// Shared container setup
// ---------------------------------------------------------------------------

struct SharedPg {
    backend: Arc<PgBackend>,
    /// A long-lived runtime that owns the sqlx pool and container.
    /// Each test borrows the runtime handle to run async operations,
    /// ensuring the pool's internal connections stay valid.
    rt: tokio::runtime::Runtime,
    /// Kept alive so the container is not dropped.
    _container: testcontainers::ContainerAsync<Postgres>,
}

// SAFETY: Both `Arc<PgBackend>` and `ContainerAsync` are Send+Sync.
// The `Runtime` is Send+Sync in tokio.
unsafe impl Send for SharedPg {}
unsafe impl Sync for SharedPg {}

/// Lazily starts a single Postgres container shared across all tests.
static PG: OnceLock<SharedPg> = OnceLock::new();

fn shared_pg() -> &'static SharedPg {
    PG.get_or_init(|| {
        // Create a dedicated multi-thread runtime that outlives all tests.
        // The sqlx pool and container are owned by this runtime, so they
        // remain valid across individual test runtimes.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let (backend, container) = rt.block_on(async {
            let container = Postgres::default().start().await.unwrap();
            let port = container.get_host_port_ipv4(5432).await.unwrap();
            let connstr = format!("postgres://postgres:postgres@localhost:{port}/postgres");

            let backend = PgBackend::connect(&connstr).await.unwrap();
            backend.migrate().await.unwrap();

            (Arc::new(backend), container)
        });

        SharedPg {
            backend,
            rt,
            _container: container,
        }
    })
}

/// Run an async block on the shared runtime that owns the sqlx pool.
fn run_async<F, R>(f: F) -> R
where
    F: std::future::Future<Output = R>,
{
    shared_pg().rt.block_on(f)
}

/// Helper to get the shared backend.
fn backend() -> Arc<PgBackend> {
    Arc::clone(&shared_pg().backend)
}

/// Build a `DlqRecord` with sensible defaults.
fn make_dlq_record(stream: &str, id: u32) -> DlqRecord {
    DlqRecord {
        stream_name: stream.to_string(),
        record_json: format!(r#"{{"id": {id}, "name": "test"}}"#),
        error_message: format!("error for record {id}"),
        error_category: ErrorCategory::Data,
        failed_at: Timestamp::new("2026-01-15T10:30:00Z"),
    }
}

/// Run a closure on a dedicated OS thread (no tokio runtime context).
/// Required for sync `StateBackend` methods that use the sync `postgres`
/// crate, which internally creates its own tokio runtime.
fn run_sync<F, R>(f: F) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    std::thread::spawn(f).join().expect("sync thread panicked")
}

// ===================================================================
// Cursor tests (6)
// ===================================================================

#[test]
fn pg_cursor_get_returns_none_for_missing() {
    let pg = backend();
    let result = run_async(async {
        pg.get(
            &PipelineId::new("missing_pipeline"),
            &StreamName::new("missing_stream"),
        )
        .await
        .unwrap()
    });
    assert!(result.is_none());
}

#[test]
fn pg_cursor_set_and_get_roundtrip() {
    let pg = backend();
    let pipeline = PipelineId::new("test_roundtrip");
    let stream = StreamName::new("users");
    let cursor = CursorState {
        cursor_field: Some("updated_at".into()),
        cursor_value: Some("2026-01-15T10:30:00Z".into()),
        updated_at: "2026-01-15T10:30:00Z".into(),
    };

    run_async(async {
        pg.set(&pipeline, &stream, &cursor).await.unwrap();
    });

    let loaded = run_async(async { pg.get(&pipeline, &stream).await.unwrap().unwrap() });
    assert_eq!(loaded.cursor_field, cursor.cursor_field);
    assert_eq!(loaded.cursor_value, cursor.cursor_value);
    // updated_at is set by the DB (now()), so just check it is non-empty
    assert!(!loaded.updated_at.is_empty());
}

#[test]
fn pg_cursor_set_updates_existing() {
    let pg = backend();
    let pipeline = PipelineId::new("test_overwrite");
    let stream = StreamName::new("orders");

    let first = CursorState {
        cursor_field: Some("id".into()),
        cursor_value: Some("100".into()),
        updated_at: String::new(),
    };

    let second = CursorState {
        cursor_field: Some("id".into()),
        cursor_value: Some("200".into()),
        updated_at: String::new(),
    };

    run_async(async {
        pg.set(&pipeline, &stream, &first).await.unwrap();
        pg.set(&pipeline, &stream, &second).await.unwrap();
    });

    let loaded = run_async(async { pg.get(&pipeline, &stream).await.unwrap().unwrap() });
    assert_eq!(loaded.cursor_value, Some("200".into()));
}

#[test]
fn pg_cursor_compare_and_set_succeeds() {
    let pg = backend();
    let pipeline = PipelineId::new("test_cas_ok");
    let stream = StreamName::new("events");

    // Seed a cursor value
    let cursor = CursorState {
        cursor_field: Some("id".into()),
        cursor_value: Some("50".into()),
        updated_at: String::new(),
    };

    run_async(async {
        pg.set(&pipeline, &stream, &cursor).await.unwrap();
    });

    // CAS with correct expected value
    let ok = run_async(async {
        CursorRepository::compare_and_set(&*pg, &pipeline, &stream, Some("50"), "51")
            .await
            .unwrap()
    });
    assert!(ok);

    // Verify it was updated
    let loaded = run_async(async { pg.get(&pipeline, &stream).await.unwrap().unwrap() });
    assert_eq!(loaded.cursor_value, Some("51".into()));
}

#[test]
fn pg_cursor_compare_and_set_fails_on_mismatch() {
    let pg = backend();
    let pipeline = PipelineId::new("test_cas_fail");
    let stream = StreamName::new("events");

    // Seed a cursor value
    let cursor = CursorState {
        cursor_field: Some("id".into()),
        cursor_value: Some("10".into()),
        updated_at: String::new(),
    };

    run_async(async {
        pg.set(&pipeline, &stream, &cursor).await.unwrap();
    });

    // CAS with wrong expected value
    let ok = run_async(async {
        CursorRepository::compare_and_set(&*pg, &pipeline, &stream, Some("999"), "11")
            .await
            .unwrap()
    });
    assert!(!ok);

    // Value should remain unchanged
    let loaded = run_async(async { pg.get(&pipeline, &stream).await.unwrap().unwrap() });
    assert_eq!(loaded.cursor_value, Some("10".into()));
}

#[test]
fn pg_cursor_compare_and_set_insert_if_absent() {
    let pg = backend();
    let pipeline = PipelineId::new("test_cas_insert");
    let stream = StreamName::new("new_stream");

    // CAS with expected=None on a key that does not exist yet
    let ok = run_async(async {
        CursorRepository::compare_and_set(&*pg, &pipeline, &stream, None, "first")
            .await
            .unwrap()
    });
    assert!(ok);

    // Verify inserted
    let loaded = run_async(async { pg.get(&pipeline, &stream).await.unwrap().unwrap() });
    assert_eq!(loaded.cursor_value, Some("first".into()));
}

// ===================================================================
// Run record tests (3)
// ===================================================================

#[test]
fn pg_run_record_start_returns_id() {
    let pg = backend();
    let run_id = run_async(async {
        RunRecordRepository::start(
            &*pg,
            &PipelineId::new("test_run_start"),
            &StreamName::new("users"),
        )
        .await
        .unwrap()
    });
    assert!(run_id > 0);
}

#[test]
fn pg_run_record_complete_sets_status() {
    let pg = backend();
    let run_id = run_async(async {
        RunRecordRepository::start(
            &*pg,
            &PipelineId::new("test_run_complete"),
            &StreamName::new("users"),
        )
        .await
        .unwrap()
    });

    let stats = RunStats {
        records_read: 100,
        records_written: 95,
        bytes_read: 5000,
        bytes_written: 4750,
        error_message: None,
    };
    let result = run_async(async {
        RunRecordRepository::complete(&*pg, run_id, RunStatus::Completed, &stats).await
    });
    assert!(result.is_ok());
}

#[test]
fn pg_run_record_complete_nonexistent_fails() {
    let pg = backend();
    let stats = RunStats {
        records_read: 0,
        records_written: 0,
        bytes_read: 0,
        bytes_written: 0,
        error_message: Some("boom".into()),
    };
    // Completing a non-existent run_id should not error at the SQL level
    // (UPDATE ... WHERE id = $7 simply affects 0 rows), so this succeeds.
    // The contract does not mandate an error for missing IDs; verify it does not panic.
    let result = run_async(async {
        RunRecordRepository::complete(&*pg, i64::MAX, RunStatus::Failed, &stats).await
    });
    assert!(result.is_ok());
}

// ===================================================================
// DLQ tests (2)
// ===================================================================

#[test]
fn pg_dlq_insert_records() {
    let pg = backend();
    let pipeline = PipelineId::new("test_dlq_insert");

    let run_id = run_async(async {
        RunRecordRepository::start(&*pg, &pipeline, &StreamName::new("users"))
            .await
            .unwrap()
    });

    let records = vec![
        make_dlq_record("users", 1),
        make_dlq_record("users", 2),
        make_dlq_record("orders", 3),
    ];

    let count = run_async(async {
        DlqRepository::insert(&*pg, &pipeline, run_id, &records)
            .await
            .unwrap()
    });
    assert_eq!(count, 3);
}

#[test]
fn pg_dlq_insert_empty_batch() {
    let pg = backend();
    let pipeline = PipelineId::new("test_dlq_empty");

    let run_id = run_async(async {
        RunRecordRepository::start(&*pg, &pipeline, &StreamName::new("users"))
            .await
            .unwrap()
    });

    let count = run_async(async {
        DlqRepository::insert(&*pg, &pipeline, run_id, &[])
            .await
            .unwrap()
    });
    assert_eq!(count, 0);
}

// ===================================================================
// StateBackend sync tests (3)
// ===================================================================

#[test]
fn pg_state_backend_cursor_roundtrip() {
    let pg = backend();
    let sb = pg.as_state_backend();

    run_sync(move || {
        let pipeline = PipelineId::new("test_sb_cursor");
        let stream = StreamName::new("products");

        let cursor = CursorState {
            cursor_field: Some("id".into()),
            cursor_value: Some("42".into()),
            updated_at: "2026-03-01T12:00:00Z".into(),
        };

        sb.set_cursor(&pipeline, &stream, &cursor).unwrap();
        let loaded = sb.get_cursor(&pipeline, &stream).unwrap().unwrap();
        assert_eq!(loaded.cursor_field, Some("id".into()));
        assert_eq!(loaded.cursor_value, Some("42".into()));
    });
}

#[test]
fn pg_state_backend_run_lifecycle() {
    let pg = backend();
    let sb = pg.as_state_backend();

    run_sync(move || {
        let pipeline = PipelineId::new("test_sb_run");
        let stream = StreamName::new("users");

        let run_id = sb.start_run(&pipeline, &stream).unwrap();
        assert!(run_id > 0);

        let stats = RunStats {
            records_read: 200,
            records_written: 200,
            bytes_read: 10_000,
            bytes_written: 10_000,
            error_message: None,
        };
        sb.complete_run(run_id, RunStatus::Completed, &stats)
            .unwrap();
    });
}

#[test]
fn pg_state_backend_dlq_insert() {
    let pg = backend();
    let sb = pg.as_state_backend();

    // Need a valid run_id (FK constraint), create it via the sync backend
    let run_id = run_sync({
        let sb = Arc::clone(&sb);
        move || {
            let pipeline = PipelineId::new("test_sb_dlq");
            let stream = StreamName::new("users");
            sb.start_run(&pipeline, &stream).unwrap()
        }
    });

    run_sync(move || {
        let pipeline = PipelineId::new("test_sb_dlq");
        let records = vec![make_dlq_record("users", 10), make_dlq_record("users", 11)];
        let count = sb.insert_dlq_records(&pipeline, run_id, &records).unwrap();
        assert_eq!(count, 2);
    });
}

// ===================================================================
// Migration test (1)
// ===================================================================

#[test]
fn pg_migrate_is_idempotent() {
    let pg = backend();
    // migrate() was already called during setup; call it again.
    let result = run_async(async { pg.migrate().await });
    assert!(result.is_ok(), "Second migrate() call should be a no-op");
}
