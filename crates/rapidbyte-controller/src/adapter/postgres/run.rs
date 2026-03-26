use async_trait::async_trait;
use sqlx::{PgPool, Row};

use crate::domain::ports::repository::{
    Pagination, RepositoryError, RunFilter, RunPage, RunRepository,
};
use crate::domain::run::{Run, RunError, RunMetrics, RunState};

use super::error::box_err;

fn parse_run_state(s: &str) -> Result<RunState, RepositoryError> {
    use std::str::FromStr;
    RunState::from_str(s).map_err(|()| {
        RepositoryError::Other(Box::from(format!("unknown run state in database: {s}")))
    })
}

pub(super) fn run_state_to_str(state: RunState) -> &'static str {
    match state {
        RunState::Pending => "pending",
        RunState::Running => "running",
        RunState::Completed => "completed",
        RunState::Failed => "failed",
        RunState::Cancelled => "cancelled",
    }
}

pub(super) fn run_from_row(row: &sqlx::postgres::PgRow) -> Result<Run, RepositoryError> {
    let id: String = row.try_get("id").map_err(box_err)?;
    let idempotency_key: Option<String> = row.try_get("idempotency_key").map_err(box_err)?;
    let pipeline_name: String = row.try_get("pipeline_name").map_err(box_err)?;
    let pipeline_yaml: String = row.try_get("pipeline_yaml").map_err(box_err)?;
    let state_str: String = row.try_get("state").map_err(box_err)?;
    let cancel_requested: bool = row.try_get("cancel_requested").map_err(box_err)?;
    let attempt: i32 = row.try_get("attempt").map_err(box_err)?;
    let max_retries: i32 = row.try_get("max_retries").map_err(box_err)?;
    let timeout_seconds: Option<i64> = row.try_get("timeout_seconds").map_err(box_err)?;
    let error_code: Option<String> = row.try_get("error_code").map_err(box_err)?;
    let error_message: Option<String> = row.try_get("error_message").map_err(box_err)?;
    let rows_read: Option<i64> = row.try_get("rows_read").map_err(box_err)?;
    let rows_written: Option<i64> = row.try_get("rows_written").map_err(box_err)?;
    let bytes_read: Option<i64> = row.try_get("bytes_read").map_err(box_err)?;
    let bytes_written: Option<i64> = row.try_get("bytes_written").map_err(box_err)?;
    let duration_ms: Option<i64> = row.try_get("duration_ms").map_err(box_err)?;
    let created_at = row.try_get("created_at").map_err(box_err)?;
    let updated_at = row.try_get("updated_at").map_err(box_err)?;

    let state = parse_run_state(&state_str)?;

    let error = match (error_code, error_message) {
        (Some(code), Some(message)) => Some(RunError { code, message }),
        _ => None,
    };

    let metrics = match (
        rows_read,
        rows_written,
        bytes_read,
        bytes_written,
        duration_ms,
    ) {
        (Some(rr), Some(rw), Some(br), Some(bw), Some(dm)) => Some(RunMetrics {
            rows_read: rr.cast_unsigned(),
            rows_written: rw.cast_unsigned(),
            bytes_read: br.cast_unsigned(),
            bytes_written: bw.cast_unsigned(),
            duration_ms: dm.cast_unsigned(),
        }),
        _ => None,
    };

    Ok(Run::from_row(
        id,
        idempotency_key,
        pipeline_name,
        pipeline_yaml,
        state,
        attempt.cast_unsigned(),
        max_retries.cast_unsigned(),
        timeout_seconds.map(i64::cast_unsigned),
        cancel_requested,
        error,
        metrics,
        created_at,
        updated_at,
    ))
}

pub struct PgRunRepository {
    pool: PgPool,
}

impl PgRunRepository {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl RunRepository for PgRunRepository {
    async fn find_by_id(&self, id: &str) -> Result<Option<Run>, RepositoryError> {
        let row = sqlx::query("SELECT * FROM runs WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(box_err)?;

        row.as_ref().map(run_from_row).transpose()
    }

    async fn find_by_idempotency_key(&self, key: &str) -> Result<Option<Run>, RepositoryError> {
        let row = sqlx::query("SELECT * FROM runs WHERE idempotency_key = $1")
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(box_err)?;

        row.as_ref().map(run_from_row).transpose()
    }

    async fn save(&self, run: &Run) -> Result<(), RepositoryError> {
        let (error_code, error_message) = match run.error() {
            Some(e) => (Some(e.code.as_str()), Some(e.message.as_str())),
            None => (None, None),
        };

        let (rows_read, rows_written, bytes_read, bytes_written, duration_ms) = match run.metrics()
        {
            Some(m) => (
                Some(m.rows_read.cast_signed()),
                Some(m.rows_written.cast_signed()),
                Some(m.bytes_read.cast_signed()),
                Some(m.bytes_written.cast_signed()),
                Some(m.duration_ms.cast_signed()),
            ),
            None => (None, None, None, None, None),
        };

        sqlx::query(
            "INSERT INTO runs (id, idempotency_key, pipeline_name, pipeline_yaml, state, cancel_requested, attempt, max_retries, timeout_seconds, error_code, error_message, rows_read, rows_written, bytes_read, bytes_written, duration_ms, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
             ON CONFLICT (id) DO UPDATE SET
                idempotency_key = EXCLUDED.idempotency_key,
                pipeline_name = EXCLUDED.pipeline_name,
                pipeline_yaml = EXCLUDED.pipeline_yaml,
                state = EXCLUDED.state,
                cancel_requested = EXCLUDED.cancel_requested,
                attempt = EXCLUDED.attempt,
                max_retries = EXCLUDED.max_retries,
                timeout_seconds = EXCLUDED.timeout_seconds,
                error_code = EXCLUDED.error_code,
                error_message = EXCLUDED.error_message,
                rows_read = EXCLUDED.rows_read,
                rows_written = EXCLUDED.rows_written,
                bytes_read = EXCLUDED.bytes_read,
                bytes_written = EXCLUDED.bytes_written,
                duration_ms = EXCLUDED.duration_ms,
                updated_at = EXCLUDED.updated_at"
        )
        .bind(run.id())
        .bind(run.idempotency_key())
        .bind(run.pipeline_name())
        .bind(run.pipeline_yaml())
        .bind(run_state_to_str(run.state()))
        .bind(run.is_cancel_requested())
        .bind(run.current_attempt().cast_signed())
        .bind(run.max_retries().cast_signed())
        .bind(run.timeout_seconds().map(u64::cast_signed))
        .bind(error_code)
        .bind(error_message)
        .bind(rows_read)
        .bind(rows_written)
        .bind(bytes_read)
        .bind(bytes_written)
        .bind(duration_ms)
        .bind(run.created_at())
        .bind(run.updated_at())
        .execute(&self.pool)
        .await
        .map_err(box_err)?;

        Ok(())
    }

    async fn list(
        &self,
        filter: RunFilter,
        pagination: Pagination,
    ) -> Result<RunPage, RepositoryError> {
        // Cursor-based pagination using (created_at, id).
        // page_token format: "<created_at_rfc3339>|<id>"
        // Dynamic query building to support optional state + pipeline filters.
        use std::fmt::Write;

        let page_size = if pagination.page_size == 0 {
            20
        } else {
            pagination.page_size
        };
        let limit = i64::from(page_size) + 1;

        let mut sql = String::from("SELECT * FROM runs WHERE true");
        let mut param_idx: u32 = 1;

        // Optional state filter
        let state_str = filter.state.map(|s| run_state_to_str(s).to_string());
        if state_str.is_some() {
            let _ = write!(sql, " AND state = ${param_idx}");
            param_idx += 1;
        }

        // Optional pipeline filter
        if filter.pipeline.is_some() {
            let _ = write!(sql, " AND pipeline_name = ${param_idx}");
            param_idx += 1;
        }

        // Cursor-based pagination
        let cursor = if let Some(ref token) = pagination.page_token {
            let parts: Vec<&str> = token.splitn(2, '|').collect();
            if parts.len() != 2 {
                return Err(RepositoryError::Other(Box::from("invalid page token")));
            }
            let cursor_ts: chrono::DateTime<chrono::Utc> = parts[0]
                .parse()
                .map_err(|e: chrono::ParseError| RepositoryError::Other(Box::new(e)))?;
            let cursor_id = parts[1].to_string();
            let _ = write!(
                sql,
                " AND (created_at, id) < (${}, ${})",
                param_idx,
                param_idx + 1
            );
            param_idx += 2;
            Some((cursor_ts, cursor_id))
        } else {
            None
        };

        let _ = write!(sql, " ORDER BY created_at DESC, id DESC LIMIT ${param_idx}");

        // Bind parameters in order
        let mut query = sqlx::query(&sql);
        if let Some(ref s) = state_str {
            query = query.bind(s);
        }
        if let Some(ref p) = filter.pipeline {
            query = query.bind(p);
        }
        if let Some((ref ts, ref id)) = cursor {
            query = query.bind(ts);
            query = query.bind(id);
        }
        query = query.bind(limit);

        let rows = query.fetch_all(&self.pool).await.map_err(box_err)?;

        let has_next = rows.len() > page_size as usize;
        let take = if has_next {
            page_size as usize
        } else {
            rows.len()
        };

        let mut runs = Vec::with_capacity(take);
        for row in rows.iter().take(take) {
            runs.push(run_from_row(row)?);
        }

        let next_page_token = if has_next {
            let last = runs.last().expect("must have at least one run");
            Some(format!("{}|{}", last.created_at().to_rfc3339(), last.id()))
        } else {
            None
        };

        Ok(RunPage {
            runs,
            next_page_token,
        })
    }
}
