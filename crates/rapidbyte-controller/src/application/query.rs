use crate::application::context::AppContext;
use crate::application::error::AppError;
use crate::domain::ports::repository::{Pagination, RunFilter, RunPage};
use crate::domain::run::Run;

/// Retrieve a single run by id.
///
/// # Errors
///
/// Returns `AppError::NotFound` if no run with the given id exists.
pub async fn get_run(ctx: &AppContext, run_id: &str) -> Result<Run, AppError> {
    ctx.find_run(run_id).await
}

/// List runs with optional filtering and pagination.
///
/// # Errors
///
/// Returns a repository error on failure.
pub async fn list_runs(
    ctx: &AppContext,
    filter: RunFilter,
    pagination: Pagination,
) -> Result<RunPage, AppError> {
    Ok(ctx.runs.list(filter, pagination).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::submit::submit_pipeline;
    use crate::application::testing::fake_context;
    use crate::domain::run::RunState;

    #[tokio::test]
    async fn get_run_found() {
        let tc = fake_context();
        let yaml = "pipeline: test-pipe\nversion: '1.0'";
        let submit = submit_pipeline(&tc.ctx, None, yaml.to_string(), 0, None)
            .await
            .unwrap();

        let run = get_run(&tc.ctx, &submit.run_id).await.unwrap();
        assert_eq!(run.id(), submit.run_id);
        assert_eq!(run.state(), RunState::Pending);
    }

    #[tokio::test]
    async fn get_run_not_found() {
        let tc = fake_context();
        let result = get_run(&tc.ctx, "nonexistent").await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), AppError::NotFound { .. }));
    }

    #[tokio::test]
    async fn list_runs_returns_results() {
        let tc = fake_context();
        let yaml = "pipeline: test-pipe\nversion: '1.0'";
        submit_pipeline(&tc.ctx, None, yaml.to_string(), 0, None)
            .await
            .unwrap();
        submit_pipeline(&tc.ctx, None, yaml.to_string(), 0, None)
            .await
            .unwrap();

        let page = list_runs(
            &tc.ctx,
            RunFilter {
                state: None,
                pipeline: None,
            },
            Pagination {
                page_size: 10,
                page_token: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(page.runs.len(), 2);
    }

    #[tokio::test]
    async fn list_runs_with_state_filter() {
        let tc = fake_context();
        let yaml = "pipeline: test-pipe\nversion: '1.0'";
        submit_pipeline(&tc.ctx, None, yaml.to_string(), 0, None)
            .await
            .unwrap();

        let page = list_runs(
            &tc.ctx,
            RunFilter {
                state: Some(RunState::Pending),
                pipeline: None,
            },
            Pagination {
                page_size: 10,
                page_token: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(page.runs.len(), 1);

        let empty_page = list_runs(
            &tc.ctx,
            RunFilter {
                state: Some(RunState::Running),
                pipeline: None,
            },
            Pagination {
                page_size: 10,
                page_token: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(empty_page.runs.len(), 0);
    }

    #[tokio::test]
    async fn list_runs_empty_result() {
        let tc = fake_context();

        let page = list_runs(
            &tc.ctx,
            RunFilter {
                state: None,
                pipeline: None,
            },
            Pagination {
                page_size: 10,
                page_token: None,
            },
        )
        .await
        .unwrap();

        assert!(page.runs.is_empty());
        assert!(page.next_page_token.is_none());
    }

    #[tokio::test]
    async fn list_runs_pagination() {
        let tc = fake_context();
        let yaml = "pipeline: test-pipe\nversion: '1.0'";
        submit_pipeline(&tc.ctx, None, yaml.to_string(), 0, None)
            .await
            .unwrap();
        submit_pipeline(&tc.ctx, None, yaml.to_string(), 0, None)
            .await
            .unwrap();
        submit_pipeline(&tc.ctx, None, yaml.to_string(), 0, None)
            .await
            .unwrap();

        let page = list_runs(
            &tc.ctx,
            RunFilter {
                state: None,
                pipeline: None,
            },
            Pagination {
                page_size: 2,
                page_token: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(page.runs.len(), 2);
        assert!(page.next_page_token.is_some());
    }

    #[tokio::test]
    async fn list_runs_default_page_size() {
        let tc = fake_context();

        // page_size=0 should work (returns empty result since 0 items requested)
        let page = list_runs(
            &tc.ctx,
            RunFilter {
                state: None,
                pipeline: None,
            },
            Pagination {
                page_size: 0,
                page_token: None,
            },
        )
        .await
        .unwrap();

        assert!(page.runs.is_empty());
    }
}
