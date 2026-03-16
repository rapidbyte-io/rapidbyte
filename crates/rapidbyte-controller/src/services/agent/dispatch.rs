//! Task dispatch: resolve secrets and build the `PollTaskResponse`.

use tonic::Status;

use crate::proto::rapidbyte::v1::{
    poll_task_response, ExecutionOptions, PollTaskResponse, TaskAssignment as ProtoTaskAssignment,
};

/// Resolve `${vault:...}` secret references in the pipeline YAML and build
/// the protobuf response sent to the polling agent.
///
/// Environment variables (`${ENV_VAR}`) are intentionally preserved so the
/// agent can expand them from its own environment, maintaining distributed-mode
/// semantics.
pub(crate) async fn resolve_and_build_response(
    assignment: crate::scheduler::TaskAssignment,
    secrets: &rapidbyte_secrets::SecretProviders,
) -> Result<PollTaskResponse, Status> {
    // Resolve only secret references (${vault:...}) at dispatch time.
    // Env vars (${ENV_VAR}) are left for the agent to expand from its own
    // environment, preserving distributed-mode semantics where agents may
    // have different env vars than the controller.
    let yaml_str = std::str::from_utf8(&assignment.pipeline_yaml)
        .map_err(|e| Status::internal(format!("pipeline YAML is not valid UTF-8: {e}")))?;
    // Reject malformed secret references (e.g. ${vault:path} without #key).
    rapidbyte_engine::config::parser::reject_malformed_refs(yaml_str)
        .map_err(|e| Status::internal(e.to_string()))?;
    let resolved = rapidbyte_engine::config::parser::substitute_secrets(yaml_str, secrets)
        .await
        .map_err(|e| {
            let msg = format!("failed to resolve secrets: {e}");
            let is_transient = e
                .downcast_ref::<rapidbyte_secrets::SecretError>()
                .is_some_and(rapidbyte_secrets::SecretError::is_transient);
            if is_transient {
                Status::unavailable(msg)
            } else {
                Status::internal(msg)
            }
        })?;

    // Redact YAML parse errors whenever secret substitution was performed,
    // since resolved secret values may appear in serde error messages.
    let had_secrets = resolved != yaml_str;

    // Validate the resolved YAML parses correctly.
    if let Err(e) = serde_yaml::from_str::<serde_yaml::Value>(&resolved) {
        let msg = if had_secrets {
            format!(
                "pipeline YAML invalid after variable resolution (source redacted): line {}, column {}",
                e.location().map_or(0, |l| l.line()),
                e.location().map_or(0, |l| l.column()),
            )
        } else {
            format!("pipeline YAML invalid after variable resolution: {e}")
        };
        return Err(Status::internal(msg));
    }

    let pipeline_yaml = resolved.into_bytes();

    Ok(PollTaskResponse {
        result: Some(poll_task_response::Result::Task(ProtoTaskAssignment {
            task_id: assignment.task_id,
            run_id: assignment.run_id,
            attempt: assignment.attempt,
            lease_epoch: assignment.lease_epoch,
            lease_expires_at: None,
            pipeline_yaml_utf8: pipeline_yaml,
            execution: Some(ExecutionOptions {
                dry_run: assignment.dry_run,
                limit: assignment.limit,
            }),
        })),
    })
}
