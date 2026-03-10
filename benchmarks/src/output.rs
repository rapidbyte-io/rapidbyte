#![cfg_attr(not(test), allow(dead_code))]

use std::io::Write;

use anyhow::Result;

use crate::artifact::BenchmarkArtifact;

pub fn write_artifact_json<W: Write>(writer: &mut W, artifact: &BenchmarkArtifact) -> Result<()> {
    serde_json::to_writer(&mut *writer, artifact)?;
    writer.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::write_artifact_json;
    use crate::runner::{materialize_artifact, RunResult};

    #[test]
    fn missing_correctness_assertions_fail_the_run() {
        let err = materialize_artifact(RunResult::without_assertions("pr", "pr_smoke_pipeline"))
            .expect_err("should fail");

        assert!(err.to_string().contains("correctness assertions"));
    }

    #[test]
    fn artifact_writer_emits_json_document() {
        let artifact = materialize_artifact(RunResult::success(
            "pr",
            "pr_smoke_pipeline",
            "debug",
            serde_json::json!({}),
            1_000,
            false,
        ))
        .expect("artifact");

        let mut buf = Vec::new();
        write_artifact_json(&mut buf, &artifact).expect("write artifact");
        let rendered = String::from_utf8(buf).expect("utf8");

        assert!(rendered.contains("\"scenario_id\":\"pr_smoke_pipeline\""));
        assert!(rendered.ends_with('\n'));
    }
}
