//! Arrow IPC helpers shared by source-postgres read paths.

use anyhow::Context;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

/// Encode a RecordBatch into Arrow IPC stream bytes.
pub(crate) fn batch_to_ipc(batch: &RecordBatch) -> anyhow::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut writer =
        StreamWriter::try_new(&mut buf, batch.schema().as_ref()).context("IPC writer error")?;
    writer.write(batch).context("IPC write error")?;
    writer.finish().context("IPC finish error")?;
    Ok(buf)
}
