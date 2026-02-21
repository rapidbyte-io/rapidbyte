//! Arrow IPC serialization and streaming batch utility helpers.

use std::io::Cursor;

use anyhow::{Context, Result};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

const IPC_STREAM_OVERHEAD_BYTES: usize = 1024;

fn estimate_ipc_capacity(batch: &RecordBatch) -> usize {
    batch
        .get_array_memory_size()
        .saturating_add(IPC_STREAM_OVERHEAD_BYTES)
}

/// Serialize a RecordBatch to Arrow IPC stream format bytes.
pub fn record_batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(estimate_ipc_capacity(batch));
    let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref())
        .context("Failed to create Arrow IPC StreamWriter")?;
    writer
        .write(batch)
        .context("Failed to write RecordBatch to IPC")?;
    writer.finish().context("Failed to finish IPC stream")?;
    Ok(buf)
}

/// Decode Arrow IPC and call `f` once per batch without building an intermediate `Vec`.
pub fn for_each_ipc_batch<F>(ipc_bytes: &[u8], mut f: F) -> Result<()>
where
    F: FnMut(RecordBatch) -> Result<()>,
{
    let cursor = Cursor::new(ipc_bytes);
    let reader =
        StreamReader::try_new(cursor, None).context("Failed to create Arrow IPC StreamReader")?;

    for maybe_batch in reader {
        let batch = maybe_batch.context("Failed to read RecordBatch from IPC stream")?;
        f(batch)?;
    }
    Ok(())
}

/// Deserialize Arrow IPC stream format bytes into a Vec of RecordBatches.
pub fn ipc_to_record_batches(ipc_bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    let mut batches: Vec<RecordBatch> = Vec::new();
    for_each_ipc_batch(ipc_bytes, |batch| {
        batches.push(batch);
        Ok(())
    })?;
    Ok(batches)
}

/// Count rows in an Arrow IPC stream without materializing all batches at once.
pub fn count_ipc_rows(ipc_bytes: &[u8]) -> Result<u64> {
    let mut total_rows = 0u64;
    for_each_ipc_batch(ipc_bytes, |batch| {
        total_rows += batch.num_rows() as u64;
        Ok(())
    })?;
    Ok(total_rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_roundtrip_record_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
            ],
        )
        .unwrap();

        let ipc_bytes = record_batch_to_ipc(&batch).unwrap();
        assert!(!ipc_bytes.is_empty());

        let batches = ipc_to_record_batches(&ipc_bytes).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[0].num_columns(), 2);

        // Verify data integrity
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
        assert_eq!(ids.value(2), 3);

        let names = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alice");
        assert_eq!(names.value(1), "bob");
        assert_eq!(names.value(2), "carol");
    }

    #[test]
    fn test_roundtrip_empty_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(Vec::<i64>::new()))])
                .unwrap();

        let ipc_bytes = record_batch_to_ipc(&batch).unwrap();
        let batches = ipc_to_record_batches(&ipc_bytes).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);
    }

    #[test]
    fn test_roundtrip_multiple_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
            Field::new("label", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(Float64Array::from(vec![1.5, 2.5])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ],
        )
        .unwrap();

        let ipc_bytes = record_batch_to_ipc(&batch).unwrap();
        let batches = ipc_to_record_batches(&ipc_bytes).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].num_columns(), 3);

        // Check nullable column
        let labels = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(labels.value(0), "a");
        assert!(labels.is_null(1));
    }

    #[test]
    fn test_invalid_ipc_bytes_errors() {
        let result = ipc_to_record_batches(b"not valid ipc data");
        assert!(result.is_err());
    }

    #[test]
    fn test_count_ipc_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            ],
        )
        .unwrap();

        let ipc_bytes = record_batch_to_ipc(&batch).unwrap();
        let row_count = count_ipc_rows(&ipc_bytes).unwrap();
        assert_eq!(row_count, 4);
    }
}
