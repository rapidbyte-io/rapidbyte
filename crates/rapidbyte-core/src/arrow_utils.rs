use anyhow::{Context, Result};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

/// Serialize a RecordBatch to Arrow IPC stream format bytes.
pub fn record_batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref())
        .context("Failed to create Arrow IPC StreamWriter")?;
    writer
        .write(batch)
        .context("Failed to write RecordBatch to IPC")?;
    writer.finish().context("Failed to finish IPC stream")?;
    Ok(buf)
}

/// Deserialize Arrow IPC stream format bytes into a Vec of RecordBatches.
pub fn ipc_to_record_batches(ipc_bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    let cursor = std::io::Cursor::new(ipc_bytes);
    let reader = StreamReader::try_new(cursor, None)
        .context("Failed to create Arrow IPC StreamReader")?;
    let batches: Result<Vec<_>, _> = reader.collect();
    let batches = batches.context("Failed to read RecordBatches from IPC stream")?;
    Ok(batches)
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
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
        )
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
}
