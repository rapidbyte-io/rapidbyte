//! Criterion benchmarks for Arrow IPC encoding and decoding.
//!
//! These measure the hot path: source -> Arrow IPC encode -> channel -> Arrow IPC decode -> dest.

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{
    BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

fn create_test_batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("event_type", DataType::Utf8, true),
        Field::new("user_id", DataType::Int64, true),
        Field::new("amount", DataType::Float64, true),
        Field::new("is_active", DataType::Boolean, true),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]));

    let ids: Vec<i32> = (0..rows as i32).collect();
    let events: Vec<&str> = (0..rows)
        .map(|i| match i % 5 {
            0 => "click",
            1 => "purchase",
            2 => "view",
            3 => "signup",
            _ => "logout",
        })
        .collect();
    let user_ids: Vec<i64> = (0..rows).map(|i| (i % 1000) as i64 + 1).collect();
    let amounts: Vec<f64> = (0..rows).map(|i| i as f64 * 1.5).collect();
    let actives: Vec<bool> = (0..rows).map(|i| i % 2 == 0).collect();
    let timestamps: Vec<i64> = (0..rows)
        .map(|i| 1_700_000_000_000_000 + i as i64 * 1_000_000)
        .collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(events)),
            Arc::new(Int64Array::from(user_ids)),
            Arc::new(Float64Array::from(amounts)),
            Arc::new(BooleanArray::from(actives)),
            Arc::new(TimestampMicrosecondArray::from(timestamps)),
        ],
    )
    .unwrap()
}

fn encode_batch(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref()).unwrap();
    writer.write(batch).unwrap();
    writer.finish().unwrap();
    buf
}

fn bench_arrow_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrow/encode");

    for rows in [100, 1_000, 10_000, 100_000] {
        let batch = create_test_batch(rows);
        group.throughput(Throughput::Elements(rows as u64));

        group.bench_with_input(BenchmarkId::from_parameter(rows), &batch, |b, batch| {
            b.iter(|| encode_batch(batch));
        });
    }
    group.finish();
}

fn bench_arrow_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrow/decode");

    for rows in [100, 1_000, 10_000, 100_000] {
        let batch = create_test_batch(rows);
        let encoded = encode_batch(&batch);
        group.throughput(Throughput::Elements(rows as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(rows),
            &encoded,
            |b, encoded| {
                b.iter(|| {
                    let reader = StreamReader::try_new(Cursor::new(encoded), None).unwrap();
                    let _batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
                });
            },
        );
    }
    group.finish();
}

fn bench_arrow_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrow/roundtrip");

    for rows in [1_000, 10_000] {
        let batch = create_test_batch(rows);
        group.throughput(Throughput::Elements(rows as u64));

        group.bench_with_input(BenchmarkId::from_parameter(rows), &batch, |b, batch| {
            b.iter(|| {
                let encoded = encode_batch(batch);
                let reader = StreamReader::try_new(Cursor::new(&encoded), None).unwrap();
                let _batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_arrow_encode, bench_arrow_decode, bench_arrow_roundtrip);
criterion_main!(benches);
