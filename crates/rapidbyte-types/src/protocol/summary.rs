use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MetricValue {
    Counter(u64),
    Gauge(f64),
    Histogram(f64),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Metric {
    pub name: String,
    pub value: MetricValue,
    pub labels: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WritePerf {
    pub connect_secs: f64,
    pub flush_secs: f64,
    pub commit_secs: f64,
    #[serde(default)]
    pub arrow_decode_secs: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReadPerf {
    pub connect_secs: f64,
    pub query_secs: f64,
    pub fetch_secs: f64,
    #[serde(default)]
    pub arrow_encode_secs: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReadSummary {
    pub records_read: u64,
    pub bytes_read: u64,
    pub batches_emitted: u64,
    pub checkpoint_count: u64,
    #[serde(default)]
    pub records_skipped: u64,
    #[serde(default)]
    pub perf: Option<ReadPerf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WriteSummary {
    pub records_written: u64,
    pub bytes_written: u64,
    pub batches_written: u64,
    pub checkpoint_count: u64,
    #[serde(default)]
    pub records_failed: u64,
    pub perf: Option<WritePerf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TransformSummary {
    pub records_in: u64,
    pub records_out: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub batches_processed: u64,
}
