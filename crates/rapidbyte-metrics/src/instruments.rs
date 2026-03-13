//! OTel instrument accessors using OnceLock.
//!
//! All instruments are lazily registered on first access via the OTel global meter provider.
//! If `metrics::init()` has not been called, instruments use the no-op global provider.

use std::sync::OnceLock;

use opentelemetry::global;
use opentelemetry::metrics::{Counter, Gauge, Histogram, UpDownCounter};

const METER_NAME: &str = "rapidbyte";

fn meter() -> opentelemetry::metrics::Meter {
    global::meter(METER_NAME)
}

macro_rules! define_counter_u64 {
    ($name:ident, $metric_name:expr) => {
        pub fn $name() -> &'static Counter<u64> {
            static INST: OnceLock<Counter<u64>> = OnceLock::new();
            INST.get_or_init(|| meter().u64_counter($metric_name).build())
        }
    };
}

macro_rules! define_histogram_f64 {
    ($name:ident, $metric_name:expr) => {
        pub fn $name() -> &'static Histogram<f64> {
            static INST: OnceLock<Histogram<f64>> = OnceLock::new();
            INST.get_or_init(|| meter().f64_histogram($metric_name).build())
        }
    };
}

macro_rules! define_gauge_f64 {
    ($name:ident, $metric_name:expr) => {
        pub fn $name() -> &'static Gauge<f64> {
            static INST: OnceLock<Gauge<f64>> = OnceLock::new();
            INST.get_or_init(|| meter().f64_gauge($metric_name).build())
        }
    };
}

macro_rules! define_updown_i64 {
    ($name:ident, $metric_name:expr) => {
        pub fn $name() -> &'static UpDownCounter<i64> {
            static INST: OnceLock<UpDownCounter<i64>> = OnceLock::new();
            INST.get_or_init(|| meter().i64_up_down_counter($metric_name).build())
        }
    };
}

pub mod pipeline {
    use super::*;
    define_counter_u64!(records_read, "pipeline.records_read");
    define_counter_u64!(records_written, "pipeline.records_written");
    define_counter_u64!(bytes_read, "pipeline.bytes_read");
    define_counter_u64!(bytes_written, "pipeline.bytes_written");
    define_histogram_f64!(duration, "pipeline.duration");
    define_counter_u64!(run_total, "pipeline.run_total");
    define_counter_u64!(run_errors, "pipeline.run_errors");
}

pub mod host {
    use super::*;
    define_histogram_f64!(emit_batch_duration, "host.emit_batch_duration");
    define_histogram_f64!(next_batch_duration, "host.next_batch_duration");
    define_histogram_f64!(next_batch_wait_duration, "host.next_batch_wait_duration");
    define_histogram_f64!(
        next_batch_process_duration,
        "host.next_batch_process_duration"
    );
    define_histogram_f64!(compress_duration, "host.compress_duration");
    define_histogram_f64!(decompress_duration, "host.decompress_duration");
    define_histogram_f64!(module_load_duration, "host.module_load_duration");
}

pub mod plugin {
    use super::*;
    define_histogram_f64!(source_connect_duration, "plugin.source_connect_duration");
    define_histogram_f64!(source_query_duration, "plugin.source_query_duration");
    define_histogram_f64!(source_fetch_duration, "plugin.source_fetch_duration");
    define_histogram_f64!(source_encode_duration, "plugin.source_encode_duration");
    define_histogram_f64!(dest_connect_duration, "plugin.dest_connect_duration");
    define_histogram_f64!(dest_flush_duration, "plugin.dest_flush_duration");
    define_histogram_f64!(dest_commit_duration, "plugin.dest_commit_duration");
    define_histogram_f64!(dest_decode_duration, "plugin.dest_decode_duration");

    // Dynamic plugin metrics from DashMap cache (Task 5 is already complete)
    pub use crate::cache::{custom_counter, custom_gauge, custom_histogram};
}

pub mod controller {
    use super::*;
    define_updown_i64!(active_runs, "controller.active_runs");
    define_updown_i64!(active_agents, "controller.active_agents");
    define_gauge_f64!(preview_store_size, "controller.preview_store_size");
    define_counter_u64!(runs_submitted, "controller.runs_submitted");
    define_counter_u64!(runs_completed, "controller.runs_completed");
    define_counter_u64!(tasks_assigned, "controller.tasks_assigned");
    define_counter_u64!(tasks_completed, "controller.tasks_completed");
    define_counter_u64!(lease_grants, "controller.lease_grants");
    define_counter_u64!(lease_revocations, "controller.lease_revocations");
    define_counter_u64!(lease_renewals, "controller.lease_renewals");
    define_counter_u64!(heartbeat_received, "controller.heartbeat_received");
    define_counter_u64!(heartbeat_timeouts, "controller.heartbeat_timeouts");
    define_counter_u64!(reconciliation_sweeps, "controller.reconciliation_sweeps");
    define_counter_u64!(
        reconciliation_timeouts,
        "controller.reconciliation_timeouts"
    );
    define_histogram_f64!(state_persist_duration, "controller.state_persist_duration");
    define_counter_u64!(state_persist_errors, "controller.state_persist_errors");
}

pub mod agent {
    use super::*;
    define_updown_i64!(active_tasks, "agent.active_tasks");
    define_gauge_f64!(spool_entries, "agent.spool_entries");
    define_gauge_f64!(spool_disk_bytes, "agent.spool_disk_bytes");
    define_counter_u64!(tasks_received, "agent.tasks_received");
    define_counter_u64!(tasks_completed, "agent.tasks_completed");
    define_histogram_f64!(task_duration, "agent.task_duration");
    define_counter_u64!(records_processed, "agent.records_processed");
    define_counter_u64!(bytes_processed, "agent.bytes_processed");
    define_counter_u64!(flight_requests, "agent.flight_requests");
    define_histogram_f64!(flight_request_duration, "agent.flight_request_duration");
    define_counter_u64!(flight_batches_served, "agent.flight_batches_served");
    define_counter_u64!(previews_stored, "agent.previews_stored");
    define_counter_u64!(previews_evicted, "agent.previews_evicted");
    define_counter_u64!(preview_spill_to_disk, "agent.preview_spill_to_disk");
}

pub mod process {
    use super::*;
    define_gauge_f64!(cpu_seconds, "process.cpu_seconds");
    define_gauge_f64!(peak_rss_bytes, "process.peak_rss_bytes");
}

pub mod grpc {
    use super::*;
    define_histogram_f64!(request_duration, "grpc.request.duration");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_instruments_are_stable_references() {
        // Calling the accessor twice returns the same reference
        let a = pipeline::records_read();
        let b = pipeline::records_read();
        assert!(std::ptr::eq(a, b));
    }

    #[test]
    fn host_instruments_are_stable_references() {
        let a = host::emit_batch_duration();
        let b = host::emit_batch_duration();
        assert!(std::ptr::eq(a, b));
    }
}
