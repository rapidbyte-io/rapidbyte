use rapidbyte_sdk::prelude::*;

/// Best-effort: metric failures are silently ignored.
pub(crate) fn emit_dest_timings(ctx: &Context, perf: &WritePerf) {
    let _ = ctx.histogram("dest_connect_secs", perf.connect_secs, &[]);
    let _ = ctx.histogram("dest_flush_secs", perf.flush_secs, &[]);
    let _ = ctx.histogram("dest_commit_secs", perf.commit_secs, &[]);
}
