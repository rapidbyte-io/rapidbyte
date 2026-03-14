use rapidbyte_sdk::prelude::*;

pub(crate) fn emit_dest_timings(ctx: &Context, perf: &WritePerf) -> Result<(), PluginError> {
    ctx.histogram("dest_connect_secs", perf.connect_secs)?;
    ctx.histogram("dest_flush_secs", perf.flush_secs)?;
    ctx.histogram("dest_commit_secs", perf.commit_secs)?;
    Ok(())
}
