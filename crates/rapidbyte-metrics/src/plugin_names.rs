//! Single source of truth for built-in plugin duration metric names.
//!
//! Maps plugin-side histogram names (e.g. `"source_connect_secs"`) to their
//! OTel instrument names (e.g. `"plugin.source_connect_duration"`) and
//! snapshot field names.

/// A built-in plugin duration metric entry.
pub struct BuiltinDuration {
    /// Plugin-side name (e.g. `"source_connect_secs"`).
    pub plugin_name: &'static str,
    /// OTel instrument name (e.g. `"plugin.source_connect_duration"`).
    pub instrument_name: &'static str,
    /// Canonical snapshot field name. Usually same as `plugin_name`,
    /// except `"dest_arrow_decode_secs"` maps to itself for backward compat.
    pub snapshot_name: &'static str,
}

/// All built-in plugin duration metrics.
pub const BUILTIN_DURATIONS: &[BuiltinDuration] = &[
    BuiltinDuration {
        plugin_name: "source_connect_secs",
        instrument_name: "plugin.source_connect_duration",
        snapshot_name: "source_connect_secs",
    },
    BuiltinDuration {
        plugin_name: "source_query_secs",
        instrument_name: "plugin.source_query_duration",
        snapshot_name: "source_query_secs",
    },
    BuiltinDuration {
        plugin_name: "source_fetch_secs",
        instrument_name: "plugin.source_fetch_duration",
        snapshot_name: "source_fetch_secs",
    },
    BuiltinDuration {
        plugin_name: "source_arrow_encode_secs",
        instrument_name: "plugin.source_encode_duration",
        snapshot_name: "source_arrow_encode_secs",
    },
    BuiltinDuration {
        plugin_name: "dest_connect_secs",
        instrument_name: "plugin.dest_connect_duration",
        snapshot_name: "dest_connect_secs",
    },
    BuiltinDuration {
        plugin_name: "dest_flush_secs",
        instrument_name: "plugin.dest_flush_duration",
        snapshot_name: "dest_flush_secs",
    },
    BuiltinDuration {
        plugin_name: "dest_commit_secs",
        instrument_name: "plugin.dest_commit_duration",
        snapshot_name: "dest_commit_secs",
    },
    BuiltinDuration {
        plugin_name: "dest_arrow_decode_secs",
        instrument_name: "plugin.dest_decode_duration",
        snapshot_name: "dest_arrow_decode_secs",
    },
];

/// Look up a built-in duration by plugin-side name.
#[must_use]
pub fn find_by_plugin_name(name: &str) -> Option<&'static BuiltinDuration> {
    BUILTIN_DURATIONS.iter().find(|d| d.plugin_name == name)
}

/// Look up a built-in duration by OTel instrument name.
#[must_use]
pub fn find_by_instrument_name(name: &str) -> Option<&'static BuiltinDuration> {
    BUILTIN_DURATIONS.iter().find(|d| d.instrument_name == name)
}

/// Check if a raw name is a built-in plugin duration.
#[must_use]
pub fn is_builtin_name(name: &str) -> bool {
    BUILTIN_DURATIONS
        .iter()
        .any(|d| d.plugin_name == name || d.snapshot_name == name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_source_connect_by_plugin_name() {
        let entry = find_by_plugin_name("source_connect_secs").unwrap();
        assert_eq!(entry.instrument_name, "plugin.source_connect_duration");
    }

    #[test]
    fn find_dest_decode_by_instrument_name() {
        let entry = find_by_instrument_name("plugin.dest_decode_duration").unwrap();
        assert_eq!(entry.plugin_name, "dest_arrow_decode_secs");
    }

    #[test]
    fn unknown_name_returns_none() {
        assert!(find_by_plugin_name("unknown_metric").is_none());
        assert!(find_by_instrument_name("unknown.metric").is_none());
    }

    #[test]
    fn is_builtin_recognizes_all_entries() {
        for entry in BUILTIN_DURATIONS {
            assert!(is_builtin_name(entry.plugin_name));
        }
    }
}
