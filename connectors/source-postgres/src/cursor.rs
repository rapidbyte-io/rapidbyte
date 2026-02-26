//! Cursor extraction and max-value tracking for incremental reads.
//!
//! `CursorTracker` encapsulates the logic for determining which column to
//! track, what extraction strategy to use, and building the final checkpoint.

use rapidbyte_sdk::cursor::CursorType;
use rapidbyte_sdk::prelude::*;

use crate::types::Column;

/// Tracks the maximum cursor value observed during a read.
pub struct CursorTracker {
    col_idx: usize,
    cursor_field: String,
    strategy: Strategy,
    max_int: Option<i64>,
    max_text: Option<String>,
}

/// Whether to track cursor as integer or text.
enum Strategy {
    Int,
    Text,
}

impl CursorTracker {
    /// Create a tracker for the given cursor info and column list.
    ///
    /// Returns `Err` if the cursor field is not found in `columns`.
    pub fn new(info: &CursorInfo, columns: &[Column]) -> Result<Self, String> {
        let col_idx = columns
            .iter()
            .position(|c| c.name == info.cursor_field)
            .ok_or_else(|| {
                format!(
                    "cursor field '{}' not found in columns: [{}]",
                    info.cursor_field,
                    columns
                        .iter()
                        .map(|c| c.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })?;

        // Determine strategy: use Int if cursor type is Int64 or column is integer
        let strategy = if matches!(info.cursor_type, CursorType::Int64)
            || matches!(
                columns[col_idx].arrow_type,
                ArrowDataType::Int16 | ArrowDataType::Int32 | ArrowDataType::Int64
            ) {
            Strategy::Int
        } else {
            Strategy::Text
        };

        Ok(Self {
            col_idx,
            cursor_field: info.cursor_field.clone(),
            strategy,
            max_int: None,
            max_text: None,
        })
    }

    /// Column index to extract cursor values from.
    pub fn col_idx(&self) -> usize {
        self.col_idx
    }

    /// Whether this tracker uses integer extraction.
    pub fn is_int_strategy(&self) -> bool {
        matches!(self.strategy, Strategy::Int)
    }

    /// Record an integer cursor observation. Updates max if larger.
    pub fn observe_int(&mut self, value: i64) {
        match &self.max_int {
            Some(current) if value <= *current => {}
            _ => self.max_int = Some(value),
        }
    }

    /// Record a text cursor observation. Updates max if larger (lexicographic).
    pub fn observe_text(&mut self, value: &str) {
        match &self.max_text {
            Some(current) if value <= current.as_str() => {}
            _ => self.max_text = Some(value.to_string()),
        }
    }

    /// Consume the tracker and build a checkpoint if any values were observed.
    pub fn into_checkpoint(
        self,
        stream_name: &str,
        records_processed: u64,
        bytes_processed: u64,
    ) -> Option<Checkpoint> {
        let cursor_value = match self.strategy {
            Strategy::Int => self.max_int.map(|v| CursorValue::Utf8 {
                value: v.to_string(),
            }),
            Strategy::Text => self.max_text.map(|v| CursorValue::Utf8 { value: v }),
        };

        cursor_value.map(|cv| Checkpoint {
            id: 0,
            kind: CheckpointKind::Source,
            stream: stream_name.to_string(),
            cursor_field: Some(self.cursor_field),
            cursor_value: Some(cv),
            records_processed,
            bytes_processed,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cursor_info(
        field: &str,
        cursor_type: CursorType,
        last: Option<CursorValue>,
    ) -> CursorInfo {
        CursorInfo {
            cursor_field: field.to_string(),
            cursor_type,
            last_value: last,
        }
    }

    #[test]
    fn int_strategy_for_int64_cursor() {
        let info = make_cursor_info("id", CursorType::Int64, None);
        let cols = vec![Column::new("id", "bigint", false)];
        let tracker = CursorTracker::new(&info, &cols).unwrap();
        assert_eq!(tracker.col_idx(), 0);
    }

    #[test]
    fn int_strategy_for_int_column_with_utf8_cursor() {
        let info = make_cursor_info("id", CursorType::Utf8, None);
        let cols = vec![Column::new("id", "integer", false)];
        let tracker = CursorTracker::new(&info, &cols).unwrap();
        assert_eq!(tracker.col_idx(), 0);
        assert!(tracker.is_int_strategy());
    }

    #[test]
    fn missing_cursor_column_returns_error() {
        let info = make_cursor_info("missing", CursorType::Int64, None);
        let cols = vec![Column::new("id", "integer", false)];
        assert!(CursorTracker::new(&info, &cols).is_err());
    }

    #[test]
    fn track_max_int() {
        let info = make_cursor_info("id", CursorType::Int64, None);
        let cols = vec![Column::new("id", "bigint", false)];
        let mut tracker = CursorTracker::new(&info, &cols).unwrap();
        tracker.observe_int(5);
        tracker.observe_int(3);
        tracker.observe_int(10);
        tracker.observe_int(7);
        let cp = tracker.into_checkpoint("stream", 100, 2048);
        assert!(cp.is_some());
        let cp = cp.unwrap();
        assert_eq!(
            cp.cursor_value,
            Some(CursorValue::Utf8 {
                value: "10".to_string()
            })
        );
    }

    #[test]
    fn track_max_text() {
        let info = make_cursor_info("ts", CursorType::Utf8, None);
        let cols = vec![Column::new("ts", "text", false)];
        let mut tracker = CursorTracker::new(&info, &cols).unwrap();
        tracker.observe_text("2024-01-01");
        tracker.observe_text("2024-06-15");
        tracker.observe_text("2024-03-01");
        let cp = tracker.into_checkpoint("stream", 50, 1024);
        let cp = cp.unwrap();
        assert_eq!(
            cp.cursor_value,
            Some(CursorValue::Utf8 {
                value: "2024-06-15".to_string()
            })
        );
    }

    #[test]
    fn no_observations_returns_none() {
        let info = make_cursor_info("id", CursorType::Int64, None);
        let cols = vec![Column::new("id", "bigint", false)];
        let tracker = CursorTracker::new(&info, &cols).unwrap();
        assert!(tracker.into_checkpoint("stream", 0, 0).is_none());
    }
}
