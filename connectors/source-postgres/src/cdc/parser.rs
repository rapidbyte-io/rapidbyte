//! Pure parsing functions for `PostgreSQL` `test_decoding` output.
//!
//! No I/O, no async, no host imports -- only deterministic text parsing
//! of the WAL change format emitted by `pg_logical_slot_get_changes()`.

/// CDC operation type extracted from `test_decoding` output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CdcOp {
    Insert,
    Update,
    Delete,
}

impl CdcOp {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            CdcOp::Insert => "insert",
            CdcOp::Update => "update",
            CdcOp::Delete => "delete",
        }
    }
}

/// A single parsed change from `test_decoding` output.
#[derive(Debug, Clone)]
pub(crate) struct CdcChange {
    pub(crate) op: CdcOp,
    pub(crate) table: String,
    pub(crate) columns: Vec<(String, String, String)>, // (name, pg_type, value)
}

/// Parse a single `test_decoding` output line into a `CdcChange`.
///
/// Format examples:
///   BEGIN 12345
///   table public.users: INSERT: id[integer]:1 name[text]:'John' active[boolean]:t
///   table public.users: UPDATE: id[integer]:1 name[text]:'Jane'
///   table public.users: DELETE: id[integer]:1
///   COMMIT 12345
///
/// Returns None for BEGIN/COMMIT lines.
pub(crate) fn parse_change_line(line: &str) -> Option<CdcChange> {
    let line = line.trim();

    if line.starts_with("BEGIN") || line.starts_with("COMMIT") {
        return None;
    }

    // Expected format: "table <schema.table>: <OP>: <columns>"
    let rest = line.strip_prefix("table ")?;

    // Split on ": " to get table and the remainder
    let first_colon = rest.find(": ")?;
    let table = rest[..first_colon].to_string();
    let after_table = &rest[first_colon + 2..];

    // Extract operation
    let op_colon = after_table.find(": ");
    let (op_str, columns_str) = match op_colon {
        Some(pos) => (&after_table[..pos], &after_table[pos + 2..]),
        None => (after_table, ""),
    };

    let op = match op_str {
        "INSERT" => CdcOp::Insert,
        "UPDATE" => CdcOp::Update,
        "DELETE" => CdcOp::Delete,
        _ => return None,
    };

    let columns = if columns_str.is_empty() {
        Vec::new()
    } else {
        parse_columns(columns_str)
    };

    Some(CdcChange { op, table, columns })
}

/// Parse `test_decoding` column values from a string like:
///   id[integer]:1 name[text]:'John' active[boolean]:t
pub(crate) fn parse_columns(s: &str) -> Vec<(String, String, String)> {
    let mut columns = Vec::new();
    let mut remaining = s.trim();

    while !remaining.is_empty() {
        // Find column name: everything before '['
        let Some(bracket_start) = remaining.find('[') else {
            break;
        };
        let col_name = remaining[..bracket_start].to_string();

        // Find type: everything between '[' and ']'
        let Some(bracket_end_offset) = remaining[bracket_start..].find(']') else {
            break;
        };
        let bracket_end = bracket_start + bracket_end_offset;
        let col_type = remaining[bracket_start + 1..bracket_end].to_string();

        // After ']' should be ':'
        let after_bracket = &remaining[bracket_end + 1..];
        if !after_bracket.starts_with(':') {
            break;
        }
        let value_start = &after_bracket[1..];

        // Parse value: could be quoted string or unquoted value
        let (value, rest) = parse_value(value_start);
        columns.push((col_name, col_type, value));
        remaining = rest.trim_start();
    }

    columns
}

/// Parse a single value from `test_decoding` output.
///
/// Informal grammar:
/// - `value = quoted | unquoted`
/// - `quoted = "'" ( char | "''" )* "'"` (`''` escapes a literal `'`)
/// - `unquoted = [^ ]+` (terminated by space or EOF)
///
/// Edge cases:
/// - `'O''Brien'` -> `O'Brien`
/// - `'eleve'` / CJK text -> preserved via char-wise iteration
/// - `null` -> literal string `"null"` (null semantics handled by caller)
/// - unterminated quoted value returns the accumulated payload
pub(crate) fn parse_value(s: &str) -> (String, &str) {
    if let Some(inner) = s.strip_prefix('\'') {
        // Quoted string value -- iterate by char to handle multi-byte UTF-8 correctly
        let mut value = String::new();
        let mut chars = inner.char_indices();
        loop {
            match chars.next() {
                Some((i, '\'')) => {
                    // Check for escaped quote ('')
                    let rest = &inner[i + 1..];
                    if rest.starts_with('\'') {
                        value.push('\'');
                        chars.next(); // skip the second quote
                    } else {
                        // End of quoted string; +1 for opening quote, +i+1 for closing quote
                        return (value, rest);
                    }
                }
                Some((_, ch)) => value.push(ch),
                None => return (value, ""), // unterminated quote
            }
        }
    } else {
        // Unquoted value - ends at space or end of string
        match s.find(' ') {
            Some(pos) => (s[..pos].to_string(), &s[pos..]),
            None => (s.to_string(), ""),
        }
    }
}

/// Compare two LSN strings (format: "X/YYYYYYYY").
/// Returns true if `a` is greater than `b`.
pub(crate) fn lsn_gt(a: &str, b: &str) -> bool {
    let parse_lsn = |s: &str| -> Option<(u64, u64)> {
        let parts: Vec<&str> = s.split('/').collect();
        if parts.len() != 2 {
            return None;
        }
        let high = u64::from_str_radix(parts[0], 16).ok()?;
        let low = u64::from_str_radix(parts[1], 16).ok()?;
        Some((high, low))
    };

    match (parse_lsn(a), parse_lsn(b)) {
        (Some(a), Some(b)) => a > b,
        _ => false, // unparseable LSN -- don't advance
    }
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_insert_line() {
        let line = "table public.users: INSERT: id[integer]:1 name[text]:'John' active[boolean]:t";
        let change = parse_change_line(line).expect("should parse INSERT");
        assert_eq!(change.op, CdcOp::Insert);
        assert_eq!(change.table, "public.users");
        assert_eq!(change.columns.len(), 3);
        assert_eq!(
            change.columns[0],
            ("id".into(), "integer".into(), "1".into())
        );
        assert_eq!(
            change.columns[1],
            ("name".into(), "text".into(), "John".into())
        );
        assert_eq!(
            change.columns[2],
            ("active".into(), "boolean".into(), "t".into())
        );
    }

    #[test]
    fn test_parse_update_line() {
        let line = "table public.users: UPDATE: id[integer]:1 name[text]:'Jane'";
        let change = parse_change_line(line).expect("should parse UPDATE");
        assert_eq!(change.op, CdcOp::Update);
        assert_eq!(change.table, "public.users");
        assert_eq!(change.columns.len(), 2);
        assert_eq!(
            change.columns[0],
            ("id".into(), "integer".into(), "1".into())
        );
        assert_eq!(
            change.columns[1],
            ("name".into(), "text".into(), "Jane".into())
        );
    }

    #[test]
    fn test_parse_delete_line() {
        let line = "table public.users: DELETE: id[integer]:1";
        let change = parse_change_line(line).expect("should parse DELETE");
        assert_eq!(change.op, CdcOp::Delete);
        assert_eq!(change.table, "public.users");
        assert_eq!(change.columns.len(), 1);
        assert_eq!(
            change.columns[0],
            ("id".into(), "integer".into(), "1".into())
        );
    }

    #[test]
    fn test_parse_begin_commit() {
        assert!(parse_change_line("BEGIN 12345").is_none());
        assert!(parse_change_line("COMMIT 12345").is_none());
    }

    #[test]
    fn test_parse_full_transaction() {
        let lines = vec![
            "BEGIN 12345",
            "table public.users: INSERT: id[integer]:1 name[text]:'John'",
            "table public.users: UPDATE: id[integer]:1 name[text]:'Jane'",
            "table public.users: DELETE: id[integer]:2",
            "COMMIT 12345",
        ];

        let changes: Vec<CdcChange> = lines.into_iter().filter_map(parse_change_line).collect();

        assert_eq!(changes.len(), 3);
        assert_eq!(changes[0].op, CdcOp::Insert);
        assert_eq!(changes[1].op, CdcOp::Update);
        assert_eq!(changes[2].op, CdcOp::Delete);
    }

    #[test]
    fn test_parse_empty_changes() {
        let lines: Vec<&str> = vec![];
        let changes: Vec<CdcChange> = lines.into_iter().filter_map(parse_change_line).collect();
        assert!(changes.is_empty());
    }

    #[test]
    fn test_parse_quoted_value_with_spaces() {
        let line = "table public.users: INSERT: id[integer]:1 name[text]:'John Doe'";
        let change = parse_change_line(line).unwrap();
        assert_eq!(
            change.columns[1],
            ("name".into(), "text".into(), "John Doe".into())
        );
    }

    #[test]
    fn test_parse_quoted_value_with_escaped_quote() {
        let line = "table public.users: INSERT: id[integer]:1 name[text]:'O''Brien'";
        let change = parse_change_line(line).unwrap();
        assert_eq!(
            change.columns[1],
            ("name".into(), "text".into(), "O'Brien".into())
        );
    }

    #[test]
    fn test_parse_null_value() {
        let line = "table public.users: INSERT: id[integer]:1 email[text]:null";
        let change = parse_change_line(line).unwrap();
        assert_eq!(
            change.columns[1],
            ("email".into(), "text".into(), "null".into())
        );
    }

    #[test]
    fn test_lsn_ordering() {
        // Basic ordering
        assert!(lsn_gt("0/16B3748", "0/16B3740"));
        assert!(!lsn_gt("0/16B3740", "0/16B3748"));
        assert!(!lsn_gt("0/16B3748", "0/16B3748"));

        // High part comparison
        assert!(lsn_gt("1/0", "0/FFFFFFFF"));
        assert!(!lsn_gt("0/FFFFFFFF", "1/0"));

        // Same high, different low
        assert!(lsn_gt("0/2", "0/1"));
    }

    #[test]
    fn test_cdc_op_as_str() {
        assert_eq!(CdcOp::Insert.as_str(), "insert");
        assert_eq!(CdcOp::Update.as_str(), "update");
        assert_eq!(CdcOp::Delete.as_str(), "delete");
    }

    #[test]
    fn test_parse_numeric_values() {
        let line = "table public.orders: INSERT: id[integer]:42 amount[numeric]:123.45 qty[bigint]:1000000";
        let change = parse_change_line(line).unwrap();
        assert_eq!(change.columns.len(), 3);
        assert_eq!(
            change.columns[0],
            ("id".into(), "integer".into(), "42".into())
        );
        assert_eq!(
            change.columns[1],
            ("amount".into(), "numeric".into(), "123.45".into())
        );
        assert_eq!(
            change.columns[2],
            ("qty".into(), "bigint".into(), "1000000".into())
        );
    }

    #[test]
    fn test_parse_boolean_values() {
        let line =
            "table public.flags: INSERT: id[integer]:1 active[boolean]:t archived[boolean]:f";
        let change = parse_change_line(line).unwrap();
        assert_eq!(
            change.columns[1],
            ("active".into(), "boolean".into(), "t".into())
        );
        assert_eq!(
            change.columns[2],
            ("archived".into(), "boolean".into(), "f".into())
        );
    }

    #[test]
    fn test_parse_different_schemas() {
        let line = "table myschema.orders: INSERT: id[integer]:1";
        let change = parse_change_line(line).unwrap();
        assert_eq!(change.table, "myschema.orders");
    }

    #[test]
    fn test_parse_multibyte_utf8_value() {
        let line = "table public.users: INSERT: id[integer]:1 name[text]:'\u{00e9}l\u{00e8}ve'";
        let change = parse_change_line(line).unwrap();
        assert_eq!(
            change.columns[1],
            ("name".into(), "text".into(), "\u{00e9}l\u{00e8}ve".into())
        );
    }

    #[test]
    fn test_parse_cjk_utf8_value() {
        let line = "table public.users: INSERT: id[integer]:1 name[text]:'\u{4f60}\u{597d}\u{4e16}\u{754c}'";
        let change = parse_change_line(line).unwrap();
        assert_eq!(
            change.columns[1],
            (
                "name".into(),
                "text".into(),
                "\u{4f60}\u{597d}\u{4e16}\u{754c}".into()
            )
        );
    }

    #[test]
    fn test_lsn_gt_unparseable_returns_false() {
        // Malformed LSN should not advance
        assert!(!lsn_gt("invalid", "0/1"));
        assert!(!lsn_gt("0/1", "invalid"));
        assert!(!lsn_gt("bad", "also_bad"));
    }
}
