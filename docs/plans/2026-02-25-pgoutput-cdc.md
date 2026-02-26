# pgoutput CDC Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace `test_decoding` CDC with `pgoutput` binary protocol — the built-in output plugin available on all managed PostgreSQL (RDS, Aurora, Cloud SQL, Supabase) without extensions.

**Architecture:** Keep the existing polling approach via `pg_logical_slot_get_binary_changes()` (compatible with tokio-postgres and WASI). Write a pure binary pgoutput decoder module. Use pgoutput protocol v1 (PG 10+). Add `publication` config field. Cache Relation messages (OID → column map) for tuple decoding. Switch Arrow schema from all-Utf8 to typed columns using the Relation metadata. Remove `test_decoding` completely.

**Tech Stack:** Rust, tokio-postgres, Arrow 53, wasm32-wasip2

---

## Context for Implementer

### Files you'll touch most

| File | Purpose |
|------|---------|
| `connectors/source-postgres/src/cdc/mod.rs` | CDC orchestration — the main `read_cdc_changes()` function |
| `connectors/source-postgres/src/cdc/parser.rs` | Currently: test_decoding text parser. Will be **deleted** |
| `connectors/source-postgres/src/cdc/pgoutput.rs` | **NEW**: pgoutput binary message decoder |
| `connectors/source-postgres/src/cdc/encode.rs` | **NEW**: pgoutput tuple → Arrow RecordBatch encoder |
| `connectors/source-postgres/src/config.rs` | Add `publication` config field |
| `connectors/source-postgres/src/types.rs` | OID → Arrow type mapping helper |
| `tests/connectors/postgres/scenarios/15_cdc.sh` | E2E test — update for pgoutput/publications |
| `tests/connectors/postgres/fixtures/pipelines/e2e_cdc.yaml` | Add `publication` to config |
| `tests/connectors/postgres/fixtures/sql/source_seed.sql` | Add `CREATE PUBLICATION` |

### Key design decisions

1. **Polling, not streaming**: We keep `pg_logical_slot_get_binary_changes()` instead of `START_REPLICATION` streaming. This works with standard tokio-postgres (no CopyBoth mode), is WASI-compatible, and matches our existing architecture. Streaming replication can be added later as an optimization.

2. **Protocol v1 only**: Avoids streaming transaction complexity (no extra `xid` field parsing). Works on PG 10+. Large transaction support (v2) can be added later.

3. **Typed Arrow columns**: pgoutput Relation messages carry column OIDs. We map OIDs to Arrow types instead of storing everything as Utf8. This is a significant improvement over test_decoding.

4. **`_rb_op` metadata column**: Preserved. Values remain `"insert"`, `"update"`, `"delete"`.

5. **TOAST handling**: Unchanged TOAST values (`'u'` marker) produce `null` in the Arrow batch. Users needing full rows should set `REPLICA IDENTITY FULL`. Documented in config validation warnings.

6. **Publication-based filtering**: pgoutput uses PostgreSQL publications for table filtering (server-side). The `publication` config field replaces client-side table name matching.

### pgoutput binary protocol quick reference

All integers are big-endian. Strings are null-terminated. LSNs are u64.

| Message | Type byte | Key fields |
|---------|-----------|------------|
| Begin | `'B'` | final_lsn(u64), timestamp(i64), xid(u32) |
| Commit | `'C'` | flags(u8), commit_lsn(u64), end_lsn(u64), timestamp(i64) |
| Relation | `'R'` | oid(u32), namespace(str), name(str), replica_identity(u8), columns[] |
| Insert | `'I'` | relation_oid(u32), `'N'`, TupleData |
| Update | `'U'` | relation_oid(u32), [`'K'`/`'O'` TupleData], `'N'` TupleData |
| Delete | `'D'` | relation_oid(u32), `'K'`/`'O'` TupleData |
| Truncate | `'T'` | num_relations(u32), option_bits(u8), oids[] |

TupleData: `num_columns(u16)`, then per column: kind(`'n'`=null, `'u'`=unchanged toast, `'t'`=text + len(u32) + bytes).

### Build commands

```bash
# Host tests (workspace)
cargo test --workspace

# Build connector (wasm32-wasip2)
cd connectors/source-postgres && cargo build

# Clippy
cd connectors/source-postgres && cargo clippy

# E2E
just e2e postgres
```

---

### Task 1: pgoutput Binary Decoder — Types and Helpers

**Files:**
- Create: `connectors/source-postgres/src/cdc/pgoutput.rs`

This is the core binary decoder. Pure functions, no I/O, no async. It parses raw `&[u8]` from `pg_logical_slot_get_binary_changes()` into typed Rust enums.

**Step 1: Write the failing tests**

Create `connectors/source-postgres/src/cdc/pgoutput.rs` with the types and test module:

```rust
//! Pure binary decoder for PostgreSQL `pgoutput` logical replication messages.
//!
//! No I/O, no async — only deterministic binary parsing of the WAL change
//! format returned by `pg_logical_slot_get_binary_changes()` with the
//! `pgoutput` output plugin.

use std::io::{Cursor, Read};

// ── Error ────────────────────────────────────────────────────────

/// Errors from decoding pgoutput binary messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DecodeError {
    /// Input buffer was empty.
    EmptyMessage,
    /// Unexpected end of buffer while reading a field.
    UnexpectedEof,
    /// String field contained invalid UTF-8.
    InvalidUtf8,
    /// Unknown message type byte.
    UnknownMessageType(u8),
    /// Unknown column value kind byte in TupleData.
    UnknownColumnKind(u8),
    /// Unknown replica identity byte.
    UnknownReplicaIdentity(u8),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyMessage => write!(f, "empty pgoutput message"),
            Self::UnexpectedEof => write!(f, "unexpected end of pgoutput message"),
            Self::InvalidUtf8 => write!(f, "invalid UTF-8 in pgoutput string field"),
            Self::UnknownMessageType(b) => {
                write!(f, "unknown pgoutput message type: 0x{b:02X}")
            }
            Self::UnknownColumnKind(b) => {
                write!(f, "unknown pgoutput column kind: 0x{b:02X}")
            }
            Self::UnknownReplicaIdentity(b) => {
                write!(f, "unknown replica identity: 0x{b:02X}")
            }
        }
    }
}

// ── Types ────────────────────────────────────────────────────────

/// Replica identity setting from a Relation message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplicaIdentity {
    /// Uses primary key (default).
    Default,
    /// No identity — DELETEs cannot be replicated.
    Nothing,
    /// Uses all columns.
    Full,
    /// Uses a specific unique index.
    Index,
}

/// A single column definition from a Relation message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ColumnDef {
    /// 1 if column is part of the replica identity key, 0 otherwise.
    pub flags: u8,
    /// Column name.
    pub name: String,
    /// PostgreSQL type OID.
    pub type_oid: u32,
    /// Type modifier (`atttypmod`), -1 if unset.
    pub type_modifier: i32,
}

/// A single column value from a TupleData section.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ColumnValue {
    /// SQL NULL.
    Null,
    /// Unchanged TOAST value (not materialized in WAL).
    UnchangedToast,
    /// Text-format value bytes (UTF-8, same as `SELECT col::text`).
    Text(Vec<u8>),
}

/// Ordered list of column values from an Insert/Update/Delete tuple.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TupleData {
    pub columns: Vec<ColumnValue>,
}

/// CDC operation type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CdcOp {
    Insert,
    Update,
    Delete,
}

impl CdcOp {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

/// Parsed pgoutput message.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PgOutputMessage {
    Begin {
        final_lsn: u64,
        timestamp: i64,
        xid: u32,
    },
    Commit {
        flags: u8,
        commit_lsn: u64,
        end_lsn: u64,
        timestamp: i64,
    },
    Relation {
        oid: u32,
        namespace: String,
        name: String,
        replica_identity: ReplicaIdentity,
        columns: Vec<ColumnDef>,
    },
    Insert {
        relation_oid: u32,
        new_tuple: TupleData,
    },
    Update {
        relation_oid: u32,
        key_tuple: Option<TupleData>,
        old_tuple: Option<TupleData>,
        new_tuple: TupleData,
    },
    Delete {
        relation_oid: u32,
        key_tuple: Option<TupleData>,
        old_tuple: Option<TupleData>,
    },
    Truncate {
        option_bits: u8,
        relation_oids: Vec<u32>,
    },
    Origin {
        lsn: u64,
        name: String,
    },
    Type {
        oid: u32,
        namespace: String,
        name: String,
    },
    Message {
        flags: u8,
        lsn: u64,
        prefix: String,
        content: Vec<u8>,
    },
}

// ── Binary read helpers ──────────────────────────────────────────

fn read_u8(cursor: &mut Cursor<&[u8]>) -> Result<u8, DecodeError> {
    let mut buf = [0u8; 1];
    cursor.read_exact(&mut buf).map_err(|_| DecodeError::UnexpectedEof)?;
    Ok(buf[0])
}

fn read_i16(cursor: &mut Cursor<&[u8]>) -> Result<i16, DecodeError> {
    let mut buf = [0u8; 2];
    cursor.read_exact(&mut buf).map_err(|_| DecodeError::UnexpectedEof)?;
    Ok(i16::from_be_bytes(buf))
}

fn read_u32(cursor: &mut Cursor<&[u8]>) -> Result<u32, DecodeError> {
    let mut buf = [0u8; 4];
    cursor.read_exact(&mut buf).map_err(|_| DecodeError::UnexpectedEof)?;
    Ok(u32::from_be_bytes(buf))
}

fn read_i32(cursor: &mut Cursor<&[u8]>) -> Result<i32, DecodeError> {
    let mut buf = [0u8; 4];
    cursor.read_exact(&mut buf).map_err(|_| DecodeError::UnexpectedEof)?;
    Ok(i32::from_be_bytes(buf))
}

fn read_u64(cursor: &mut Cursor<&[u8]>) -> Result<u64, DecodeError> {
    let mut buf = [0u8; 8];
    cursor.read_exact(&mut buf).map_err(|_| DecodeError::UnexpectedEof)?;
    Ok(u64::from_be_bytes(buf))
}

fn read_i64(cursor: &mut Cursor<&[u8]>) -> Result<i64, DecodeError> {
    let mut buf = [0u8; 8];
    cursor.read_exact(&mut buf).map_err(|_| DecodeError::UnexpectedEof)?;
    Ok(i64::from_be_bytes(buf))
}

/// Read a null-terminated UTF-8 string.
fn read_cstring(cursor: &mut Cursor<&[u8]>) -> Result<String, DecodeError> {
    let mut bytes = Vec::new();
    loop {
        let b = read_u8(cursor)?;
        if b == 0 {
            break;
        }
        bytes.push(b);
    }
    String::from_utf8(bytes).map_err(|_| DecodeError::InvalidUtf8)
}

/// Read a TupleData section (column count + per-column kind/value).
fn read_tuple_data(cursor: &mut Cursor<&[u8]>) -> Result<TupleData, DecodeError> {
    let num_cols = read_i16(cursor)? as usize;
    let mut columns = Vec::with_capacity(num_cols);

    for _ in 0..num_cols {
        let kind = read_u8(cursor)?;
        let value = match kind {
            b'n' => ColumnValue::Null,
            b'u' => ColumnValue::UnchangedToast,
            b't' => {
                let len = read_i32(cursor)?;
                if len < 0 {
                    return Err(DecodeError::UnexpectedEof);
                }
                let len = len as usize;
                let mut data = vec![0u8; len];
                cursor.read_exact(&mut data).map_err(|_| DecodeError::UnexpectedEof)?;
                ColumnValue::Text(data)
            }
            _ => return Err(DecodeError::UnknownColumnKind(kind)),
        };
        columns.push(value);
    }

    Ok(TupleData { columns })
}

fn decode_replica_identity(b: u8) -> Result<ReplicaIdentity, DecodeError> {
    match b {
        b'd' => Ok(ReplicaIdentity::Default),
        b'n' => Ok(ReplicaIdentity::Nothing),
        b'f' => Ok(ReplicaIdentity::Full),
        b'i' => Ok(ReplicaIdentity::Index),
        _ => Err(DecodeError::UnknownReplicaIdentity(b)),
    }
}

// ── Top-level decoder ────────────────────────────────────────────

/// Decode a single pgoutput message from raw bytes.
///
/// The input is the `data` column from `pg_logical_slot_get_binary_changes()`,
/// which is a single pgoutput message starting with its type byte.
pub(crate) fn decode(buf: &[u8]) -> Result<PgOutputMessage, DecodeError> {
    if buf.is_empty() {
        return Err(DecodeError::EmptyMessage);
    }

    let mut cursor = Cursor::new(buf);
    let msg_type = read_u8(&mut cursor)?;

    match msg_type {
        b'B' => {
            let final_lsn = read_u64(&mut cursor)?;
            let timestamp = read_i64(&mut cursor)?;
            let xid = read_u32(&mut cursor)?;
            Ok(PgOutputMessage::Begin { final_lsn, timestamp, xid })
        }
        b'C' => {
            let flags = read_u8(&mut cursor)?;
            let commit_lsn = read_u64(&mut cursor)?;
            let end_lsn = read_u64(&mut cursor)?;
            let timestamp = read_i64(&mut cursor)?;
            Ok(PgOutputMessage::Commit { flags, commit_lsn, end_lsn, timestamp })
        }
        b'R' => {
            let oid = read_u32(&mut cursor)?;
            let namespace = read_cstring(&mut cursor)?;
            let name = read_cstring(&mut cursor)?;
            let replica_identity = decode_replica_identity(read_u8(&mut cursor)?)?;
            let num_cols = read_i16(&mut cursor)? as usize;
            let mut columns = Vec::with_capacity(num_cols);
            for _ in 0..num_cols {
                let flags = read_u8(&mut cursor)?;
                let col_name = read_cstring(&mut cursor)?;
                let type_oid = read_u32(&mut cursor)?;
                let type_modifier = read_i32(&mut cursor)?;
                columns.push(ColumnDef {
                    flags,
                    name: col_name,
                    type_oid,
                    type_modifier,
                });
            }
            Ok(PgOutputMessage::Relation {
                oid,
                namespace,
                name,
                replica_identity,
                columns,
            })
        }
        b'I' => {
            let relation_oid = read_u32(&mut cursor)?;
            let marker = read_u8(&mut cursor)?;
            debug_assert_eq!(marker, b'N', "Insert message should have 'N' marker");
            let new_tuple = read_tuple_data(&mut cursor)?;
            Ok(PgOutputMessage::Insert { relation_oid, new_tuple })
        }
        b'U' => {
            let relation_oid = read_u32(&mut cursor)?;
            let first_marker = read_u8(&mut cursor)?;
            let (key_tuple, old_tuple, new_tuple) = match first_marker {
                b'K' => {
                    let key = read_tuple_data(&mut cursor)?;
                    let n_marker = read_u8(&mut cursor)?;
                    debug_assert_eq!(n_marker, b'N');
                    let new = read_tuple_data(&mut cursor)?;
                    (Some(key), None, new)
                }
                b'O' => {
                    let old = read_tuple_data(&mut cursor)?;
                    let n_marker = read_u8(&mut cursor)?;
                    debug_assert_eq!(n_marker, b'N');
                    let new = read_tuple_data(&mut cursor)?;
                    (None, Some(old), new)
                }
                b'N' => {
                    let new = read_tuple_data(&mut cursor)?;
                    (None, None, new)
                }
                _ => return Err(DecodeError::UnknownColumnKind(first_marker)),
            };
            Ok(PgOutputMessage::Update { relation_oid, key_tuple, old_tuple, new_tuple })
        }
        b'D' => {
            let relation_oid = read_u32(&mut cursor)?;
            let marker = read_u8(&mut cursor)?;
            let (key_tuple, old_tuple) = match marker {
                b'K' => (Some(read_tuple_data(&mut cursor)?), None),
                b'O' => (None, Some(read_tuple_data(&mut cursor)?)),
                _ => return Err(DecodeError::UnknownColumnKind(marker)),
            };
            Ok(PgOutputMessage::Delete { relation_oid, key_tuple, old_tuple })
        }
        b'T' => {
            let num_relations = read_u32(&mut cursor)? as usize;
            let option_bits = read_u8(&mut cursor)?;
            let mut relation_oids = Vec::with_capacity(num_relations);
            for _ in 0..num_relations {
                relation_oids.push(read_u32(&mut cursor)?);
            }
            Ok(PgOutputMessage::Truncate { option_bits, relation_oids })
        }
        b'O' => {
            let lsn = read_u64(&mut cursor)?;
            let name = read_cstring(&mut cursor)?;
            Ok(PgOutputMessage::Origin { lsn, name })
        }
        b'Y' => {
            let oid = read_u32(&mut cursor)?;
            let namespace = read_cstring(&mut cursor)?;
            let name = read_cstring(&mut cursor)?;
            Ok(PgOutputMessage::Type { oid, namespace, name })
        }
        b'M' => {
            let flags = read_u8(&mut cursor)?;
            let lsn = read_u64(&mut cursor)?;
            let prefix = read_cstring(&mut cursor)?;
            let content_length = read_u32(&mut cursor)? as usize;
            let mut content = vec![0u8; content_length];
            cursor
                .read_exact(&mut content)
                .map_err(|_| DecodeError::UnexpectedEof)?;
            Ok(PgOutputMessage::Message { flags, lsn, prefix, content })
        }
        _ => Err(DecodeError::UnknownMessageType(msg_type)),
    }
}

/// Format a u64 LSN as PostgreSQL "X/YYYYYYYY" string.
pub(crate) fn lsn_to_string(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)
}

/// Parse a PostgreSQL "X/YYYYYYYY" LSN string into a u64.
pub(crate) fn parse_lsn(s: &str) -> Option<u64> {
    let (hi, lo) = s.split_once('/')?;
    let hi = u64::from_str_radix(hi, 16).ok()?;
    let lo = u64::from_str_radix(lo, 16).ok()?;
    Some((hi << 32) | lo)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Helper: build binary messages ────────────────────────────

    fn build_begin(final_lsn: u64, timestamp: i64, xid: u32) -> Vec<u8> {
        let mut buf = vec![b'B'];
        buf.extend_from_slice(&final_lsn.to_be_bytes());
        buf.extend_from_slice(&timestamp.to_be_bytes());
        buf.extend_from_slice(&xid.to_be_bytes());
        buf
    }

    fn build_commit(flags: u8, commit_lsn: u64, end_lsn: u64, timestamp: i64) -> Vec<u8> {
        let mut buf = vec![b'C', flags];
        buf.extend_from_slice(&commit_lsn.to_be_bytes());
        buf.extend_from_slice(&end_lsn.to_be_bytes());
        buf.extend_from_slice(&timestamp.to_be_bytes());
        buf
    }

    fn build_relation(
        oid: u32,
        namespace: &str,
        name: &str,
        replica_identity: u8,
        columns: &[(u8, &str, u32, i32)],
    ) -> Vec<u8> {
        let mut buf = vec![b'R'];
        buf.extend_from_slice(&oid.to_be_bytes());
        buf.extend_from_slice(namespace.as_bytes());
        buf.push(0); // null terminator
        buf.extend_from_slice(name.as_bytes());
        buf.push(0);
        buf.push(replica_identity);
        buf.extend_from_slice(&(columns.len() as i16).to_be_bytes());
        for (flags, col_name, type_oid, type_mod) in columns {
            buf.push(*flags);
            buf.extend_from_slice(col_name.as_bytes());
            buf.push(0);
            buf.extend_from_slice(&type_oid.to_be_bytes());
            buf.extend_from_slice(&type_mod.to_be_bytes());
        }
        buf
    }

    fn build_tuple_data(columns: &[ColumnValue]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(columns.len() as i16).to_be_bytes());
        for col in columns {
            match col {
                ColumnValue::Null => buf.push(b'n'),
                ColumnValue::UnchangedToast => buf.push(b'u'),
                ColumnValue::Text(data) => {
                    buf.push(b't');
                    buf.extend_from_slice(&(data.len() as i32).to_be_bytes());
                    buf.extend_from_slice(data);
                }
            }
        }
        buf
    }

    fn build_insert(relation_oid: u32, tuple: &[ColumnValue]) -> Vec<u8> {
        let mut buf = vec![b'I'];
        buf.extend_from_slice(&relation_oid.to_be_bytes());
        buf.push(b'N');
        buf.extend(build_tuple_data(tuple));
        buf
    }

    fn build_update_simple(relation_oid: u32, new_tuple: &[ColumnValue]) -> Vec<u8> {
        let mut buf = vec![b'U'];
        buf.extend_from_slice(&relation_oid.to_be_bytes());
        buf.push(b'N');
        buf.extend(build_tuple_data(new_tuple));
        buf
    }

    fn build_update_with_old(
        relation_oid: u32,
        old_tuple: &[ColumnValue],
        new_tuple: &[ColumnValue],
    ) -> Vec<u8> {
        let mut buf = vec![b'U'];
        buf.extend_from_slice(&relation_oid.to_be_bytes());
        buf.push(b'O');
        buf.extend(build_tuple_data(old_tuple));
        buf.push(b'N');
        buf.extend(build_tuple_data(new_tuple));
        buf
    }

    fn build_delete_key(relation_oid: u32, key_tuple: &[ColumnValue]) -> Vec<u8> {
        let mut buf = vec![b'D'];
        buf.extend_from_slice(&relation_oid.to_be_bytes());
        buf.push(b'K');
        buf.extend(build_tuple_data(key_tuple));
        buf
    }

    fn build_truncate(relation_oids: &[u32], option_bits: u8) -> Vec<u8> {
        let mut buf = vec![b'T'];
        buf.extend_from_slice(&(relation_oids.len() as u32).to_be_bytes());
        buf.push(option_bits);
        for oid in relation_oids {
            buf.extend_from_slice(&oid.to_be_bytes());
        }
        buf
    }

    fn text(s: &str) -> ColumnValue {
        ColumnValue::Text(s.as_bytes().to_vec())
    }

    // ── Tests ────────────────────────────────────────────────────

    #[test]
    fn decode_begin() {
        let buf = build_begin(0x0000_0000_016B_3748, 704_000_000_000, 42);
        let msg = decode(&buf).unwrap();
        assert_eq!(
            msg,
            PgOutputMessage::Begin {
                final_lsn: 0x0000_0000_016B_3748,
                timestamp: 704_000_000_000,
                xid: 42,
            }
        );
    }

    #[test]
    fn decode_commit() {
        let buf = build_commit(0, 100, 200, 704_000_000_000);
        let msg = decode(&buf).unwrap();
        assert_eq!(
            msg,
            PgOutputMessage::Commit {
                flags: 0,
                commit_lsn: 100,
                end_lsn: 200,
                timestamp: 704_000_000_000,
            }
        );
    }

    #[test]
    fn decode_relation_with_columns() {
        let buf = build_relation(
            16385,
            "public",
            "users",
            b'd', // DEFAULT
            &[
                (1, "id", 23, -1),   // int4, key
                (0, "name", 25, -1), // text
                (0, "email", 25, -1),
            ],
        );
        let msg = decode(&buf).unwrap();
        match msg {
            PgOutputMessage::Relation {
                oid,
                namespace,
                name,
                replica_identity,
                columns,
            } => {
                assert_eq!(oid, 16385);
                assert_eq!(namespace, "public");
                assert_eq!(name, "users");
                assert_eq!(replica_identity, ReplicaIdentity::Default);
                assert_eq!(columns.len(), 3);
                assert_eq!(columns[0].flags, 1);
                assert_eq!(columns[0].name, "id");
                assert_eq!(columns[0].type_oid, 23);
                assert_eq!(columns[1].name, "name");
                assert_eq!(columns[2].name, "email");
            }
            _ => panic!("expected Relation"),
        }
    }

    #[test]
    fn decode_insert() {
        let buf = build_insert(16385, &[text("1"), text("Alice"), ColumnValue::Null]);
        let msg = decode(&buf).unwrap();
        match msg {
            PgOutputMessage::Insert { relation_oid, new_tuple } => {
                assert_eq!(relation_oid, 16385);
                assert_eq!(new_tuple.columns.len(), 3);
                assert_eq!(new_tuple.columns[0], text("1"));
                assert_eq!(new_tuple.columns[1], text("Alice"));
                assert_eq!(new_tuple.columns[2], ColumnValue::Null);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn decode_update_simple() {
        let buf = build_update_simple(16385, &[text("1"), text("Bob")]);
        let msg = decode(&buf).unwrap();
        match msg {
            PgOutputMessage::Update {
                relation_oid,
                key_tuple,
                old_tuple,
                new_tuple,
            } => {
                assert_eq!(relation_oid, 16385);
                assert!(key_tuple.is_none());
                assert!(old_tuple.is_none());
                assert_eq!(new_tuple.columns.len(), 2);
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn decode_update_with_old_tuple() {
        let buf = build_update_with_old(
            16385,
            &[text("1"), text("Alice")],
            &[text("1"), text("Bob")],
        );
        let msg = decode(&buf).unwrap();
        match msg {
            PgOutputMessage::Update {
                old_tuple,
                new_tuple,
                ..
            } => {
                assert!(old_tuple.is_some());
                assert_eq!(new_tuple.columns[1], text("Bob"));
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn decode_delete_with_key() {
        let buf = build_delete_key(16385, &[text("1")]);
        let msg = decode(&buf).unwrap();
        match msg {
            PgOutputMessage::Delete {
                relation_oid,
                key_tuple,
                old_tuple,
            } => {
                assert_eq!(relation_oid, 16385);
                assert!(key_tuple.is_some());
                assert!(old_tuple.is_none());
                assert_eq!(key_tuple.unwrap().columns[0], text("1"));
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn decode_truncate() {
        let buf = build_truncate(&[16385, 16386], 1);
        let msg = decode(&buf).unwrap();
        match msg {
            PgOutputMessage::Truncate {
                option_bits,
                relation_oids,
            } => {
                assert_eq!(option_bits, 1);
                assert_eq!(relation_oids, vec![16385, 16386]);
            }
            _ => panic!("expected Truncate"),
        }
    }

    #[test]
    fn decode_unchanged_toast_in_tuple() {
        let buf = build_insert(
            16385,
            &[text("1"), ColumnValue::UnchangedToast, text("short")],
        );
        let msg = decode(&buf).unwrap();
        match msg {
            PgOutputMessage::Insert { new_tuple, .. } => {
                assert_eq!(new_tuple.columns[1], ColumnValue::UnchangedToast);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn decode_empty_message_errors() {
        assert_eq!(decode(&[]), Err(DecodeError::EmptyMessage));
    }

    #[test]
    fn decode_unknown_type_errors() {
        assert_eq!(decode(&[0xFF]), Err(DecodeError::UnknownMessageType(0xFF)));
    }

    #[test]
    fn decode_truncated_begin_errors() {
        // Begin needs 20 bytes after the type byte, only give 4
        let buf = vec![b'B', 0, 0, 0, 0];
        assert_eq!(decode(&buf), Err(DecodeError::UnexpectedEof));
    }

    #[test]
    fn lsn_roundtrip() {
        let lsn_str = "0/16B3748";
        let parsed = parse_lsn(lsn_str).unwrap();
        assert_eq!(parsed, 0x16B3748);
        assert_eq!(lsn_to_string(parsed), lsn_str);
    }

    #[test]
    fn lsn_high_part() {
        let parsed = parse_lsn("1/0").unwrap();
        assert_eq!(parsed, 0x1_0000_0000);
        assert_eq!(lsn_to_string(parsed), "1/0");
    }

    #[test]
    fn lsn_comparison() {
        let a = parse_lsn("0/16B3748").unwrap();
        let b = parse_lsn("0/16B3740").unwrap();
        assert!(a > b);

        let c = parse_lsn("1/0").unwrap();
        let d = parse_lsn("0/FFFFFFFF").unwrap();
        assert!(c > d);
    }

    #[test]
    fn cdc_op_as_str() {
        assert_eq!(CdcOp::Insert.as_str(), "insert");
        assert_eq!(CdcOp::Update.as_str(), "update");
        assert_eq!(CdcOp::Delete.as_str(), "delete");
    }
}
```

**Step 2: Register module in `cdc/mod.rs`**

Add `pub(crate) mod pgoutput;` at the top of `cdc/mod.rs` (alongside existing `mod parser;`).

**Step 3: Verify tests compile and pass**

```bash
cd connectors/source-postgres && cargo build
```

Tests can't run natively (wasm32 target), but they must compile. Verify with `cargo build --tests 2>&1 | grep error` (should be empty).

**Step 4: Commit**

```bash
git add connectors/source-postgres/src/cdc/pgoutput.rs connectors/source-postgres/src/cdc/mod.rs
git commit -m "feat(source-postgres): add pgoutput binary decoder with full message coverage"
```

---

### Task 2: Add `publication` Config Field

**Files:**
- Modify: `connectors/source-postgres/src/config.rs`

**Step 1: Write failing test**

Add to `config.rs` tests (or add a test module if none exists):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_accepts_publication_name() {
        let config = Config {
            host: "localhost".into(),
            port: 5432,
            user: "postgres".into(),
            password: "postgres".into(),
            database: "test".into(),
            replication_slot: None,
            publication: Some("my_pub".into()),
        };
        config.validate().unwrap();
    }

    #[test]
    fn validate_rejects_empty_publication() {
        let config = Config {
            host: "localhost".into(),
            port: 5432,
            user: "postgres".into(),
            password: "postgres".into(),
            database: "test".into(),
            replication_slot: None,
            publication: Some("".into()),
        };
        assert!(config.validate().is_err());
    }
}
```

**Step 2: Add the field and validation**

In `config.rs`, add to `Config`:

```rust
/// Publication name for CDC mode (pgoutput). If not set, defaults to `rapidbyte_{pipeline_name}`.
#[serde(default)]
pub publication: Option<String>,
```

In `Config::validate()`, add after the replication_slot check:

```rust
if let Some(pub_name) = self.publication.as_ref() {
    if pub_name.is_empty() {
        return Err(ConnectorError::config(
            "INVALID_CONFIG",
            "publication must not be empty".to_string(),
        ));
    }
    if pub_name.len() > 63 {
        return Err(ConnectorError::config(
            "INVALID_CONFIG",
            format!("publication '{pub_name}' exceeds PostgreSQL 63-byte limit"),
        ));
    }
}
```

**Step 3: Build and verify**

```bash
cd connectors/source-postgres && cargo build
```

**Step 4: Commit**

```bash
git add connectors/source-postgres/src/config.rs
git commit -m "feat(source-postgres): add publication config field for pgoutput CDC"
```

---

### Task 3: OID → Arrow Type Mapping

**Files:**
- Modify: `connectors/source-postgres/src/types.rs`

pgoutput Relation messages carry PostgreSQL type OIDs instead of type names. We need an `oid_to_arrow_type()` function.

**Step 1: Write failing tests**

Add to `types.rs` test module:

```rust
#[test]
fn oid_to_arrow_maps_common_types() {
    // int4 (OID 23) -> Int32
    assert_eq!(oid_to_arrow_type(23), ArrowDataType::Int32);
    // int8 (OID 20) -> Int64
    assert_eq!(oid_to_arrow_type(20), ArrowDataType::Int64);
    // text (OID 25) -> Utf8
    assert_eq!(oid_to_arrow_type(25), ArrowDataType::Utf8);
    // float8 (OID 701) -> Float64
    assert_eq!(oid_to_arrow_type(701), ArrowDataType::Float64);
    // bool (OID 16) -> Boolean
    assert_eq!(oid_to_arrow_type(16), ArrowDataType::Boolean);
    // timestamp (OID 1114) -> TimestampMicros
    assert_eq!(oid_to_arrow_type(1114), ArrowDataType::TimestampMicros);
    // unknown OID -> Utf8 fallback
    assert_eq!(oid_to_arrow_type(99999), ArrowDataType::Utf8);
}
```

**Step 2: Implement**

Add to `types.rs`:

```rust
/// Map a PostgreSQL type OID to an Arrow data type.
///
/// Common OIDs from `pg_type` system catalog. Unknown OIDs fall back to Utf8
/// since pgoutput text-format values are always valid UTF-8 strings.
pub(crate) fn oid_to_arrow_type(oid: u32) -> ArrowDataType {
    match oid {
        16 => ArrowDataType::Boolean,        // bool
        20 => ArrowDataType::Int64,          // int8
        21 => ArrowDataType::Int16,          // int2
        23 => ArrowDataType::Int32,          // int4
        25 => ArrowDataType::Utf8,           // text
        700 => ArrowDataType::Float32,       // float4
        701 => ArrowDataType::Float64,       // float8
        1043 => ArrowDataType::Utf8,         // varchar
        1082 => ArrowDataType::Date32,       // date
        1114 => ArrowDataType::TimestampMicros, // timestamp
        1184 => ArrowDataType::TimestampMicros, // timestamptz
        1700 => ArrowDataType::Utf8,         // numeric (stored as text)
        2950 => ArrowDataType::Utf8,         // uuid
        3802 => ArrowDataType::Utf8,         // jsonb
        114 => ArrowDataType::Utf8,          // json
        17 => ArrowDataType::Utf8,           // bytea (hex-encoded text)
        _ => ArrowDataType::Utf8,            // fallback
    }
}
```

**Step 3: Build**

```bash
cd connectors/source-postgres && cargo build
```

**Step 4: Commit**

```bash
git add connectors/source-postgres/src/types.rs
git commit -m "feat(source-postgres): add OID to Arrow type mapping for pgoutput CDC"
```

---

### Task 4: pgoutput CDC Arrow Encoder

**Files:**
- Create: `connectors/source-postgres/src/cdc/encode.rs`

This module converts pgoutput tuples + Relation metadata into typed Arrow RecordBatches.

**Step 1: Create the encoder**

```rust
//! Arrow batch encoding for pgoutput CDC changes.
//!
//! Converts parsed pgoutput tuples into typed Arrow `RecordBatch` using
//! Relation metadata for column names and type OIDs.

use std::collections::HashMap;
use std::sync::Arc;

use rapidbyte_sdk::arrow::array::{
    Array, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use rapidbyte_sdk::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use rapidbyte_sdk::arrow::record_batch::RecordBatch;
use rapidbyte_sdk::prelude::ArrowDataType;

use super::pgoutput::{CdcOp, ColumnDef, ColumnValue, TupleData};
use crate::types::oid_to_arrow_type;

/// Cached relation info used during batch encoding.
#[derive(Debug, Clone)]
pub(crate) struct RelationInfo {
    pub oid: u32,
    pub namespace: String,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub arrow_schema: Arc<Schema>,
}

impl RelationInfo {
    /// Build a `RelationInfo` from a pgoutput Relation message.
    pub fn new(oid: u32, namespace: String, name: String, columns: Vec<ColumnDef>) -> Self {
        let mut fields: Vec<Field> = columns
            .iter()
            .map(|col| {
                let arrow_type = oid_to_arrow_type(col.type_oid);
                let dt = match arrow_type {
                    ArrowDataType::TimestampMicros => {
                        DataType::Timestamp(TimeUnit::Microsecond, None)
                    }
                    other => other.into(),
                };
                Field::new(&col.name, dt, true)
            })
            .collect();
        // Add _rb_op metadata column
        fields.push(Field::new("_rb_op", DataType::Utf8, false));

        Self {
            oid,
            namespace,
            name,
            columns,
            arrow_schema: Arc::new(Schema::new(fields)),
        }
    }
}

/// A CDC change ready for Arrow encoding.
pub(crate) struct CdcRow {
    pub op: CdcOp,
    pub tuple: TupleData,
}

/// Encode accumulated CDC rows into an Arrow `RecordBatch`.
///
/// Uses the `RelationInfo` schema to produce typed columns instead of
/// all-Utf8. Values that fail type parsing (e.g., non-numeric text in an
/// Int32 column) are stored as null.
pub(crate) fn encode_cdc_batch(
    rows: &[CdcRow],
    relation: &RelationInfo,
) -> Result<RecordBatch, String> {
    let num_data_cols = relation.columns.len();
    let capacity = rows.len();

    // Build typed arrays for each column
    let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(num_data_cols + 1);

    for (col_idx, col_def) in relation.columns.iter().enumerate() {
        let arrow_type = oid_to_arrow_type(col_def.type_oid);
        let array = build_typed_array(rows, col_idx, arrow_type, capacity)?;
        arrays.push(array);
    }

    // _rb_op column (always Utf8)
    let mut op_builder = StringBuilder::with_capacity(capacity, capacity * 8);
    for row in rows {
        op_builder.append_value(row.op.as_str());
    }
    arrays.push(Arc::new(op_builder.finish()));

    RecordBatch::try_new(relation.arrow_schema.clone(), arrays)
        .map_err(|e| format!("failed to create CDC RecordBatch: {e}"))
}

/// Build a typed Arrow array from the column values across all rows.
fn build_typed_array(
    rows: &[CdcRow],
    col_idx: usize,
    arrow_type: ArrowDataType,
    capacity: usize,
) -> Result<Arc<dyn Array>, String> {
    match arrow_type {
        ArrowDataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(capacity);
            for row in rows {
                match row.tuple.columns.get(col_idx) {
                    Some(ColumnValue::Text(bytes)) => {
                        let s = std::str::from_utf8(bytes).unwrap_or("");
                        match s {
                            "t" | "true" => builder.append_value(true),
                            "f" | "false" => builder.append_value(false),
                            _ => builder.append_null(),
                        }
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::Int16 => build_int_array::<Int16Builder, i16>(rows, col_idx, capacity),
        ArrowDataType::Int32 => build_int_array::<Int32Builder, i32>(rows, col_idx, capacity),
        ArrowDataType::Int64 => build_int_array::<Int64Builder, i64>(rows, col_idx, capacity),
        ArrowDataType::Float32 => {
            let mut builder = Float32Builder::with_capacity(capacity);
            for row in rows {
                match row.tuple.columns.get(col_idx) {
                    Some(ColumnValue::Text(bytes)) => {
                        let s = std::str::from_utf8(bytes).unwrap_or("");
                        match s.parse::<f32>() {
                            Ok(v) => builder.append_value(v),
                            Err(_) => builder.append_null(),
                        }
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(capacity);
            for row in rows {
                match row.tuple.columns.get(col_idx) {
                    Some(ColumnValue::Text(bytes)) => {
                        let s = std::str::from_utf8(bytes).unwrap_or("");
                        match s.parse::<f64>() {
                            Ok(v) => builder.append_value(v),
                            Err(_) => builder.append_null(),
                        }
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::Date32 => {
            let mut builder = Date32Builder::with_capacity(capacity);
            for row in rows {
                match row.tuple.columns.get(col_idx) {
                    Some(ColumnValue::Text(bytes)) => {
                        let s = std::str::from_utf8(bytes).unwrap_or("");
                        // PG date format: "YYYY-MM-DD" → days since epoch
                        if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                            let days = (date - epoch).num_days() as i32;
                            builder.append_value(days);
                        } else {
                            builder.append_null();
                        }
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::TimestampMicros => {
            let mut builder = TimestampMicrosecondBuilder::with_capacity(capacity);
            for row in rows {
                match row.tuple.columns.get(col_idx) {
                    Some(ColumnValue::Text(bytes)) => {
                        let s = std::str::from_utf8(bytes).unwrap_or("");
                        // PG timestamp format: "YYYY-MM-DD HH:MM:SS.ffffff" or with timezone
                        if let Ok(dt) =
                            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                        {
                            builder.append_value(dt.and_utc().timestamp_micros());
                        } else if let Ok(dt) = chrono::DateTime::parse_from_str(
                            s,
                            "%Y-%m-%d %H:%M:%S%.f%z",
                        ) {
                            builder.append_value(dt.timestamp_micros());
                        } else {
                            builder.append_null();
                        }
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        // Utf8 fallback for text, varchar, numeric, uuid, json, jsonb, bytea, unknown
        _ => {
            let mut builder = StringBuilder::with_capacity(capacity, capacity * 32);
            for row in rows {
                match row.tuple.columns.get(col_idx) {
                    Some(ColumnValue::Text(bytes)) => {
                        let s = std::str::from_utf8(bytes).unwrap_or("");
                        builder.append_value(s);
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

/// Generic helper for integer types (Int16, Int32, Int64).
fn build_int_array<B, T>(
    rows: &[CdcRow],
    col_idx: usize,
    capacity: usize,
) -> Result<Arc<dyn Array>, String>
where
    B: Default,
    T: std::str::FromStr,
    B: rapidbyte_sdk::arrow::array::ArrayBuilder,
{
    // Use StringBuilder fallback for the generic case, specialized below
    let mut builder = StringBuilder::with_capacity(capacity, capacity * 16);
    for row in rows {
        match row.tuple.columns.get(col_idx) {
            Some(ColumnValue::Text(bytes)) => {
                let s = std::str::from_utf8(bytes).unwrap_or("");
                builder.append_value(s);
            }
            _ => builder.append_null(),
        }
    }
    // For now, store as Utf8 and let the destination cast. Typed builders
    // require more complex generic bounds. This can be optimized later.
    Ok(Arc::new(builder.finish()))
}
```

Wait — the generic approach is getting complex. Let me simplify. Since pgoutput text values are just strings, and we want typed Arrow columns, let me use dedicated builder logic per type instead of generics.

Actually, looking at this more carefully — the `build_int_array` generic function has issues with Arrow builder traits in the WASI environment. Let me simplify the entire approach:

**Revised step 1: Create `cdc/encode.rs` with direct typed builders**

```rust
//! Arrow batch encoding for pgoutput CDC changes.
//!
//! Converts parsed pgoutput tuples into typed Arrow `RecordBatch` using
//! Relation metadata for column names and type OIDs.

use std::sync::Arc;

use rapidbyte_sdk::arrow::array::{
    Array, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use rapidbyte_sdk::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use rapidbyte_sdk::arrow::record_batch::RecordBatch;
use rapidbyte_sdk::prelude::ArrowDataType;

use super::pgoutput::{CdcOp, ColumnDef, ColumnValue, TupleData};
use crate::types::oid_to_arrow_type;

/// Cached relation info used during batch encoding.
#[derive(Debug, Clone)]
pub(crate) struct RelationInfo {
    pub oid: u32,
    pub namespace: String,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub arrow_schema: Arc<Schema>,
}

impl RelationInfo {
    pub fn new(oid: u32, namespace: String, name: String, columns: Vec<ColumnDef>) -> Self {
        let mut fields: Vec<Field> = columns
            .iter()
            .map(|col| {
                let arrow_type = oid_to_arrow_type(col.type_oid);
                let dt = match arrow_type {
                    ArrowDataType::TimestampMicros => {
                        DataType::Timestamp(TimeUnit::Microsecond, None)
                    }
                    other => other.into(),
                };
                Field::new(&col.name, dt, true)
            })
            .collect();
        fields.push(Field::new("_rb_op", DataType::Utf8, false));
        Self {
            oid,
            namespace,
            name,
            columns,
            arrow_schema: Arc::new(Schema::new(fields)),
        }
    }
}

/// A CDC change ready for Arrow encoding.
pub(crate) struct CdcRow {
    pub op: CdcOp,
    pub tuple: TupleData,
}

/// Extract the text value from a column, or None if null/toast/missing.
fn col_text(tuple: &TupleData, idx: usize) -> Option<&str> {
    match tuple.columns.get(idx)? {
        ColumnValue::Text(bytes) => std::str::from_utf8(bytes).ok(),
        _ => None,
    }
}

/// Encode accumulated CDC rows into a typed Arrow `RecordBatch`.
pub(crate) fn encode_cdc_batch(
    rows: &[CdcRow],
    relation: &RelationInfo,
) -> Result<RecordBatch, String> {
    let cap = rows.len();
    let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(relation.columns.len() + 1);

    for (col_idx, col_def) in relation.columns.iter().enumerate() {
        let arrow_type = oid_to_arrow_type(col_def.type_oid);
        let array: Arc<dyn Array> = match arrow_type {
            ArrowDataType::Boolean => {
                let mut b = BooleanBuilder::with_capacity(cap);
                for row in rows {
                    match col_text(&row.tuple, col_idx) {
                        Some("t" | "true") => b.append_value(true),
                        Some("f" | "false") => b.append_value(false),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            ArrowDataType::Int16 => {
                let mut b = Int16Builder::with_capacity(cap);
                for row in rows {
                    match col_text(&row.tuple, col_idx).and_then(|s| s.parse().ok()) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            ArrowDataType::Int32 => {
                let mut b = Int32Builder::with_capacity(cap);
                for row in rows {
                    match col_text(&row.tuple, col_idx).and_then(|s| s.parse().ok()) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            ArrowDataType::Int64 => {
                let mut b = Int64Builder::with_capacity(cap);
                for row in rows {
                    match col_text(&row.tuple, col_idx).and_then(|s| s.parse().ok()) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            ArrowDataType::Float32 => {
                let mut b = Float32Builder::with_capacity(cap);
                for row in rows {
                    match col_text(&row.tuple, col_idx).and_then(|s| s.parse().ok()) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            ArrowDataType::Float64 => {
                let mut b = Float64Builder::with_capacity(cap);
                for row in rows {
                    match col_text(&row.tuple, col_idx).and_then(|s| s.parse().ok()) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            ArrowDataType::Date32 => {
                let mut b = Date32Builder::with_capacity(cap);
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                for row in rows {
                    match col_text(&row.tuple, col_idx)
                        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
                    {
                        Some(d) => b.append_value((d - epoch).num_days() as i32),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            ArrowDataType::TimestampMicros => {
                let mut b = TimestampMicrosecondBuilder::with_capacity(cap);
                for row in rows {
                    let s = col_text(&row.tuple, col_idx);
                    let micros = s.and_then(|s| {
                        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                            .ok()
                            .map(|dt| dt.and_utc().timestamp_micros())
                            .or_else(|| {
                                chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%z")
                                    .ok()
                                    .map(|dt| dt.timestamp_micros())
                            })
                    });
                    match micros {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            _ => {
                // Utf8 fallback
                let mut b = StringBuilder::with_capacity(cap, cap * 32);
                for row in rows {
                    match col_text(&row.tuple, col_idx) {
                        Some(s) => b.append_value(s),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
        };
        arrays.push(array);
    }

    // _rb_op column
    let mut op_builder = StringBuilder::with_capacity(cap, cap * 8);
    for row in rows {
        op_builder.append_value(row.op.as_str());
    }
    arrays.push(Arc::new(op_builder.finish()));

    RecordBatch::try_new(relation.arrow_schema.clone(), arrays)
        .map_err(|e| format!("failed to create CDC RecordBatch: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::pgoutput::{ColumnDef, ColumnValue, TupleData};

    fn text(s: &str) -> ColumnValue {
        ColumnValue::Text(s.as_bytes().to_vec())
    }

    fn make_relation() -> RelationInfo {
        RelationInfo::new(
            16385,
            "public".into(),
            "users".into(),
            vec![
                ColumnDef { flags: 1, name: "id".into(), type_oid: 23, type_modifier: -1 },
                ColumnDef { flags: 0, name: "name".into(), type_oid: 25, type_modifier: -1 },
                ColumnDef { flags: 0, name: "active".into(), type_oid: 16, type_modifier: -1 },
            ],
        )
    }

    #[test]
    fn encode_insert_row() {
        let rel = make_relation();
        let rows = vec![CdcRow {
            op: CdcOp::Insert,
            tuple: TupleData {
                columns: vec![text("1"), text("Alice"), text("t")],
            },
        }];
        let batch = encode_cdc_batch(&rows, &rel).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 4); // id, name, active, _rb_op
    }

    #[test]
    fn encode_null_and_toast() {
        let rel = make_relation();
        let rows = vec![CdcRow {
            op: CdcOp::Update,
            tuple: TupleData {
                columns: vec![text("2"), ColumnValue::Null, ColumnValue::UnchangedToast],
            },
        }];
        let batch = encode_cdc_batch(&rows, &rel).unwrap();
        assert_eq!(batch.num_rows(), 1);
        // null and unchanged toast should both produce null in Arrow
        assert!(batch.column(1).is_null(0));
        assert!(batch.column(2).is_null(0));
    }

    #[test]
    fn encode_multiple_rows() {
        let rel = make_relation();
        let rows = vec![
            CdcRow {
                op: CdcOp::Insert,
                tuple: TupleData { columns: vec![text("1"), text("Alice"), text("t")] },
            },
            CdcRow {
                op: CdcOp::Delete,
                tuple: TupleData { columns: vec![text("2"), ColumnValue::Null, ColumnValue::Null] },
            },
        ];
        let batch = encode_cdc_batch(&rows, &rel).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }
}
```

**Step 2: Register module**

Add `pub(crate) mod encode;` to `cdc/mod.rs`.

**Step 3: Build**

```bash
cd connectors/source-postgres && cargo build
```

**Step 4: Commit**

```bash
git add connectors/source-postgres/src/cdc/encode.rs connectors/source-postgres/src/cdc/mod.rs
git commit -m "feat(source-postgres): add pgoutput CDC Arrow encoder with typed columns"
```

---

### Task 5: Rewrite CDC Reader (`mod.rs`) to Use pgoutput

**Files:**
- Modify: `connectors/source-postgres/src/cdc/mod.rs` (major rewrite)

This is the core task. Replace the entire `read_cdc_changes()` function to use pgoutput binary protocol.

**Step 1: Rewrite `cdc/mod.rs`**

The new implementation:
1. Creates slot with `'pgoutput'` instead of `'test_decoding'`
2. Calls `pg_logical_slot_get_binary_changes()` with pgoutput options
3. Decodes binary messages via `pgoutput::decode()`
4. Caches Relation messages in `HashMap<u32, RelationInfo>`
5. Accumulates `CdcRow` structs per relation OID
6. Emits typed Arrow batches via `encode::encode_cdc_batch()`

Key changes to `ensure_replication_slot()`:
```rust
// OLD: pg_create_logical_replication_slot($1, 'test_decoding')
// NEW: pg_create_logical_replication_slot($1, 'pgoutput')
```

Key changes to the query:
```rust
// OLD: SELECT lsn::text, data FROM pg_logical_slot_get_changes($1, NULL, $2)
// NEW: SELECT lsn::text, data FROM pg_logical_slot_get_binary_changes(
//        $1, NULL, $2,
//        'proto_version', '1',
//        'publication_names', $3
//      )
```

Key changes to parsing loop:
```rust
// OLD: parse_change_line(&data) → CdcChange { op, table, columns: Vec<(String, String, String)> }
// NEW: pgoutput::decode(data_bytes) → PgOutputMessage enum
```

Here is the full new `cdc/mod.rs`:

```rust
//! Change Data Capture via PostgreSQL logical replication with pgoutput.
//!
//! Reads WAL changes through `pg_logical_slot_get_binary_changes()` with the
//! built-in `pgoutput` plugin, decodes binary messages, emits typed Arrow IPC
//! batches, and checkpoints by LSN.

mod encode;
pub(crate) mod pgoutput;

use std::collections::HashMap;
use std::time::Instant;

use tokio_postgres::Client;

use rapidbyte_sdk::prelude::*;

use crate::config::Config;
use crate::metrics::emit_read_metrics;

use encode::{encode_cdc_batch, CdcRow, RelationInfo};
use pgoutput::{CdcOp, PgOutputMessage};

/// Maximum number of change rows per Arrow `RecordBatch`.
const BATCH_SIZE: usize = 10_000;

/// Maximum WAL changes consumed per CDC invocation to avoid unbounded memory use.
/// Type is i32 because `pg_logical_slot_get_binary_changes()` expects int4.
const CDC_MAX_CHANGES: i32 = 100_000;

/// Default replication slot prefix. Full slot names are `rapidbyte_{stream_name}`.
const SLOT_PREFIX: &str = "rapidbyte_";

/// Default publication prefix.
const PUB_PREFIX: &str = "rapidbyte_";

struct CdcEmitState {
    total_records: u64,
    total_bytes: u64,
    batches_emitted: u64,
    arrow_encode_nanos: u64,
}

fn emit_batch(
    rows: &mut Vec<CdcRow>,
    relation: &RelationInfo,
    ctx: &Context,
    state: &mut CdcEmitState,
) -> Result<(), String> {
    let encode_start = Instant::now();
    let batch = encode_cdc_batch(rows, relation)?;
    #[allow(clippy::cast_possible_truncation)]
    {
        state.arrow_encode_nanos += encode_start.elapsed().as_nanos() as u64;
    }

    state.total_records += rows.len() as u64;
    state.total_bytes += batch.get_array_memory_size() as u64;
    state.batches_emitted += 1;

    ctx.emit_batch(&batch)
        .map_err(|e| format!("emit_batch failed: {}", e.message))?;
    emit_read_metrics(ctx, state.total_records, state.total_bytes);

    rows.clear();
    Ok(())
}

/// Read CDC changes from a logical replication slot using pgoutput.
#[allow(clippy::too_many_lines)]
pub async fn read_cdc_changes(
    client: &Client,
    ctx: &Context,
    stream: &StreamContext,
    config: &Config,
    connect_secs: f64,
) -> Result<ReadSummary, String> {
    ctx.log(
        LogLevel::Info,
        &format!("CDC reading stream: {}", stream.stream_name),
    );

    let query_start = Instant::now();

    // 1. Derive slot and publication names
    let slot_name = config
        .replication_slot
        .clone()
        .unwrap_or_else(|| format!("{SLOT_PREFIX}{}", stream.stream_name));

    let publication_name = config
        .publication
        .clone()
        .unwrap_or_else(|| format!("{PUB_PREFIX}{}", stream.stream_name));

    // 2. Ensure replication slot exists (idempotent)
    ensure_replication_slot(client, ctx, &slot_name).await?;

    // 3. Read binary changes from the slot
    let changes_query = concat!(
        "SELECT lsn::text, data ",
        "FROM pg_logical_slot_get_binary_changes(",
        "  $1, NULL, $2,",
        "  'proto_version', '1',",
        "  'publication_names', $3",
        ")"
    );
    let change_rows = client
        .query(changes_query, &[&slot_name, &CDC_MAX_CHANGES, &publication_name])
        .await
        .map_err(|e| {
            format!("pg_logical_slot_get_binary_changes failed for slot {slot_name}: {e}")
        })?;

    let query_secs = query_start.elapsed().as_secs_f64();

    let fetch_start = Instant::now();
    let mut state = CdcEmitState {
        total_records: 0,
        total_bytes: 0,
        batches_emitted: 0,
        arrow_encode_nanos: 0,
    };
    let mut max_lsn: Option<u64> = None;

    // 4. Relation cache: OID → RelationInfo
    let mut relations: HashMap<u32, RelationInfo> = HashMap::new();

    // Accumulated rows per stream (we only emit for matching stream)
    let mut accumulated_rows: Vec<CdcRow> = Vec::new();
    // Track which relation OID matches our target stream
    let mut target_oid: Option<u32> = None;

    // 5. Decode and process each binary message
    for row in &change_rows {
        let lsn_str: String = row.get(0);
        let data: &[u8] = row.get(1);

        // Track max LSN
        if let Some(lsn) = pgoutput::parse_lsn(&lsn_str) {
            if max_lsn.is_none_or(|current| lsn > current) {
                max_lsn = Some(lsn);
            }
        }

        let msg = match pgoutput::decode(data) {
            Ok(m) => m,
            Err(e) => {
                ctx.log(
                    LogLevel::Warn,
                    &format!("Skipping unparseable pgoutput message: {e}"),
                );
                continue;
            }
        };

        match msg {
            PgOutputMessage::Relation {
                oid,
                namespace,
                name,
                columns,
                ..
            } => {
                // Check if this relation matches our target stream
                let full_name = if namespace.is_empty() {
                    name.clone()
                } else {
                    format!("{namespace}.{name}")
                };
                if full_name == stream.stream_name
                    || name == stream.stream_name
                {
                    target_oid = Some(oid);
                }
                relations.insert(
                    oid,
                    RelationInfo::new(oid, namespace, name, columns),
                );
            }
            PgOutputMessage::Insert {
                relation_oid,
                new_tuple,
            } if Some(relation_oid) == target_oid => {
                accumulated_rows.push(CdcRow {
                    op: CdcOp::Insert,
                    tuple: new_tuple,
                });
            }
            PgOutputMessage::Update {
                relation_oid,
                new_tuple,
                ..
            } if Some(relation_oid) == target_oid => {
                accumulated_rows.push(CdcRow {
                    op: CdcOp::Update,
                    tuple: new_tuple,
                });
            }
            PgOutputMessage::Delete {
                relation_oid,
                key_tuple,
                old_tuple,
            } if Some(relation_oid) == target_oid => {
                // Use old_tuple (REPLICA IDENTITY FULL) or key_tuple (DEFAULT)
                let tuple = old_tuple.or(key_tuple).unwrap_or_else(|| {
                    pgoutput::TupleData { columns: Vec::new() }
                });
                accumulated_rows.push(CdcRow {
                    op: CdcOp::Delete,
                    tuple,
                });
            }
            // Skip Begin, Commit, Truncate, Origin, Type, Message, non-matching DML
            _ => {}
        }

        // Flush batch if accumulated enough
        if accumulated_rows.len() >= BATCH_SIZE {
            if let Some(oid) = target_oid {
                if let Some(rel) = relations.get(&oid) {
                    emit_batch(&mut accumulated_rows, rel, ctx, &mut state)?;
                }
            }
        }
    }

    // Flush remaining
    if !accumulated_rows.is_empty() {
        if let Some(oid) = target_oid {
            if let Some(rel) = relations.get(&oid) {
                emit_batch(&mut accumulated_rows, rel, ctx, &mut state)?;
            }
        }
    }

    let fetch_secs = fetch_start.elapsed().as_secs_f64();

    // 6. Emit checkpoint with max LSN
    let checkpoint_count = if let Some(lsn) = max_lsn {
        let lsn_str = pgoutput::lsn_to_string(lsn);
        let cp = Checkpoint {
            id: 1,
            kind: CheckpointKind::Source,
            stream: stream.stream_name.clone(),
            cursor_field: Some("lsn".to_string()),
            cursor_value: Some(CursorValue::Lsn { value: lsn_str.clone() }),
            records_processed: state.total_records,
            bytes_processed: state.total_bytes,
        };
        ctx.checkpoint(&cp).map_err(|e| {
            format!(
                "CDC checkpoint failed (WAL already consumed): {}",
                e.message
            )
        })?;
        ctx.log(
            LogLevel::Info,
            &format!("CDC checkpoint: stream={} lsn={lsn_str}", stream.stream_name),
        );
        1u64
    } else {
        ctx.log(
            LogLevel::Info,
            &format!("CDC: no new changes for stream '{}'", stream.stream_name),
        );
        0u64
    };

    ctx.log(
        LogLevel::Info,
        &format!(
            "CDC stream '{}' complete: {} records, {} bytes, {} batches",
            stream.stream_name, state.total_records, state.total_bytes, state.batches_emitted
        ),
    );

    #[allow(clippy::cast_precision_loss)]
    let arrow_encode_secs = state.arrow_encode_nanos as f64 / 1e9;

    Ok(ReadSummary {
        records_read: state.total_records,
        bytes_read: state.total_bytes,
        batches_emitted: state.batches_emitted,
        checkpoint_count,
        records_skipped: 0,
        perf: Some(ReadPerf {
            connect_secs,
            query_secs,
            fetch_secs,
            arrow_encode_secs,
        }),
    })
}

/// Ensure the logical replication slot exists with pgoutput plugin.
async fn ensure_replication_slot(
    client: &Client,
    ctx: &Context,
    slot_name: &str,
) -> Result<(), String> {
    ctx.log(
        LogLevel::Debug,
        &format!("Ensuring replication slot '{slot_name}' exists (pgoutput)"),
    );

    let result = client
        .query_one(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot_name],
        )
        .await;

    match result {
        Ok(_) => {
            ctx.log(
                LogLevel::Info,
                &format!("Created replication slot '{slot_name}' with pgoutput"),
            );
        }
        Err(e) => {
            let is_duplicate = e
                .as_db_error()
                .is_some_and(|db| db.code().code() == "42710");

            if is_duplicate {
                ctx.log(
                    LogLevel::Debug,
                    &format!("Replication slot '{slot_name}' already exists"),
                );
            } else {
                return Err(format!(
                    "Failed to create logical replication slot '{slot_name}'. \
                     Ensure wal_level=logical in postgresql.conf: {e}"
                ));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::encode::{CdcRow, RelationInfo};
    use super::pgoutput::{CdcOp, ColumnDef, ColumnValue, TupleData};

    #[test]
    fn relation_info_schema_includes_rb_op() {
        let rel = RelationInfo::new(
            1,
            "public".into(),
            "users".into(),
            vec![
                ColumnDef { flags: 1, name: "id".into(), type_oid: 23, type_modifier: -1 },
                ColumnDef { flags: 0, name: "name".into(), type_oid: 25, type_modifier: -1 },
            ],
        );
        assert_eq!(rel.arrow_schema.fields().len(), 3); // id, name, _rb_op
        assert_eq!(rel.arrow_schema.field(2).name(), "_rb_op");
        assert!(!rel.arrow_schema.field(2).is_nullable());
    }
}
```

**Step 2: Delete old parser**

Delete `connectors/source-postgres/src/cdc/parser.rs` entirely.

**Step 3: Build and verify**

```bash
cd connectors/source-postgres && cargo build && cargo clippy
```

**Step 4: Commit**

```bash
git add connectors/source-postgres/src/cdc/
git rm connectors/source-postgres/src/cdc/parser.rs
git commit -m "feat(source-postgres): replace test_decoding with pgoutput CDC

BREAKING: CDC now uses the built-in pgoutput output plugin instead of
test_decoding. This enables CDC on managed PostgreSQL services (RDS,
Aurora, Cloud SQL, Supabase) without custom extensions.

Key changes:
- Binary pgoutput decoder (cdc/pgoutput.rs)
- Typed Arrow columns from OID metadata (cdc/encode.rs)
- Publication-based server-side table filtering
- Removed test_decoding parser entirely"
```

---

### Task 6: Update E2E Test Fixtures for pgoutput

**Files:**
- Modify: `tests/connectors/postgres/fixtures/sql/source_seed.sql`
- Modify: `tests/connectors/postgres/fixtures/pipelines/e2e_cdc.yaml`
- Modify: `tests/connectors/postgres/scenarios/15_cdc.sh`

**Step 1: Add publication creation to SQL seed**

In `source_seed.sql`, add at the end:

```sql
-- Publication for CDC (pgoutput requires a publication)
-- Drop first to make script idempotent on re-runs.
DROP PUBLICATION IF EXISTS rapidbyte_users;
CREATE PUBLICATION rapidbyte_users FOR TABLE users;
```

**Step 2: Update pipeline YAML**

In `e2e_cdc.yaml`, add `publication` to the source config:

```yaml
source:
  use: source-postgres
  config:
    host: localhost
    port: 5433
    user: postgres
    password: postgres
    database: rapidbyte_test
    publication: rapidbyte_users
  streams:
    - name: users
      sync_mode: cdc
```

**Step 3: Update E2E scenario**

In `15_cdc.sh`, update the slot cleanup to handle the slot migration from test_decoding to pgoutput. The slot needs to be recreated if it was previously created with test_decoding:

```bash
# Drop replication slot if leftover from a previous test run
# (slot may have been created with test_decoding in older versions)
pg_cmd "SELECT pg_drop_replication_slot('rapidbyte_users') FROM pg_replication_slots WHERE slot_name = 'rapidbyte_users';" 2>/dev/null || true
```

This line is already there and works. No changes needed. The rest of the test should work as-is since:
- The `_rb_op` column is still emitted
- The operations (insert/update/delete) use the same string values
- The data integrity checks work the same way

The only behavioral difference: with pgoutput, column types are now typed (Int32 for `id` instead of Utf8). The destination `dest-postgres` should handle this, but verify the E2E test still passes.

**Step 4: Build and run E2E**

```bash
just build && just build-connectors
just e2e postgres
```

If the E2E fails because the destination expects Utf8 columns but gets Int32, the dest connector's write path may need to handle typed CDC columns. Check the error and adjust.

**Step 5: Commit**

```bash
git add tests/connectors/postgres/fixtures/ tests/connectors/postgres/scenarios/15_cdc.sh
git commit -m "test(e2e): update CDC fixtures for pgoutput (add publication, typed columns)"
```

---

### Task 7: Update Connector Metadata and Docs

**Files:**
- Modify: `connectors/source-postgres/src/main.rs` (doc comment)
- Modify: `IDEA.md` (if CDC section references test_decoding)

**Step 1: Update doc comments**

In `main.rs`, update the module doc:

```rust
//! Source connector for `PostgreSQL`.
//!
//! Implements discovery and read paths (full-refresh, incremental cursor reads,
//! and CDC logical replication via pgoutput) and streams Arrow IPC batches to the host.
```

**Step 2: Update IDEA.md**

Search for any references to `test_decoding` in `IDEA.md` and replace with `pgoutput`. Update the CDC section to reflect the new architecture.

**Step 3: Commit**

```bash
git add connectors/source-postgres/src/main.rs IDEA.md
git commit -m "docs: update CDC references from test_decoding to pgoutput"
```

---

### Task 8: Final Build + Clippy + E2E Verification

**Step 1: Full workspace build and test**

```bash
cargo test --workspace
cd connectors/source-postgres && cargo build && cargo clippy
cd connectors/dest-postgres && cargo build
```

**Step 2: E2E test**

```bash
just e2e postgres
```

Specifically watch the CDC scenario (`15_cdc.sh`) output.

**Step 3: Manual dry-run test**

```bash
# Create a CDC pipeline YAML pointing to your local PG
# with publication and sync_mode: cdc
RAPIDBYTE_CONNECTOR_DIR=target/connectors \
  ./target/debug/rapidbyte run /tmp/cdc-test.yaml --dry-run
```

**Step 4: Commit (if any fixes needed)**

```bash
git commit -m "fix: address build/test issues from pgoutput migration"
```

---

## Summary of Changes

| File | Action | Description |
|------|--------|-------------|
| `cdc/pgoutput.rs` | **CREATE** | pgoutput binary decoder (all message types, ~400 lines) |
| `cdc/encode.rs` | **CREATE** | Typed Arrow encoder for pgoutput tuples (~250 lines) |
| `cdc/parser.rs` | **DELETE** | Remove test_decoding text parser |
| `cdc/mod.rs` | **REWRITE** | Use pgoutput instead of test_decoding |
| `config.rs` | **MODIFY** | Add `publication` field |
| `types.rs` | **MODIFY** | Add `oid_to_arrow_type()` |
| `main.rs` | **MODIFY** | Update doc comment |
| `source_seed.sql` | **MODIFY** | Add `CREATE PUBLICATION` |
| `e2e_cdc.yaml` | **MODIFY** | Add `publication` config |
| `15_cdc.sh` | **MODIFY** | Minor updates for pgoutput behavior |
| `IDEA.md` | **MODIFY** | Update CDC references |

**Total: ~800 new lines, ~400 removed lines, 8 tasks**
