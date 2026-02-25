//! Unified `PostgreSQL` -> Arrow type registry.
//!
//! Single source of truth for type mapping, text-cast decisions,
//! and column metadata. Every downstream module works with `Column`.

use rapidbyte_sdk::prelude::*;

/// Resolved type information for a `PostgreSQL` column type.
pub struct TypeInfo {
    /// Target Arrow data type.
    pub arrow_type: ArrowDataType,
    /// Whether the SELECT should cast this column to text (`col::text AS col`).
    pub needs_cast: bool,
}

/// Column metadata carrying both PG origin and Arrow target info.
///
/// Built during schema resolution and threaded through the entire pipeline:
/// query building, Arrow encoding, cursor tracking.
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub pg_type: String,
    pub arrow_type: ArrowDataType,
    pub nullable: bool,
    pub needs_cast: bool,
}

impl Column {
    /// Build a `Column` from a PG type name, resolving Arrow type and cast requirement.
    #[must_use] 
    pub fn new(name: &str, pg_type: &str, nullable: bool) -> Self {
        let info = resolve(pg_type);
        Self {
            name: name.to_string(),
            pg_type: pg_type.to_string(),
            arrow_type: info.arrow_type,
            nullable,
            needs_cast: info.needs_cast,
        }
    }

    /// Whether this column is json or jsonb (needs special Arrow encoding).
    #[must_use] 
    pub fn is_json(&self) -> bool {
        self.pg_type == "json" || self.pg_type == "jsonb"
    }

    /// Convert to SDK `ColumnSchema` for catalog discovery.
    #[must_use] 
    pub fn to_schema(&self) -> ColumnSchema {
        ColumnSchema {
            name: self.name.clone(),
            data_type: self.arrow_type,
            nullable: self.nullable,
        }
    }
}

/// Resolve a `PostgreSQL` type name to its Arrow type and cast requirement.
#[must_use] 
pub fn resolve(pg_type: &str) -> TypeInfo {
    match pg_type {
        // Integer types -- native FromSql
        "smallint" | "int2" | "smallserial" => TypeInfo {
            arrow_type: ArrowDataType::Int16,
            needs_cast: false,
        },
        "integer" | "int4" | "serial" => TypeInfo {
            arrow_type: ArrowDataType::Int32,
            needs_cast: false,
        },
        "bigint" | "int8" | "bigserial" => TypeInfo {
            arrow_type: ArrowDataType::Int64,
            needs_cast: false,
        },

        // Float types -- native FromSql
        "real" | "float4" => TypeInfo {
            arrow_type: ArrowDataType::Float32,
            needs_cast: false,
        },
        "double precision" | "float8" => TypeInfo {
            arrow_type: ArrowDataType::Float64,
            needs_cast: false,
        },

        // Boolean -- native FromSql
        "boolean" | "bool" => TypeInfo {
            arrow_type: ArrowDataType::Boolean,
            needs_cast: false,
        },

        // Text types -- native FromSql, no cast
        "text" | "varchar" | "character varying" | "char" | "character" | "bpchar" | "name" => {
            TypeInfo {
                arrow_type: ArrowDataType::Utf8,
                needs_cast: false,
            }
        }

        // Timestamps -- native FromSql (NaiveDateTime / DateTime<Utc>)
        "timestamp"
        | "timestamp without time zone"
        | "timestamp with time zone"
        | "timestamptz" => TypeInfo {
            arrow_type: ArrowDataType::TimestampMicros,
            needs_cast: false,
        },

        // Date -- native FromSql (NaiveDate)
        "date" => TypeInfo {
            arrow_type: ArrowDataType::Date32,
            needs_cast: false,
        },

        // Binary -- native FromSql
        "bytea" => TypeInfo {
            arrow_type: ArrowDataType::Binary,
            needs_cast: false,
        },

        // JSON -- native FromSql via serde_json::Value, no text cast
        "json" | "jsonb" => TypeInfo {
            arrow_type: ArrowDataType::Utf8,
            needs_cast: false,
        },

        // Types that need text cast -- no native FromSql in tokio-postgres
        "uuid"
        | "numeric"
        | "decimal"
        | "money"
        | "time"
        | "time without time zone"
        | "time with time zone"
        | "timetz"
        | "interval"
        | "inet"
        | "cidr"
        | "macaddr"
        | "macaddr8"
        | "bit"
        | "bit varying"
        | "varbit"
        | "xml"
        | "pg_lsn"
        | "tsquery"
        | "tsvector"
        | "point"
        | "line"
        | "lseg"
        | "box"
        | "path"
        | "polygon"
        | "circle" => TypeInfo {
            arrow_type: ArrowDataType::Utf8,
            needs_cast: true,
        },

        // Array types (any[]) -- cast to text representation
        t if t.starts_with('_') || t.ends_with("[]") => TypeInfo {
            arrow_type: ArrowDataType::Utf8,
            needs_cast: true,
        },

        // Unknown / custom types -- safe fallback
        _ => TypeInfo {
            arrow_type: ArrowDataType::Utf8,
            needs_cast: true,
        },
    }
}

/// Map a `PostgreSQL` type OID to an Arrow data type.
///
/// Common OIDs from `pg_type` system catalog. Unknown OIDs fall back to `Utf8`
/// since pgoutput text-format values are always valid UTF-8 strings.
#[must_use]
pub(crate) fn oid_to_arrow_type(oid: u32) -> ArrowDataType {
    match oid {
        16 => ArrowDataType::Boolean,                  // bool
        20 => ArrowDataType::Int64,                    // int8
        21 => ArrowDataType::Int16,                    // int2
        23 => ArrowDataType::Int32,                    // int4
        700 => ArrowDataType::Float32,                 // float4
        701 => ArrowDataType::Float64,                 // float8
        1082 => ArrowDataType::Date32,                 // date
        1114 | 1184 => ArrowDataType::TimestampMicros, // timestamp / timestamptz
        // text, varchar, numeric, uuid, jsonb, json, bytea — all Utf8
        _ => ArrowDataType::Utf8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_types_resolve_correctly() {
        for (pg, expected) in [
            ("int2", ArrowDataType::Int16),
            ("smallint", ArrowDataType::Int16),
            ("smallserial", ArrowDataType::Int16),
            ("int4", ArrowDataType::Int32),
            ("integer", ArrowDataType::Int32),
            ("serial", ArrowDataType::Int32),
            ("int8", ArrowDataType::Int64),
            ("bigint", ArrowDataType::Int64),
            ("bigserial", ArrowDataType::Int64),
        ] {
            let info = resolve(pg);
            assert_eq!(info.arrow_type, expected, "arrow_type mismatch for {pg}");
            assert!(!info.needs_cast, "integer type {pg} should not need cast");
        }
    }

    #[test]
    fn float_types_resolve_correctly() {
        for (pg, expected) in [
            ("real", ArrowDataType::Float32),
            ("float4", ArrowDataType::Float32),
            ("double precision", ArrowDataType::Float64),
            ("float8", ArrowDataType::Float64),
        ] {
            let info = resolve(pg);
            assert_eq!(info.arrow_type, expected, "arrow_type mismatch for {pg}");
            assert!(!info.needs_cast, "float type {pg} should not need cast");
        }
    }

    #[test]
    fn text_types_need_no_cast() {
        for pg in [
            "text",
            "varchar",
            "character varying",
            "char",
            "character",
            "bpchar",
            "name",
        ] {
            let info = resolve(pg);
            assert_eq!(
                info.arrow_type,
                ArrowDataType::Utf8,
                "text type {pg} should be Utf8"
            );
            assert!(!info.needs_cast, "text type {pg} should not need cast");
        }
    }

    #[test]
    fn timestamp_types_resolve_to_micros() {
        for pg in [
            "timestamp",
            "timestamp without time zone",
            "timestamp with time zone",
            "timestamptz",
        ] {
            let info = resolve(pg);
            assert_eq!(
                info.arrow_type,
                ArrowDataType::TimestampMicros,
                "timestamp type {pg} should be TimestampMicros"
            );
            assert!(!info.needs_cast, "timestamp type {pg} should not need cast");
        }
    }

    #[test]
    fn types_needing_text_cast() {
        for pg in [
            "uuid",
            "numeric",
            "decimal",
            "money",
            "time",
            "time without time zone",
            "time with time zone",
            "timetz",
            "interval",
            "inet",
            "cidr",
            "macaddr",
            "macaddr8",
        ] {
            let info = resolve(pg);
            assert_eq!(
                info.arrow_type,
                ArrowDataType::Utf8,
                "cast type {pg} should be Utf8"
            );
            assert!(info.needs_cast, "{pg} should need text cast");
        }
    }

    #[test]
    fn json_types_no_cast() {
        for pg in ["json", "jsonb"] {
            let info = resolve(pg);
            assert_eq!(
                info.arrow_type,
                ArrowDataType::Utf8,
                "json type {pg} should be Utf8"
            );
            assert!(!info.needs_cast, "json type {pg} should not need cast");
        }
    }

    #[test]
    fn boolean_and_binary() {
        let info = resolve("boolean");
        assert_eq!(info.arrow_type, ArrowDataType::Boolean);
        assert!(!info.needs_cast);

        let info = resolve("bool");
        assert_eq!(info.arrow_type, ArrowDataType::Boolean);
        assert!(!info.needs_cast);

        let info = resolve("bytea");
        assert_eq!(info.arrow_type, ArrowDataType::Binary);
        assert!(!info.needs_cast);
    }

    #[test]
    fn date_resolves_to_date32() {
        let info = resolve("date");
        assert_eq!(info.arrow_type, ArrowDataType::Date32);
        assert!(!info.needs_cast);
    }

    #[test]
    fn unknown_types_fall_back_to_utf8_with_cast() {
        for pg in ["hstore", "my_custom_enum", "ltree"] {
            let info = resolve(pg);
            assert_eq!(
                info.arrow_type,
                ArrowDataType::Utf8,
                "unknown type {pg} should fall back to Utf8"
            );
            assert!(info.needs_cast, "unknown type {pg} should need cast");
        }
    }

    #[test]
    fn array_types_need_cast() {
        // Internal PG array prefix notation
        let info = resolve("_int4");
        assert_eq!(info.arrow_type, ArrowDataType::Utf8);
        assert!(info.needs_cast);

        // User-facing bracket notation
        let info = resolve("integer[]");
        assert_eq!(info.arrow_type, ArrowDataType::Utf8);
        assert!(info.needs_cast);
    }

    #[test]
    fn column_from_pg_row() {
        let col = Column::new("id", "serial", false);
        assert_eq!(col.name, "id");
        assert_eq!(col.pg_type, "serial");
        assert_eq!(col.arrow_type, ArrowDataType::Int32);
        assert!(!col.nullable);
        assert!(!col.needs_cast);

        let col = Column::new("external_id", "uuid", true);
        assert_eq!(col.arrow_type, ArrowDataType::Utf8);
        assert!(col.nullable);
        assert!(col.needs_cast);
    }

    #[test]
    fn is_json_helper() {
        assert!(Column::new("data", "json", true).is_json());
        assert!(Column::new("data", "jsonb", true).is_json());
        assert!(!Column::new("data", "text", true).is_json());
        assert!(!Column::new("data", "uuid", true).is_json());
    }

    #[test]
    fn column_to_schema_conversion() {
        let col = Column::new("id", "bigint", false);
        let schema = col.to_schema();
        assert_eq!(schema.name, "id");
        assert_eq!(schema.data_type, ArrowDataType::Int64);
        assert!(!schema.nullable);
    }

    #[test]
    fn geometric_types_need_cast() {
        for pg in ["point", "line", "lseg", "box", "path", "polygon", "circle"] {
            let info = resolve(pg);
            assert_eq!(
                info.arrow_type,
                ArrowDataType::Utf8,
                "geometric type {pg} should be Utf8"
            );
            assert!(info.needs_cast, "geometric type {pg} should need cast");
        }
    }

    #[test]
    fn search_and_bit_types_need_cast() {
        for pg in [
            "tsquery",
            "tsvector",
            "bit",
            "bit varying",
            "varbit",
            "xml",
            "pg_lsn",
        ] {
            let info = resolve(pg);
            assert_eq!(
                info.arrow_type,
                ArrowDataType::Utf8,
                "type {pg} should be Utf8"
            );
            assert!(info.needs_cast, "type {pg} should need cast");
        }
    }

    #[test]
    fn oid_to_arrow_maps_common_types() {
        assert_eq!(oid_to_arrow_type(23), ArrowDataType::Int32); // int4
        assert_eq!(oid_to_arrow_type(20), ArrowDataType::Int64); // int8
        assert_eq!(oid_to_arrow_type(25), ArrowDataType::Utf8); // text
        assert_eq!(oid_to_arrow_type(701), ArrowDataType::Float64); // float8
        assert_eq!(oid_to_arrow_type(16), ArrowDataType::Boolean); // bool
        assert_eq!(oid_to_arrow_type(1114), ArrowDataType::TimestampMicros); // timestamp
        assert_eq!(oid_to_arrow_type(99999), ArrowDataType::Utf8); // unknown -> fallback
    }
}
