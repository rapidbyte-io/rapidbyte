//! Arrow <-> PostgreSQL type mapping helpers.

use arrow::datatypes::DataType;

/// Map Arrow data types back to PostgreSQL column types.
pub(crate) fn arrow_to_pg_type(dt: &DataType) -> &'static str {
    match dt {
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::Float32 => "REAL",
        DataType::Float64 => "DOUBLE PRECISION",
        DataType::Boolean => "BOOLEAN",
        DataType::Utf8 => "TEXT",
        _ => "TEXT",
    }
}

/// Check if an information_schema type and DDL type refer to the same type.
pub(crate) fn pg_types_compatible(info_schema_type: &str, ddl_type: &str) -> bool {
    let a = info_schema_type.to_lowercase();
    let b = ddl_type.to_lowercase();

    let norm_a = match a.as_str() {
        "int" | "int4" | "integer" => "integer",
        "int2" | "smallint" => "smallint",
        "int8" | "bigint" => "bigint",
        "float4" | "real" => "real",
        "float8" | "double precision" => "double precision",
        "bool" | "boolean" => "boolean",
        "varchar" | "character varying" | "text" => "text",
        "timestamp without time zone" | "timestamp" => "timestamp",
        "timestamp with time zone" | "timestamptz" => "timestamptz",
        other => other,
    };
    let norm_b = match b.as_str() {
        "int" | "int4" | "integer" => "integer",
        "int2" | "smallint" => "smallint",
        "int8" | "bigint" => "bigint",
        "float4" | "real" => "real",
        "float8" | "double precision" => "double precision",
        "bool" | "boolean" => "boolean",
        "varchar" | "character varying" | "text" => "text",
        "timestamp without time zone" | "timestamp" => "timestamp",
        "timestamp with time zone" | "timestamptz" => "timestamptz",
        other => other,
    };
    norm_a == norm_b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_types_compatible_normalizes_aliases() {
        assert!(pg_types_compatible("integer", "INT4"));
        assert!(pg_types_compatible("character varying", "TEXT"));
        assert!(pg_types_compatible("timestamp with time zone", "timestamptz"));
        assert!(!pg_types_compatible("bigint", "text"));
    }
}
