use proptest::prelude::*;
use rapidbyte_engine::config::parser;
use rapidbyte_engine::config::validator;

proptest! {
    #[test]
    fn upsert_requires_primary_key(pk_len in 0_usize..3) {
        let primary_key_yaml = if pk_len == 0 {
            "[]".to_string()
        } else {
            "[id]".to_string()
        };

        let yaml = format!(
            r#"
version: "1.0"
pipeline: prop_upsert_policy
source:
  use: source-postgres
  config:
    host: localhost
    port: 5432
    user: postgres
    password: postgres
    database: postgres
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: dest-postgres
  config:
    host: localhost
    port: 5432
    user: postgres
    password: postgres
    database: postgres
    schema: raw
  write_mode: upsert
  primary_key: {primary_key_yaml}
state:
  backend: sqlite
"#
        );

        let config = parser::parse_pipeline_str(&yaml).expect("generated yaml must parse");
        let result = validator::validate_pipeline(&config);

        if pk_len == 0 {
            prop_assert!(result.is_err());
        } else {
            prop_assert!(result.is_ok());
        }
    }

    #[test]
    fn incremental_requires_cursor_field(has_cursor in any::<bool>()) {
        let cursor_line = if has_cursor {
            "\n      cursor_field: id"
        } else {
            ""
        };

        let yaml = format!(
            r#"
version: "1.0"
pipeline: prop_incremental_policy
source:
  use: source-postgres
  config:
    host: localhost
    port: 5432
    user: postgres
    password: postgres
    database: postgres
  streams:
    - name: users
      sync_mode: incremental{cursor_line}
destination:
  use: dest-postgres
  config:
    host: localhost
    port: 5432
    user: postgres
    password: postgres
    database: postgres
    schema: raw
  write_mode: append
state:
  backend: sqlite
"#
        );

        let config = parser::parse_pipeline_str(&yaml).expect("generated yaml must parse");
        let result = validator::validate_pipeline(&config);

        if has_cursor {
            prop_assert!(result.is_ok());
        } else {
            prop_assert!(result.is_err());
        }
    }
}
