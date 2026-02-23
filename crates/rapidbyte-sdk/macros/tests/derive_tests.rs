#![allow(dead_code)]

use rapidbyte_sdk::ConfigSchema;
use serde::Deserialize;
use serde_json::Value;

// -- Basic functionality ------------------------------------------------------

#[derive(Deserialize, ConfigSchema)]
struct BasicConfig {
    /// Database hostname
    pub host: String,
    /// Database port
    #[schema(default = 5432)]
    pub port: u16,
    pub user: String,
    #[schema(secret)]
    pub password: Option<String>,
    pub database: String,
}

#[test]
fn generates_valid_json_schema() {
    let schema: Value = serde_json::from_str(BasicConfig::SCHEMA_JSON).unwrap();
    assert_eq!(schema["$schema"], "http://json-schema.org/draft-07/schema#");
    assert_eq!(schema["type"], "object");
}

#[test]
fn non_option_fields_are_required() {
    let schema: Value = serde_json::from_str(BasicConfig::SCHEMA_JSON).unwrap();
    let required: Vec<&str> = schema["required"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(required.contains(&"host"));
    assert!(required.contains(&"port"));
    assert!(required.contains(&"user"));
    assert!(required.contains(&"database"));
    assert!(!required.contains(&"password"), "Option<T> must not be required");
}

#[test]
fn maps_rust_types_to_json_schema_types() {
    let schema: Value = serde_json::from_str(BasicConfig::SCHEMA_JSON).unwrap();
    assert_eq!(schema["properties"]["host"]["type"], "string");
    assert_eq!(schema["properties"]["port"]["type"], "integer");
}

#[test]
fn doc_comments_become_description() {
    let schema: Value = serde_json::from_str(BasicConfig::SCHEMA_JSON).unwrap();
    assert_eq!(schema["properties"]["host"]["description"], "Database hostname");
    assert_eq!(schema["properties"]["port"]["description"], "Database port");
}

#[test]
fn schema_default_attribute() {
    let schema: Value = serde_json::from_str(BasicConfig::SCHEMA_JSON).unwrap();
    assert_eq!(schema["properties"]["port"]["default"], 5432);
}

#[test]
fn schema_secret_attribute() {
    let schema: Value = serde_json::from_str(BasicConfig::SCHEMA_JSON).unwrap();
    assert_eq!(schema["properties"]["password"]["x-secret"], true);
}

// -- Extended attributes ------------------------------------------------------

#[derive(Deserialize, ConfigSchema)]
struct ExtendedConfig {
    pub host: String,
    #[schema(advanced)]
    pub debug_mode: Option<bool>,
    #[schema(example = "pg_main")]
    pub slot_name: Option<String>,
    #[schema(env = "PG_HOST")]
    pub pg_host: String,
    #[schema(values("insert", "copy"))]
    pub load_method: Option<String>,
    #[schema(default = "public")]
    pub target_schema: String,
}

#[test]
fn schema_advanced_attribute() {
    let schema: Value = serde_json::from_str(ExtendedConfig::SCHEMA_JSON).unwrap();
    assert_eq!(schema["properties"]["debug_mode"]["x-advanced"], true);
}

#[test]
fn schema_example_attribute() {
    let schema: Value = serde_json::from_str(ExtendedConfig::SCHEMA_JSON).unwrap();
    let examples = schema["properties"]["slot_name"]["examples"].as_array().unwrap();
    assert_eq!(examples, &[Value::String("pg_main".into())]);
}

#[test]
fn schema_env_attribute() {
    let schema: Value = serde_json::from_str(ExtendedConfig::SCHEMA_JSON).unwrap();
    assert_eq!(schema["properties"]["pg_host"]["x-env-var"], "PG_HOST");
}

#[test]
fn schema_values_attribute() {
    let schema: Value = serde_json::from_str(ExtendedConfig::SCHEMA_JSON).unwrap();
    let vals = schema["properties"]["load_method"]["enum"].as_array().unwrap();
    assert_eq!(vals, &[Value::String("insert".into()), Value::String("copy".into())]);
}

#[test]
fn schema_string_default() {
    let schema: Value = serde_json::from_str(ExtendedConfig::SCHEMA_JSON).unwrap();
    assert_eq!(schema["properties"]["target_schema"]["default"], "public");
}

#[test]
fn bool_maps_to_boolean() {
    let schema: Value = serde_json::from_str(ExtendedConfig::SCHEMA_JSON).unwrap();
    assert_eq!(schema["properties"]["debug_mode"]["type"], "boolean");
}

// -- Numeric types ------------------------------------------------------------

#[derive(Deserialize, ConfigSchema)]
struct NumericConfig {
    pub count: u32,
    pub offset: i64,
    pub ratio: f64,
    pub size: usize,
    pub enabled: bool,
}

#[test]
fn all_numeric_types_map_correctly() {
    let schema: Value = serde_json::from_str(NumericConfig::SCHEMA_JSON).unwrap();
    assert_eq!(schema["properties"]["count"]["type"], "integer");
    assert_eq!(schema["properties"]["offset"]["type"], "integer");
    assert_eq!(schema["properties"]["ratio"]["type"], "number");
    assert_eq!(schema["properties"]["size"]["type"], "integer");
    assert_eq!(schema["properties"]["enabled"]["type"], "boolean");
}

// -- Combined attributes ------------------------------------------------------

#[derive(Deserialize, ConfigSchema)]
struct CombinedConfig {
    /// Secret with default
    #[schema(secret, default = "changeme")]
    pub api_key: String,
}

#[test]
fn multiple_attributes_on_one_field() {
    let schema: Value = serde_json::from_str(CombinedConfig::SCHEMA_JSON).unwrap();
    let field = &schema["properties"]["api_key"];
    assert_eq!(field["x-secret"], true);
    assert_eq!(field["default"], "changeme");
    assert_eq!(field["description"], "Secret with default");
}
