use std::collections::BTreeMap;

use regex::Regex;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub rules: Vec<RuleSpec>,
}

impl rapidbyte_sdk::ConfigSchema for Config {
    const SCHEMA_JSON: &'static str = r##"{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Transform Validate Config",
  "description": "Rule-based data contract validation for in-flight Arrow batches",
  "type": "object",
  "required": ["rules"],
  "properties": {
    "rules": {
      "type": "array",
      "minItems": 1,
      "items": {
        "type": "object",
        "oneOf": [
          {
            "required": ["assert_not_null"],
            "properties": {
              "assert_not_null": {
                "oneOf": [
                  { "$ref": "#/definitions/non_empty_field" },
                  {
                    "type": "array",
                    "minItems": 1,
                    "items": { "$ref": "#/definitions/non_empty_field" }
                  }
                ]
              }
            },
            "additionalProperties": false
          },
          {
            "required": ["assert_unique"],
            "properties": {
              "assert_unique": {
                "oneOf": [
                  { "$ref": "#/definitions/non_empty_field" },
                  {
                    "type": "array",
                    "minItems": 1,
                    "items": { "$ref": "#/definitions/non_empty_field" }
                  }
                ]
              }
            },
            "additionalProperties": false
          },
          {
            "required": ["assert_regex"],
            "properties": {
              "assert_regex": {
                "oneOf": [
                  {
                    "type": "object",
                    "required": ["field", "pattern"],
                    "properties": {
                      "field": { "$ref": "#/definitions/non_empty_field" },
                      "pattern": { "type": "string", "minLength": 1 }
                    },
                    "additionalProperties": false
                  },
                  {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                      "type": "object",
                      "required": ["field", "pattern"],
                      "properties": {
                        "field": { "$ref": "#/definitions/non_empty_field" },
                        "pattern": { "type": "string", "minLength": 1 }
                      },
                      "additionalProperties": false
                    }
                  },
                  {
                    "type": "object",
                    "minProperties": 1,
                    "additionalProperties": { "type": "string", "minLength": 1 }
                  }
                ]
              }
            },
            "additionalProperties": false
          },
          {
            "required": ["assert_range"],
            "properties": {
              "assert_range": {
                "oneOf": [
                  { "$ref": "#/definitions/range_rule" },
                  {
                    "type": "array",
                    "minItems": 1,
                    "items": { "$ref": "#/definitions/range_rule" }
                  }
                ]
              }
            },
            "additionalProperties": false
          }
        ]
      }
    }
  },
  "additionalProperties": false,
  "definitions": {
    "non_empty_field": {
      "type": "string",
      "minLength": 1,
      "pattern": "\\S"
    },
    "range_rule": {
      "type": "object",
      "required": ["field"],
      "properties": {
        "field": { "$ref": "#/definitions/non_empty_field" },
        "min": { "type": "number" },
        "max": { "type": "number" }
      },
      "additionalProperties": false
    }
  }
}"##;
}

#[derive(Debug, Clone)]
pub struct CompiledConfig {
    pub rules: Vec<CompiledRule>,
}

#[derive(Debug, Clone)]
pub enum CompiledRule {
    NotNull { field: String },
    Regex { field: String, pattern: String, regex: Regex },
    Range {
        field: String,
        min: Option<f64>,
        max: Option<f64>,
    },
    Unique { field: String },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum RuleSpec {
    NotNull { assert_not_null: FieldSelector },
    Regex { assert_regex: RegexSelector },
    Range { assert_range: RangeSelector },
    Unique { assert_unique: FieldSelector },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum FieldSelector {
    One(String),
    Many(Vec<String>),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum RegexSelector {
    One(RegexRule),
    Many(Vec<RegexRule>),
    Map(BTreeMap<String, String>),
}

#[derive(Debug, Clone, Deserialize)]
pub struct RegexRule {
    pub field: String,
    pub pattern: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum RangeSelector {
    One(RangeRule),
    Many(Vec<RangeRule>),
}

#[derive(Debug, Clone, Deserialize)]
pub struct RangeRule {
    pub field: String,
    pub min: Option<f64>,
    pub max: Option<f64>,
}

impl Config {
    pub fn compile(&self) -> Result<CompiledConfig, String> {
        if self.rules.is_empty() {
            return Err("rules must not be empty".to_string());
        }

        let mut compiled = Vec::new();
        for rule in &self.rules {
            match rule {
                RuleSpec::NotNull { assert_not_null } => {
                    for field in assert_not_null.fields() {
                        ensure_non_empty_field(field)?;
                        compiled.push(CompiledRule::NotNull {
                            field: field.to_string(),
                        });
                    }
                }
                RuleSpec::Unique { assert_unique } => {
                    for field in assert_unique.fields() {
                        ensure_non_empty_field(field)?;
                        compiled.push(CompiledRule::Unique {
                            field: field.to_string(),
                        });
                    }
                }
                RuleSpec::Regex { assert_regex } => {
                    for regex_rule in assert_regex.rules() {
                        ensure_non_empty_field(&regex_rule.field)?;
                        if regex_rule.pattern.trim().is_empty() {
                            return Err(format!(
                                "regex pattern for field '{}' must not be empty",
                                regex_rule.field
                            ));
                        }
                        let regex = Regex::new(&regex_rule.pattern).map_err(|e| {
                            format!(
                                "invalid regex pattern for field '{}': {e}",
                                regex_rule.field
                            )
                        })?;
                        compiled.push(CompiledRule::Regex {
                            field: regex_rule.field.clone(),
                            pattern: regex_rule.pattern.clone(),
                            regex,
                        });
                    }
                }
                RuleSpec::Range { assert_range } => {
                    for range_rule in assert_range.rules() {
                        ensure_non_empty_field(&range_rule.field)?;
                        if range_rule.min.is_none() && range_rule.max.is_none() {
                            return Err(format!(
                                "range rule for field '{}' must set min and/or max",
                                range_rule.field
                            ));
                        }
                        if let (Some(min), Some(max)) = (range_rule.min, range_rule.max) {
                            if min > max {
                                return Err(format!(
                                    "range rule for field '{}' has min > max ({min} > {max})",
                                    range_rule.field
                                ));
                            }
                        }
                        compiled.push(CompiledRule::Range {
                            field: range_rule.field.clone(),
                            min: range_rule.min,
                            max: range_rule.max,
                        });
                    }
                }
            }
        }

        if compiled.is_empty() {
            return Err("rules must produce at least one assertion".to_string());
        }

        Ok(CompiledConfig { rules: compiled })
    }
}

fn ensure_non_empty_field(field: &str) -> Result<(), String> {
    if field.trim().is_empty() {
        Err("field name must not be empty".to_string())
    } else {
        Ok(())
    }
}

impl FieldSelector {
    fn fields(&self) -> Vec<&str> {
        match self {
            Self::One(field) => vec![field],
            Self::Many(fields) => fields.iter().map(String::as_str).collect(),
        }
    }
}

impl RegexSelector {
    fn rules(&self) -> Vec<RegexRule> {
        match self {
            Self::One(rule) => vec![rule.clone()],
            Self::Many(rules) => rules.clone(),
            Self::Map(map) => map
                .iter()
                .map(|(field, pattern)| RegexRule {
                    field: field.clone(),
                    pattern: pattern.clone(),
                })
                .collect(),
        }
    }
}

impl RangeSelector {
    fn rules(&self) -> Vec<RangeRule> {
        match self {
            Self::One(rule) => vec![rule.clone()],
            Self::Many(rules) => rules.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_sdk::ConfigSchema;

    #[test]
    fn compile_supports_repeat_and_shorthand_forms() {
        let config: Config = serde_json::from_value(serde_json::json!({
            "rules": [
                { "assert_not_null": "user_id" },
                { "assert_not_null": ["email", "account_id"] },
                { "assert_unique": ["order_id", "external_id"] },
                { "assert_regex": { "email": "^.+@.+$", "phone": "^[0-9-]+$" } },
                { "assert_range": [
                    { "field": "age", "min": 0, "max": 150 },
                    { "field": "score", "min": 0 }
                ]}
            ]
        }))
        .expect("config should deserialize");

        let compiled = config.compile().expect("config should compile");
        assert_eq!(compiled.rules.len(), 9);
    }

    #[test]
    fn compile_rejects_invalid_regex_and_bounds() {
        let regex_bad: Config = serde_json::from_value(serde_json::json!({
            "rules": [{ "assert_regex": { "field": "email", "pattern": "(" } }]
        }))
        .expect("config should deserialize");
        assert!(regex_bad.compile().is_err());

        let bounds_bad: Config = serde_json::from_value(serde_json::json!({
            "rules": [{ "assert_range": { "field": "age", "min": 10, "max": 1 } }]
        }))
        .expect("config should deserialize");
        assert!(bounds_bad.compile().is_err());
    }

    #[test]
    fn schema_declares_no_additional_properties() {
        let schema: serde_json::Value =
            serde_json::from_str(Config::SCHEMA_JSON).expect("schema should be valid json");
        assert_eq!(schema["additionalProperties"], serde_json::json!(false));
    }

    #[test]
    fn schema_rejects_empty_rules_array() {
        let bad: Result<Config, _> = serde_json::from_value(serde_json::json!({
            "rules": []
        }));
        assert!(bad.is_ok(), "serde shape is valid; compile enforces semantics");
        let config = bad.expect("config should deserialize");
        assert!(config.compile().is_err());
    }
}
