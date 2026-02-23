//! Proc macros for the Rapidbyte Connector SDK.
//!
//! This crate is an internal implementation detail of `rapidbyte-sdk`.
//! Do not depend on it directly — use `rapidbyte_sdk::ConfigSchema` instead.

mod schema;

use proc_macro::TokenStream;

/// Derive a JSON Schema (Draft 7) from a config struct at compile time.
///
/// Generates `impl ConfigSchema for T` with `const SCHEMA_JSON: &'static str`.
///
/// # Supported `#[schema(...)]` attributes
///
/// - `secret` — marks field as sensitive (`"x-secret": true`)
/// - `default = <value>` — sets JSON Schema default
/// - `advanced` — marks as advanced setting (`"x-advanced": true`)
/// - `example = <value>` — adds to `"examples"` array
/// - `env = "<VAR>"` — documents env var fallback (`"x-env-var"`)
/// - `values("<a>", "<b>")` — string enum constraint
#[proc_macro_derive(ConfigSchema, attributes(schema))]
pub fn derive_config_schema(input: TokenStream) -> TokenStream {
    schema::expand(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}
