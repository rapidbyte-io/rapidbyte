use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::Parse;
use syn::{Data, DeriveInput, Expr, Fields, Lit, Meta, Result, Type};

pub fn expand(input: TokenStream) -> Result<TokenStream> {
    let input: DeriveInput = syn::parse2(input)?;
    let name = &input.ident;

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(f) => &f.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    name,
                    "ConfigSchema requires a struct with named fields",
                ))
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                name,
                "ConfigSchema can only be derived for structs",
            ))
        }
    };

    let mut properties = serde_json::Map::new();
    let mut required = Vec::<serde_json::Value>::new();

    for field in fields {
        let field_name = field.ident.as_ref().unwrap().to_string();
        let mut prop = serde_json::Map::new();

        // Doc comments -> description
        let description = extract_doc_comment(&field.attrs);
        if let Some(desc) = description {
            prop.insert("description".into(), serde_json::Value::String(desc));
        }

        // Type analysis
        let (is_optional, inner_type_name) = analyze_type(&field.ty);
        prop.insert(
            "type".into(),
            serde_json::Value::String(rust_type_to_json_schema(&inner_type_name)),
        );

        if !is_optional {
            required.push(serde_json::Value::String(field_name.clone()));
        }

        // #[schema(...)] attributes
        parse_schema_attrs(&field.attrs, &mut prop)?;

        properties.insert(field_name, serde_json::Value::Object(prop));
    }

    let mut schema = serde_json::Map::new();
    schema.insert(
        "$schema".into(),
        serde_json::Value::String("http://json-schema.org/draft-07/schema#".into()),
    );
    schema.insert("type".into(), serde_json::Value::String("object".into()));
    schema.insert("properties".into(), serde_json::Value::Object(properties));
    if !required.is_empty() {
        schema.insert("required".into(), serde_json::Value::Array(required));
    }

    let schema_json = serde_json::to_string(&serde_json::Value::Object(schema))
        .expect("internal: schema serialization cannot fail");

    Ok(quote! {
        impl ::rapidbyte_sdk::ConfigSchema for #name {
            const SCHEMA_JSON: &'static str = #schema_json;
        }
    })
}

// -- Helpers ------------------------------------------------------------------

fn extract_doc_comment(attrs: &[syn::Attribute]) -> Option<String> {
    let lines: Vec<String> = attrs
        .iter()
        .filter(|a| a.path().is_ident("doc"))
        .filter_map(|a| match &a.meta {
            Meta::NameValue(nv) => match &nv.value {
                Expr::Lit(expr_lit) => match &expr_lit.lit {
                    Lit::Str(s) => Some(s.value().trim().to_string()),
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        })
        .filter(|s| !s.is_empty())
        .collect();

    if lines.is_empty() {
        None
    } else {
        Some(lines.join(" "))
    }
}

/// Returns `(is_optional, inner_type_name)`.
fn analyze_type(ty: &Type) -> (bool, String) {
    if let Type::Path(tp) = ty {
        if let Some(seg) = tp.path.segments.last() {
            if seg.ident == "Option" {
                if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
                    if let Some(syn::GenericArgument::Type(inner)) = args.args.first() {
                        return (true, type_ident(inner));
                    }
                }
            }
            return (false, seg.ident.to_string());
        }
    }
    (false, "unknown".to_string())
}

fn type_ident(ty: &Type) -> String {
    if let Type::Path(tp) = ty {
        if let Some(seg) = tp.path.segments.last() {
            return seg.ident.to_string();
        }
    }
    "unknown".to_string()
}

fn rust_type_to_json_schema(ty: &str) -> String {
    match ty {
        "String" | "str" => "string",
        "bool" => "boolean",
        "f32" | "f64" => "number",
        "u8" | "u16" | "u32" | "u64" | "u128" | "usize" | "i8" | "i16" | "i32" | "i64" | "i128"
        | "isize" => "integer",
        _ => "string", // custom types fall back to string
    }
    .to_string()
}

fn parse_schema_attrs(
    attrs: &[syn::Attribute],
    prop: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<()> {
    for attr in attrs {
        if !attr.path().is_ident("schema") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("secret") {
                prop.insert("x-secret".into(), serde_json::Value::Bool(true));
            } else if meta.path.is_ident("advanced") {
                prop.insert("x-advanced".into(), serde_json::Value::Bool(true));
            } else if meta.path.is_ident("default") {
                let _eq: syn::Token![=] = meta.input.parse()?;
                let lit: Lit = meta.input.parse()?;
                prop.insert("default".into(), lit_to_json(&lit));
            } else if meta.path.is_ident("example") {
                let _eq: syn::Token![=] = meta.input.parse()?;
                let lit: Lit = meta.input.parse()?;
                let arr = prop
                    .entry("examples")
                    .or_insert_with(|| serde_json::Value::Array(vec![]));
                if let serde_json::Value::Array(a) = arr {
                    a.push(lit_to_json(&lit));
                }
            } else if meta.path.is_ident("env") {
                let _eq: syn::Token![=] = meta.input.parse()?;
                let lit: syn::LitStr = meta.input.parse()?;
                prop.insert("x-env-var".into(), serde_json::Value::String(lit.value()));
            } else if meta.path.is_ident("values") {
                let content;
                syn::parenthesized!(content in meta.input);
                let vals = content.parse_terminated(Lit::parse, syn::Token![,])?;
                let enum_vals: Vec<serde_json::Value> = vals.iter().map(lit_to_json).collect();
                prop.insert("enum".into(), serde_json::Value::Array(enum_vals));
            } else {
                return Err(meta.error(format!(
                    "unknown schema attribute: `{}`",
                    meta.path
                        .get_ident()
                        .map(|i| i.to_string())
                        .unwrap_or_default()
                )));
            }
            Ok(())
        })?;
    }
    Ok(())
}

fn lit_to_json(lit: &Lit) -> serde_json::Value {
    match lit {
        Lit::Str(s) => serde_json::Value::String(s.value()),
        Lit::Int(i) => serde_json::Value::Number(serde_json::Number::from(
            i.base10_parse::<i64>().expect("integer literal"),
        )),
        Lit::Float(f) => {
            serde_json::json!(f.base10_parse::<f64>().expect("float literal"))
        }
        Lit::Bool(b) => serde_json::Value::Bool(b.value()),
        _ => serde_json::Value::Null,
    }
}
