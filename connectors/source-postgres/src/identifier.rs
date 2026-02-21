/// Connector-local PostgreSQL identifier validation.
pub fn validate_pg_identifier(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("identifier must not be empty".to_string());
    }

    if name.len() > 63 {
        return Err(format!(
            "Identifier '{}' exceeds PostgreSQL maximum length of 63 bytes (got {})",
            name,
            name.len()
        ));
    }

    let mut chars = name.chars();
    let first = chars.next().expect("non-empty identifier");
    if !first.is_ascii_alphabetic() && first != '_' {
        return Err(format!(
            "identifier must start with a letter or underscore, got '{}'",
            first
        ));
    }

    for ch in chars {
        if !ch.is_ascii_alphanumeric() && ch != '_' {
            return Err(format!("identifier contains invalid character '{}'", ch));
        }
    }

    Ok(())
}
