/// Validate that `name` is a legal PostgreSQL identifier.
///
/// Rules:
/// - Must not be empty.
/// - Must start with a letter (`a-zA-Z`) or underscore (`_`).
/// - Remaining characters must be letters, digits (`0-9`), or underscores.
/// - Must not contain dots, spaces, quotes, semicolons, or other special characters.
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

    // First character must be a letter or underscore.
    let first = chars.next().unwrap(); // safe — checked non-empty above
    if !first.is_ascii_alphabetic() && first != '_' {
        return Err(format!(
            "identifier must start with a letter or underscore, got '{}'",
            first
        ));
    }

    // Remaining characters must be letters, digits, or underscores.
    for ch in chars {
        if !ch.is_ascii_alphanumeric() && ch != '_' {
            return Err(format!(
                "identifier contains invalid character '{}'",
                ch
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_identifiers() {
        assert!(validate_pg_identifier("users").is_ok());
        assert!(validate_pg_identifier("_private").is_ok());
        assert!(validate_pg_identifier("table_123").is_ok());
    }

    #[test]
    fn test_empty_identifier() {
        let err = validate_pg_identifier("").unwrap_err();
        assert!(err.contains("empty"), "error was: {}", err);
    }

    #[test]
    fn test_starts_with_digit() {
        let err = validate_pg_identifier("123abc").unwrap_err();
        assert!(err.contains("start with"), "error was: {}", err);
    }

    #[test]
    fn test_contains_space() {
        let err = validate_pg_identifier("table name").unwrap_err();
        assert!(err.contains("invalid character"), "error was: {}", err);
    }

    #[test]
    fn test_contains_semicolon() {
        let err = validate_pg_identifier("table;DROP").unwrap_err();
        assert!(err.contains("invalid character"), "error was: {}", err);
    }

    #[test]
    fn test_contains_quote() {
        let err = validate_pg_identifier("tab\"le").unwrap_err();
        assert!(err.contains("invalid character"), "error was: {}", err);
    }

    #[test]
    fn test_contains_dot() {
        let err = validate_pg_identifier("schema.table").unwrap_err();
        assert!(err.contains("invalid character"), "error was: {}", err);
    }

    #[test]
    fn test_identifier_length_limit() {
        let long_name = "a".repeat(63);
        assert!(validate_pg_identifier(&long_name).is_ok());
        let too_long = "a".repeat(64);
        assert!(validate_pg_identifier(&too_long).is_err());
    }
}
