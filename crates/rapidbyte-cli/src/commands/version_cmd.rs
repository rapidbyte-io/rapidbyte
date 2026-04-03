//! `rapidbyte version` — print version information.

/// Print the binary version to stdout.
pub fn execute() {
    println!("rapidbyte {}", env!("CARGO_PKG_VERSION"));
}

#[cfg(test)]
mod tests {
    #[test]
    fn execute_does_not_panic() {
        // Smoke-test: just ensure the function completes without panicking.
        super::execute();
    }
}
