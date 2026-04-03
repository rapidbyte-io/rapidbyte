//! `rapidbyte version` — print version information.

/// Print the binary version to stderr.
pub fn execute() {
    eprintln!("rapidbyte {}", env!("CARGO_PKG_VERSION"));
}
