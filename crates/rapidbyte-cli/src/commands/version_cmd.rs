//! `rapidbyte version` — print version information.

/// Print the binary version to stdout.
pub fn execute() {
    println!("rapidbyte {}", env!("CARGO_PKG_VERSION"));
}
