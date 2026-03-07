//! Connector listing subcommand (connectors).

use anyhow::Result;

/// A discovered connector entry for columnar display.
struct ConnectorEntry {
    name: String,
    role: String,
    description: String,
}

/// Execute the `connectors` command: list available connectors with manifest info.
///
/// # Errors
///
/// Returns `Err` if directory scanning or manifest parsing fails.
pub fn execute(verbosity: crate::Verbosity) -> Result<()> {
    if verbosity == crate::Verbosity::Quiet {
        return Ok(());
    }

    let dirs = rapidbyte_runtime::connector_search_dirs();

    if dirs.is_empty() {
        eprintln!("  No connectors found.");
        eprintln!("  Place .wasm files in ~/.rapidbyte/plugins/ or set RAPIDBYTE_CONNECTOR_DIR");
        return Ok(());
    }

    let mut entries: Vec<ConnectorEntry> = Vec::new();

    for dir in &dirs {
        let dir_entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        for entry in dir_entries {
            let entry = entry?;
            let path = entry.path();

            if path.extension().is_some_and(|e| e == "wasm") {
                if let Some(manifest) = rapidbyte_runtime::load_connector_manifest(&path)? {
                    let role = if manifest.roles.source.is_some() {
                        "source"
                    } else if manifest.roles.destination.is_some() {
                        "destination"
                    } else if manifest.roles.transform.is_some() {
                        "transform"
                    } else {
                        "unknown"
                    };

                    entries.push(ConnectorEntry {
                        name: manifest.name.clone(),
                        role: role.to_owned(),
                        description: manifest.description.clone(),
                    });
                } else {
                    let name = path.file_stem().unwrap_or_default().to_string_lossy();
                    entries.push(ConnectorEntry {
                        name: name.into_owned(),
                        role: String::from("unknown"),
                        description: String::from("(no manifest)"),
                    });
                }
            }
        }
    }

    if entries.is_empty() {
        eprintln!("  No connectors found.");
        eprintln!("  Place .wasm files in ~/.rapidbyte/plugins/ or set RAPIDBYTE_CONNECTOR_DIR");
        return Ok(());
    }

    // Compute column widths for aligned output.
    let name_width = entries.iter().map(|e| e.name.len()).max().unwrap_or(0);
    let role_width = entries.iter().map(|e| e.role.len()).max().unwrap_or(0);

    for e in &entries {
        eprintln!(
            "  {:<name_width$}  {:<role_width$}  {}",
            e.name, e.role, e.description,
        );
    }

    Ok(())
}
