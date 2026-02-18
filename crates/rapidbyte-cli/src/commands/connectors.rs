use anyhow::Result;
use rapidbyte_core::runtime::wasm_runtime;

/// Execute the `connectors` command: list available connectors with manifest info.
pub async fn execute() -> Result<()> {
    let dirs = connector_dirs();

    if dirs.is_empty() {
        println!("No connector directories found.");
        println!("Set RAPIDBYTE_CONNECTOR_DIR or place connectors in ~/.rapidbyte/plugins/");
        return Ok(());
    }

    let mut found = false;

    for dir in &dirs {
        if !dir.exists() {
            continue;
        }

        let entries = std::fs::read_dir(dir)?;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.extension().map(|e| e == "wasm").unwrap_or(false) {
                found = true;
                match wasm_runtime::load_connector_manifest(&path)? {
                    Some(manifest) => {
                        let roles: Vec<String> = manifest
                            .roles
                            .iter()
                            .map(|r| format!("{:?}", r))
                            .collect();
                        let features: Vec<String> = manifest
                            .features
                            .iter()
                            .map(|f| format!("{:?}", f))
                            .collect();
                        println!(
                            "  {}@{}  {}  [{}]",
                            manifest.id,
                            manifest.version,
                            roles.join(", "),
                            if features.is_empty() {
                                "no features".to_string()
                            } else {
                                features.join(", ")
                            },
                        );
                        if !manifest.description.is_empty() {
                            println!("    {}", manifest.description);
                        }
                    }
                    None => {
                        let name = path.file_stem().unwrap_or_default().to_string_lossy();
                        println!("  {}  (no manifest)", name);
                    }
                }
            }
        }
    }

    if !found {
        println!("No connectors found.");
        println!("Place .wasm files in ~/.rapidbyte/plugins/ or set RAPIDBYTE_CONNECTOR_DIR");
    }

    Ok(())
}

fn connector_dirs() -> Vec<std::path::PathBuf> {
    let mut dirs = Vec::new();

    if let Ok(dir) = std::env::var("RAPIDBYTE_CONNECTOR_DIR") {
        dirs.push(std::path::PathBuf::from(dir));
    }

    if let Ok(home) = std::env::var("HOME") {
        dirs.push(
            std::path::PathBuf::from(home)
                .join(".rapidbyte")
                .join("plugins"),
        );
    }

    dirs
}
