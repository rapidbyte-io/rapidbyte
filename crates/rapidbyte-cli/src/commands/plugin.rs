//! Plugin management subcommands (pull, push, inspect, tags, list, remove).

use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use rapidbyte_registry::{artifact, PluginCache, PluginRef, RegistryClient, RegistryConfig};

/// Execute a plugin subcommand.
///
/// # Errors
///
/// Returns `Err` on network, cache, or I/O failures.
pub async fn execute(command: crate::PluginCommands) -> Result<()> {
    match command {
        crate::PluginCommands::Pull {
            plugin_ref,
            insecure,
        } => pull(&plugin_ref, insecure).await,
        crate::PluginCommands::Push {
            plugin_ref,
            wasm_path,
            insecure,
        } => push(&plugin_ref, &wasm_path, insecure).await,
        crate::PluginCommands::Inspect {
            plugin_ref,
            insecure,
        } => inspect(&plugin_ref, insecure).await,
        crate::PluginCommands::Tags {
            plugin_ref,
            insecure,
        } => tags(&plugin_ref, insecure).await,
        crate::PluginCommands::List => list(),
        crate::PluginCommands::Remove { plugin_ref } => remove(&plugin_ref),
    }
}

/// Format a byte count as a human-readable string (e.g. "1.2 MiB").
#[allow(clippy::cast_precision_loss)]
fn format_size(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;

    if bytes >= GIB {
        format!("{:.1} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}

async fn pull(plugin_ref_str: &str, insecure: bool) -> Result<()> {
    let plugin_ref = PluginRef::parse(plugin_ref_str).context("Invalid plugin reference")?;
    let cache = PluginCache::default_location()?;

    // Check cache first.
    if let Some(entry) = cache.lookup(&plugin_ref) {
        eprintln!(
            "Plugin {plugin_ref} already cached ({})",
            format_size(entry.size_bytes),
        );
        return Ok(());
    }

    let config = RegistryConfig {
        insecure,
        credentials: None,
    };
    let client = RegistryClient::new(&config)?;

    eprintln!("Pulling {plugin_ref}...");
    let image = client.pull(&plugin_ref).await?;

    let (manifest_json, wasm_bytes) =
        artifact::unpack_artifact(&image).context("Failed to unpack plugin artifact")?;

    let entry = cache
        .store(&plugin_ref, &manifest_json, &wasm_bytes)
        .context("Failed to store plugin in cache")?;

    eprintln!(
        "Pulled {plugin_ref} ({}, sha256:{})",
        format_size(entry.size_bytes),
        &entry.digest[..12],
    );

    Ok(())
}

async fn push(plugin_ref_str: &str, wasm_path: &PathBuf, insecure: bool) -> Result<()> {
    let plugin_ref = PluginRef::parse(plugin_ref_str).context("Invalid plugin reference")?;

    let wasm_bytes = std::fs::read(wasm_path)
        .with_context(|| format!("Failed to read WASM file: {}", wasm_path.display()))?;

    let manifest = rapidbyte_runtime::extract_manifest_from_wasm(&wasm_bytes)
        .context("WASM file does not contain a valid rapidbyte plugin manifest")?;

    let manifest_json =
        serde_json::to_vec_pretty(&manifest).context("Failed to serialize plugin manifest")?;

    let packed = artifact::pack_artifact(&manifest_json, &wasm_bytes);

    let config = RegistryConfig {
        insecure,
        credentials: None,
    };
    let client = RegistryClient::new(&config)?;

    eprintln!("Pushing {plugin_ref}...");
    let url = client
        .push(&plugin_ref, packed.layers, packed.config)
        .await?;

    eprintln!("Pushed {plugin_ref} ({url})");

    Ok(())
}

async fn inspect(plugin_ref_str: &str, insecure: bool) -> Result<()> {
    let plugin_ref = PluginRef::parse(plugin_ref_str).context("Invalid plugin reference")?;

    // Try cache first.
    let cache = PluginCache::default_location()?;
    if let Some(entry) = cache.lookup(&plugin_ref) {
        let manifest_bytes = std::fs::read(&entry.manifest_path).with_context(|| {
            format!(
                "Failed to read cached manifest: {}",
                entry.manifest_path.display()
            )
        })?;
        let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes)
            .context("Failed to parse cached manifest JSON")?;
        println!("{}", serde_json::to_string_pretty(&manifest)?);
        return Ok(());
    }

    // Pull manifest only from registry.
    let config = RegistryConfig {
        insecure,
        credentials: None,
    };
    let client = RegistryClient::new(&config)?;

    eprintln!("Fetching manifest for {plugin_ref}...");
    let (oci_manifest, digest) = client.pull_manifest_only(&plugin_ref).await?;

    eprintln!("Digest: {digest}");
    eprintln!("Layers:");
    for layer in &oci_manifest.layers {
        eprintln!("  {} ({} bytes)", layer.media_type, layer.size,);
    }

    Ok(())
}

async fn tags(plugin_ref_str: &str, insecure: bool) -> Result<()> {
    let plugin_ref = PluginRef::parse(plugin_ref_str).context("Invalid plugin reference")?;

    let config = RegistryConfig {
        insecure,
        credentials: None,
    };
    let client = RegistryClient::new(&config)?;

    let base_ref = plugin_ref.without_tag();
    eprintln!("Listing tags for {base_ref}...");
    let tags = client.list_tags(&plugin_ref).await?;

    if tags.is_empty() {
        eprintln!("No tags found for {base_ref}");
        return Ok(());
    }

    for tag in &tags {
        println!("{tag}");
    }

    Ok(())
}

fn list() -> Result<()> {
    let cache = PluginCache::default_location()?;
    let entries = cache.list()?;

    if entries.is_empty() {
        eprintln!("No plugins in local cache.");
        eprintln!("Use 'rapidbyte plugin pull <ref>' to download a plugin.");
        return Ok(());
    }

    // Compute column widths.
    let ref_width = entries
        .iter()
        .map(|e| e.plugin_ref.without_tag().len())
        .max()
        .unwrap_or(9)
        .max(9); // "REFERENCE"

    let tag_width = entries
        .iter()
        .map(|e| e.plugin_ref.tag.len())
        .max()
        .unwrap_or(3)
        .max(3); // "TAG"

    let size_width = 10; // enough for "999.9 MiB"

    println!(
        "{:<ref_width$}  {:<tag_width$}  {:<size_width$}  DIGEST",
        "REFERENCE", "TAG", "SIZE",
    );

    for entry in &entries {
        let short_digest = format!("sha256:{}", &entry.digest[..12.min(entry.digest.len())]);
        println!(
            "{:<ref_width$}  {:<tag_width$}  {:<size_width$}  {short_digest}",
            entry.plugin_ref.without_tag(),
            entry.plugin_ref.tag,
            format_size(entry.size_bytes),
        );
    }

    Ok(())
}

fn remove(plugin_ref_str: &str) -> Result<()> {
    let plugin_ref = PluginRef::parse(plugin_ref_str).context("Invalid plugin reference")?;
    let cache = PluginCache::default_location()?;

    let removed = cache.remove(&plugin_ref)?;

    if removed {
        eprintln!("Removed {plugin_ref} from local cache");
    } else {
        bail!("Plugin not found in cache: {plugin_ref}");
    }

    Ok(())
}
