//! Plugin management subcommands (pull, push, inspect, tags, list, remove, search).

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use rapidbyte_registry::{artifact, PluginCache, PluginRef, RegistryClient, RegistryConfig};

/// Execute a plugin subcommand.
///
/// # Errors
///
/// Returns `Err` on network, cache, or I/O failures.
pub async fn execute(command: crate::PluginCommands, global_config: &RegistryConfig) -> Result<()> {
    // Per-subcommand --insecure is OR'd with global --registry-insecure.
    let effective_insecure = |local: bool| local || global_config.insecure;

    match command {
        crate::PluginCommands::Pull {
            plugin_ref,
            insecure,
        } => pull(&plugin_ref, effective_insecure(insecure), global_config).await,
        crate::PluginCommands::Push {
            plugin_ref,
            wasm_path,
            insecure,
            sign,
            key,
        } => {
            push(
                &plugin_ref,
                &wasm_path,
                effective_insecure(insecure),
                sign,
                key.as_deref(),
            )
            .await
        }
        crate::PluginCommands::Inspect {
            plugin_ref,
            insecure,
        } => inspect(&plugin_ref, effective_insecure(insecure)).await,
        crate::PluginCommands::Tags {
            plugin_ref,
            insecure,
        } => tags(&plugin_ref, effective_insecure(insecure)).await,
        crate::PluginCommands::List => list(),
        crate::PluginCommands::Remove { plugin_ref } => remove(&plugin_ref),
        crate::PluginCommands::Search {
            query,
            plugin_type,
            registry,
            insecure,
        } => {
            let effective_registry = registry
                .as_deref()
                .or(global_config.default_registry.as_deref());
            {
                let kind = plugin_type.as_deref().map(parse_plugin_kind).transpose()?;
                search(
                    &query,
                    kind,
                    effective_registry,
                    effective_insecure(insecure),
                )
            }
            .await
        }
        crate::PluginCommands::Keygen { output } => keygen(&output),
        crate::PluginCommands::Scaffold { name, output } => {
            super::scaffold::run(&name, output.as_deref())
        }
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

async fn pull(plugin_ref_str: &str, insecure: bool, global_config: &RegistryConfig) -> Result<()> {
    let plugin_ref = PluginRef::parse(plugin_ref_str).context("Invalid plugin reference")?;
    let cache = PluginCache::default_location()?;

    // Check cache first.
    if let Some(entry) = cache.lookup(&plugin_ref) {
        let cached_config =
            cache
                .load_artifact_config(&plugin_ref)
                .unwrap_or(artifact::PluginArtifactConfig {
                    wasm_sha256: entry.digest.clone(),
                    signature: None,
                });
        // Verify the config's digest matches the actual cached wasm.
        if cached_config.wasm_sha256 != entry.digest {
            bail!(
                "cached artifact config digest mismatch: config says {}, wasm is {}",
                cached_config.wasm_sha256,
                entry.digest
            );
        }
        let trusted_keys = rapidbyte_registry::parse_trusted_keys(global_config)?;
        if let Some(warning) = rapidbyte_registry::verify_artifact_trust(
            &cached_config,
            global_config.trust_policy,
            &trusted_keys,
        )? {
            eprintln!("{warning}");
        }
        eprintln!(
            "Plugin {plugin_ref} already cached ({})",
            format_size(entry.size_bytes),
        );
        return Ok(());
    }

    let config = RegistryConfig {
        insecure,
        ..Default::default()
    };
    let client = RegistryClient::new(&config)?;

    eprintln!("Pulling {plugin_ref}...");
    let image = client.pull(&plugin_ref).await?;

    let unpacked = artifact::unpack_artifact(&image).context("Failed to unpack plugin artifact")?;

    // Verify plugin signature against trust policy before caching.
    let trusted_keys = rapidbyte_registry::parse_trusted_keys(global_config)?;
    if let Some(warning) = rapidbyte_registry::verify_artifact_trust(
        &unpacked.config,
        global_config.trust_policy,
        &trusted_keys,
    )? {
        eprintln!("{warning}");
    }

    let entry = cache
        .store(
            &plugin_ref,
            &unpacked.manifest_json,
            &unpacked.wasm_bytes,
            &unpacked.config,
        )
        .context("Failed to store plugin in cache")?;

    eprintln!(
        "Pulled {plugin_ref} ({}, sha256:{})",
        format_size(entry.size_bytes),
        &entry.digest[..12],
    );

    Ok(())
}

fn parse_plugin_kind(s: &str) -> Result<rapidbyte_types::wire::PluginKind> {
    match s {
        "source" => Ok(rapidbyte_types::wire::PluginKind::Source),
        "destination" => Ok(rapidbyte_types::wire::PluginKind::Destination),
        "transform" => Ok(rapidbyte_types::wire::PluginKind::Transform),
        _ => bail!("invalid plugin type: {s} (expected source, destination, or transform)"),
    }
}

fn plugin_kind_from_manifest(
    manifest: &rapidbyte_types::manifest::PluginManifest,
) -> rapidbyte_types::wire::PluginKind {
    if manifest.roles.source.is_some() {
        rapidbyte_types::wire::PluginKind::Source
    } else if manifest.roles.destination.is_some() {
        rapidbyte_types::wire::PluginKind::Destination
    } else if manifest.roles.transform.is_some() {
        rapidbyte_types::wire::PluginKind::Transform
    } else {
        rapidbyte_types::wire::PluginKind::Unknown
    }
}

async fn push(
    plugin_ref_str: &str,
    wasm_path: &PathBuf,
    insecure: bool,
    sign: bool,
    key: Option<&Path>,
) -> Result<()> {
    let plugin_ref = PluginRef::parse(plugin_ref_str).context("Invalid plugin reference")?;

    let wasm_bytes = std::fs::read(wasm_path)
        .with_context(|| format!("Failed to read WASM file: {}", wasm_path.display()))?;

    let manifest = rapidbyte_runtime::extract_manifest_from_wasm(&wasm_bytes)
        .context("WASM file does not contain a valid rapidbyte plugin manifest")?;

    let manifest_json =
        serde_json::to_vec_pretty(&manifest).context("Failed to serialize plugin manifest")?;

    let signing_key = if sign {
        let key_path = key.context("--key is required when --sign is used")?;
        let pem = std::fs::read_to_string(key_path)
            .with_context(|| format!("failed to read signing key: {}", key_path.display()))?;
        Some(
            rapidbyte_registry::signing::load_signing_key_pem(&pem)
                .context("failed to parse signing key")?,
        )
    } else {
        None
    };

    let packed = artifact::pack_artifact(&manifest_json, &wasm_bytes, signing_key.as_ref());

    let config = RegistryConfig {
        insecure,
        ..Default::default()
    };
    let client = RegistryClient::new(&config)?;

    eprintln!("Pushing {plugin_ref}...");
    let url = client
        .push(&plugin_ref, packed.layers, packed.config)
        .await?;

    if sign {
        eprintln!("Pushed {plugin_ref} (signed) ({url})");
    } else {
        eprintln!("Pushed {plugin_ref} ({url})");
    }

    // Update the registry index
    eprintln!("Updating registry index...");
    let mut index = client
        .pull_index(&plugin_ref.registry)
        .await?
        .unwrap_or_default();

    index.upsert(rapidbyte_registry::PluginIndexEntry {
        repository: plugin_ref.repository.clone(),
        name: manifest.name.clone(),
        description: manifest.description.clone(),
        plugin_type: plugin_kind_from_manifest(&manifest),
        latest: plugin_ref.tag.clone(),
        versions: vec![plugin_ref.tag.clone()],
        author: manifest.author.clone(),
        license: manifest.license.clone(),
        updated_at: chrono::Utc::now(),
    });

    client.push_index(&plugin_ref.registry, &index).await?;
    eprintln!("Index updated");

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
        ..Default::default()
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
        ..Default::default()
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

async fn search(
    query: &str,
    plugin_type: Option<rapidbyte_types::wire::PluginKind>,
    registry: Option<&str>,
    insecure: bool,
) -> Result<()> {
    let registry = registry.context(
        "registry is required for search. Use --registry or set --registry-url globally",
    )?;
    let registry = rapidbyte_registry::normalize_registry_url(registry);

    let config = RegistryConfig {
        insecure,
        ..Default::default()
    };
    let client = RegistryClient::new(&config)?;

    eprintln!("Searching {registry}...");
    let index = client
        .pull_index(&registry)
        .await?
        .context("no plugin index found in this registry (push a plugin first)")?;

    let results = index.search(query, plugin_type);

    if results.is_empty() {
        eprintln!("No plugins found");
        return Ok(());
    }

    println!(
        "{:<40} {:<14} {:<10} DESCRIPTION",
        "REPOSITORY", "TYPE", "LATEST"
    );
    for entry in &results {
        let desc = if entry.description.chars().count() > 50 {
            let truncated: String = entry.description.chars().take(47).collect();
            format!("{truncated}...")
        } else {
            entry.description.clone()
        };
        println!(
            "{:<40} {:<14} {:<10} {}",
            entry.repository, entry.plugin_type, entry.latest, desc
        );
    }

    eprintln!("\n{} plugin(s) found", results.len());
    Ok(())
}

fn keygen(output_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(output_dir).with_context(|| {
        format!(
            "failed to create output directory: {}",
            output_dir.display()
        )
    })?;

    let (private_pem, public_pem) = rapidbyte_registry::signing::generate_keypair_pem();

    let private_path = output_dir.join("signing-key.pem");
    let public_path = output_dir.join("signing-key.pub");

    std::fs::write(&private_path, &private_pem)
        .with_context(|| format!("failed to write private key: {}", private_path.display()))?;
    std::fs::write(&public_path, &public_pem)
        .with_context(|| format!("failed to write public key: {}", public_path.display()))?;

    // Restrict private key permissions on Unix
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&private_path, std::fs::Permissions::from_mode(0o600))?;
    }

    eprintln!("Generated Ed25519 signing keypair:");
    eprintln!("  Private key: {}", private_path.display());
    eprintln!("  Public key:  {}", public_path.display());
    eprintln!("\nShare the public key with consumers. Keep the private key secret.");
    Ok(())
}
