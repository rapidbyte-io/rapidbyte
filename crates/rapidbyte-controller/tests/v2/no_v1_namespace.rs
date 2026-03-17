use std::path::{Path, PathBuf};

fn collect_rust_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir).expect("directory should be readable");
        for entry in entries {
            let entry = entry.expect("directory entry should be readable");
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
    }
    files
}

#[test]
fn controller_sources_do_not_reference_v1_proto_namespace() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for file in collect_rust_files(&root) {
        let source =
            std::fs::read_to_string(&file).expect("source file should be readable for guardrail");
        assert!(
            !source.contains("proto::rapidbyte::v1") && !source.contains("rapidbyte::v1::"),
            "v1 namespace reference found in {}",
            file.display()
        );
    }
}
