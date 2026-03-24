use std::path::Path;

fn main() {
    let vendor_root = Path::new("../../vendor/libpg_query");

    // In CI environments without vendored sources, skip validation
    // Vendored sources are local-only and not tracked in git
    if !vendor_root.exists() {
        if std::env::var("CI").is_ok() {
            eprintln!(
                "Warning: Vendored libpg_query sources not found at {}",
                vendor_root.display()
            );
            eprintln!("Proceeding without validation (CI environment detected)");
            return;
        }
        panic!(
            "expected vendored libpg_query sources at {}",
            vendor_root.display()
        );
    }

    println!("cargo:rerun-if-changed={}", vendor_root.display());
}
