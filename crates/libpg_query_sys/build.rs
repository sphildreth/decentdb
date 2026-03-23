use std::path::Path;

fn main() {
    let vendor_root = Path::new("../../vendor/libpg_query");
    assert!(
        vendor_root.exists(),
        "expected vendored libpg_query sources at {}",
        vendor_root.display()
    );

    println!("cargo:rerun-if-changed={}", vendor_root.display());
}
