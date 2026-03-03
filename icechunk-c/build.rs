fn main() {
    let Ok(crate_dir) = std::env::var("CARGO_MANIFEST_DIR") else {
        eprintln!("CARGO_MANIFEST_DIR not set");
        return;
    };
    let config_path = std::path::Path::new(&crate_dir).join("cbindgen.toml");
    let config = cbindgen::Config::from_file(&config_path).unwrap_or_default();

    let output_path = std::path::Path::new(&crate_dir).join("include").join("icechunk.h");

    match cbindgen::Builder::new().with_crate(&crate_dir).with_config(config).generate() {
        Ok(bindings) => {
            bindings.write_to_file(&output_path);
        }
        Err(e) => {
            eprintln!("Unable to generate C bindings: {e}");
        }
    }
}
