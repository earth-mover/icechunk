fn main() {
    // `cfg(browser)`: wasm32-unknown-unknown, bound with wasm-bindgen instead of napi.
    println!("cargo::rustc-check-cfg=cfg(browser)");
    let family = std::env::var("CARGO_CFG_TARGET_FAMILY").unwrap_or_default();
    let os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if family.split(',').any(|f| f == "wasm") && os == "unknown" {
        println!("cargo::rustc-cfg=browser");
    } else {
        napi_build::setup();
    }
}
