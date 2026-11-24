use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, parse_macro_input};

#[proc_macro_attribute]
pub fn tokio_test(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    let attrs = &input_fn.attrs;
    let vis = &input_fn.vis;
    let sig = &input_fn.sig;
    let block = &input_fn.block;

    let expanded = quote! {
        #[cfg(not(feature = "shuttle"))]
        #[test_log::test(tokio::test)]
        #[test_log(default_log_filter = "warn")]
        #(#attrs)*
        #vis #sig #block
    };

    TokenStream::from(expanded)
}

#[proc_macro_attribute]
pub fn test(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    let attrs = &input_fn.attrs;
    let vis = &input_fn.vis;
    let sig = &input_fn.sig;
    let block = &input_fn.block;

    let expanded = quote! {
        #[test_log::test]
        #[test_log(default_log_filter = "warn")]
        #(#attrs)*
        #vis #sig #block
    };

    TokenStream::from(expanded)
}

/// `#[async_trait]`, but `#[async_trait(?Send)]` on `wasm32-unknown-unknown`,
/// so browser code that awaits JS promises can implement the trait.
///
/// Use it on such traits (like `Storage`) and on every impl of them. The
/// calling crate needs `async-trait` as a dependency.
#[proc_macro_attribute]
pub fn maybe_send_async_trait(attr: TokenStream, item: TokenStream) -> TokenStream {
    parse_macro_input!(attr as syn::parse::Nothing);
    let item = parse_macro_input!(item as syn::Item);
    let expanded = quote! {
        #[cfg_attr(
            not(all(target_family = "wasm", target_os = "unknown")),
            ::async_trait::async_trait
        )]
        #[cfg_attr(
            all(target_family = "wasm", target_os = "unknown"),
            ::async_trait::async_trait(?Send)
        )]
        #item
    };
    TokenStream::from(expanded)
}
