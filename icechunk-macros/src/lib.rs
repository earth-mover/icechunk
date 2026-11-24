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

/// `#[async_trait]` whose futures are `Send` everywhere except
/// `wasm32-unknown-unknown`, where they are `?Send` so browser `fetch`-based
/// code can implement the trait.
///
/// Use it on traits whose futures must not be `Send` in the browser (like
/// `Storage`) and on every impl of those traits. It expands to two
/// `cfg_attr`s, evaluated in the calling crate, so it needs `async-trait` as
/// a dependency there.
///
/// ```ignore
/// #[icechunk_storage::maybe_send_async_trait]
/// #[typetag::serde]
/// impl Storage for MyStorage { /* ... */ }
/// ```
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
