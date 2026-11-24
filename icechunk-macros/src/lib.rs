use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, Meta, Token, parse_macro_input, punctuated::Punctuated};

/// Arguments are passed to `tokio::test`, e.g. `#[tokio_test(flavor = "multi_thread")]`.
#[proc_macro_attribute]
pub fn tokio_test(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args =
        parse_macro_input!(attr with Punctuated::<Meta, Token![,]>::parse_terminated);
    let input_fn = parse_macro_input!(item as ItemFn);
    let attrs = &input_fn.attrs;
    let vis = &input_fn.vis;
    let sig = &input_fn.sig;
    let block = &input_fn.block;

    let expanded = quote! {
        #[cfg(not(feature = "shuttle"))]
        #[test_log::test(tokio::test(#args))]
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
