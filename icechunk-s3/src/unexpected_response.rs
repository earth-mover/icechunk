use std::fmt;

use aws_sdk_s3::{
    config::http::HttpResponse,
    error::{ProvideErrorMetadata, SdkError},
};
use icechunk_storage::{StorageError, StorageResult, obj_store_error};

const EXCERPT_CHARS: usize = 200;

const FOREIGN_RESPONSE_HINT: &str = "A proxy, firewall, or captive portal between this \
    machine and the object store can send a response like this. Check the HTTPS_PROXY, \
    HTTP_PROXY, and NO_PROXY environment variables, or ask your network administrator \
    to allow access to the object store.";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UnexpectedResponse {
    status: u16,
    content_type: Option<String>,
    excerpt: Option<String>,
}

impl UnexpectedResponse {
    fn new(status: u16, content_type: Option<&str>, body: &[u8]) -> Self {
        Self {
            status,
            content_type: content_type.map(str::to_string),
            excerpt: excerpt(body),
        }
    }

    fn from_raw(raw: &HttpResponse) -> Self {
        Self::new(
            raw.status().as_u16(),
            raw.headers().get("content-type"),
            raw.body().bytes().unwrap_or_default(),
        )
    }

    fn is_foreign(&self) -> bool {
        self.excerpt.is_some() && !self.content_type.as_deref().is_some_and(is_xml)
    }
}

impl fmt::Display for UnexpectedResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the server answered HTTP {} without an S3 error code", self.status)?;
        if let Some(content_type) = &self.content_type {
            write!(f, ", content type {content_type}")?;
        }
        if let Some(excerpt) = &self.excerpt {
            write!(f, ", body {excerpt:?}")?;
        }
        if self.is_foreign() {
            write!(f, ". {FOREIGN_RESPONSE_HINT}")?;
        }
        Ok(())
    }
}

impl std::error::Error for UnexpectedResponse {}

fn is_xml(content_type: &str) -> bool {
    content_type.to_ascii_lowercase().contains("xml")
}

fn excerpt(body: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(body).ok()?;
    let text = html_title(text).unwrap_or(text);
    let words: Vec<&str> = text.split_whitespace().collect();
    if words.is_empty() {
        return None;
    }
    let collapsed = words.join(" ");
    Some(match collapsed.char_indices().nth(EXCERPT_CHARS) {
        Some((cut, _)) => format!("{}…", &collapsed[..cut]),
        None => collapsed,
    })
}

fn html_title(text: &str) -> Option<&str> {
    // ASCII lowercasing keeps byte offsets, so indexes into `lower` are valid in `text`
    let lower = text.to_ascii_lowercase();
    let tag = lower.find("<title")?;
    let start = tag + lower[tag..].find('>')? + 1;
    let end = start + lower[start..].find("</title")?;
    Some(&text[start..end]).filter(|title| !title.trim().is_empty())
}

fn unexpected_response<E: ProvideErrorMetadata>(
    err: &SdkError<E, HttpResponse>,
) -> Option<UnexpectedResponse> {
    match err {
        SdkError::ServiceError(ctx) if ctx.err().code().is_none() => {
            Some(UnexpectedResponse::from_raw(ctx.raw()))
        }
        SdkError::ResponseError(ctx)
            if ctx.raw().headers().get("content-type").is_some_and(|ct| !is_xml(ct)) =>
        {
            Some(UnexpectedResponse::from_raw(ctx.raw()))
        }
        _ => None,
    }
}

pub(crate) fn s3_error<E>(err: SdkError<E, HttpResponse>) -> StorageError
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    match unexpected_response(&err) {
        Some(response) => obj_store_error(response),
        None => obj_store_error(err),
    }
}

pub(crate) fn s3_error_res<T, E>(err: SdkError<E, HttpResponse>) -> StorageResult<T>
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    Err(s3_error(err))
}

#[cfg(test)]
mod tests {
    use aws_sdk_s3::{error::ErrorMetadata, operation::get_object::GetObjectError};

    use super::*;

    const ZSCALER_PAGE: &str = "<!DOCTYPE html>\n<html><head>\n<style>body { color: red }</style>\n\
        <title>\n  Website blocked\n</title></head><body>Access denied by policy</body></html>";

    fn raw(status: u16, content_type: Option<&str>, body: &str) -> HttpResponse {
        let mut raw =
            HttpResponse::new(status.try_into().unwrap(), body.to_string().into());
        if let Some(ct) = content_type {
            raw.headers_mut().insert("content-type", ct.to_string());
        }
        raw
    }

    fn service_error(
        code: Option<&str>,
        raw: HttpResponse,
    ) -> SdkError<GetObjectError, HttpResponse> {
        let mut meta = ErrorMetadata::builder();
        if let Some(code) = code {
            meta = meta.code(code);
        }
        SdkError::service_error(GetObjectError::generic(meta.build()), raw)
    }

    #[test]
    fn describes_a_proxy_block_page() {
        let err = service_error(None, raw(403, Some("text/html"), ZSCALER_PAGE));
        let msg = s3_error(err).to_string();
        assert!(msg.contains("HTTP 403 without an S3 error code"), "{msg}");
        assert!(msg.contains("content type text/html"), "{msg}");
        assert!(msg.contains("body \"Website blocked\""), "{msg}");
        assert!(msg.contains("HTTPS_PROXY"), "{msg}");
    }

    #[test]
    fn keeps_s3_errors_with_a_code() {
        let err = service_error(
            Some("AccessDenied"),
            raw(403, Some("application/xml"), "<Error><Code>AccessDenied</Code></Error>"),
        );
        assert_eq!(unexpected_response(&err), None);
    }

    #[test]
    fn empty_body_has_no_proxy_hint() {
        let err = service_error(None, raw(403, None, ""));
        let msg = unexpected_response(&err).unwrap().to_string();
        assert_eq!(msg, "the server answered HTTP 403 without an S3 error code");
    }

    #[test]
    fn xml_body_without_a_code_has_no_proxy_hint() {
        let response = UnexpectedResponse::new(500, Some("application/xml"), b"<Error/>");
        assert!(!response.is_foreign());
    }

    #[test]
    fn unparsable_html_success_is_described() {
        let err: SdkError<GetObjectError, HttpResponse> = SdkError::response_error(
            "failed to parse XML",
            raw(
                200,
                Some("text/html; charset=utf-8"),
                "<html>Sign in to the Wi-Fi</html>",
            ),
        );
        let msg = unexpected_response(&err).unwrap().to_string();
        assert!(msg.contains("HTTP 200"), "{msg}");
        assert!(msg.contains("Sign in to the Wi-Fi"), "{msg}");
    }

    #[test]
    fn unparsable_xml_is_left_alone() {
        let err: SdkError<GetObjectError, HttpResponse> = SdkError::response_error(
            "failed to parse XML",
            raw(200, Some("application/xml"), "<ListBucketResult"),
        );
        assert_eq!(unexpected_response(&err), None);
    }

    #[test]
    fn excerpt_collapses_whitespace_and_truncates() {
        assert_eq!(
            excerpt(b"  blocked \n\n by   policy "),
            Some("blocked by policy".into())
        );
        let long = "é".repeat(EXCERPT_CHARS + 10);
        let cut = excerpt(long.as_bytes()).unwrap();
        assert_eq!(cut.chars().count(), EXCERPT_CHARS + 1);
        assert!(cut.ends_with('…'));
    }

    #[test]
    fn excerpt_skips_binary_and_empty_bodies() {
        assert_eq!(excerpt(&[0xff, 0xfe, 0x00]), None);
        assert_eq!(excerpt(b" \n "), None);
    }

    #[test]
    fn html_without_a_title_uses_the_text() {
        assert_eq!(
            excerpt(b"<html><title> </title>denied</html>"),
            Some("<html><title> </title>denied</html>".into())
        );
    }
}
