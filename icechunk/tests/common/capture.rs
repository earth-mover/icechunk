//! A fake S3-flavoured object store for wire-level tests, built on wiremock:
//! stores PUT bodies, serves GET/HEAD (with ranges and `If-None-Match`),
//! answers list and delete, and records every request's method, path and
//! `User-Agent`. No multipart support.
#![allow(dead_code)]

use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash as _, Hasher as _},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use chrono::{DateTime, SecondsFormat, Utc};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate, matchers::any};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CapturedRequest {
    pub method: String,
    /// Request path without the query string.
    pub path: String,
    pub user_agent: Option<String>,
}

#[derive(Debug, Default)]
struct State {
    objects: HashMap<String, Vec<u8>>,
    modified: HashMap<String, DateTime<Utc>>,
    requests: Vec<CapturedRequest>,
}

#[derive(Debug)]
pub(crate) struct FakeStore {
    server: MockServer,
    state: Arc<Mutex<State>>,
}

impl FakeStore {
    pub(crate) async fn start() -> Self {
        let server = MockServer::start().await;
        let state = Arc::new(Mutex::new(State::default()));
        Mock::given(any())
            .respond_with(S3Responder { state: Arc::clone(&state) })
            .mount(&server)
            .await;
        Self { server, state }
    }

    pub(crate) fn endpoint(&self) -> String {
        self.server.uri()
    }

    pub(crate) fn requests(&self) -> Vec<CapturedRequest> {
        self.lock().requests.clone()
    }

    /// Forget recorded requests. Stored objects are kept.
    pub(crate) fn clear(&self) {
        self.lock().requests.clear();
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

struct S3Responder {
    state: Arc<Mutex<State>>,
}

impl Respond for S3Responder {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let mut st = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        st.requests.push(CapturedRequest {
            method: req.method.to_string(),
            path: req.url.path().to_string(),
            user_agent: header(req, "user-agent"),
        });
        respond(req, &mut st)
    }
}

fn header(req: &Request, name: &str) -> Option<String> {
    req.headers.get(name).and_then(|v| v.to_str().ok()).map(str::to_string)
}

fn query(req: &Request, key: &str) -> Option<String> {
    req.url.query_pairs().find(|(k, _)| k == key).map(|(_, v)| v.into_owned())
}

fn etag_of(body: &[u8]) -> String {
    let mut h = DefaultHasher::new();
    body.hash(&mut h);
    format!("\"{:016x}\"", h.finish())
}

fn http_date(t: DateTime<Utc>) -> String {
    t.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

fn xml(status: u16, body: &str) -> ResponseTemplate {
    ResponseTemplate::new(status)
        .set_body_raw(body.as_bytes().to_vec(), "application/xml")
}

fn respond(req: &Request, st: &mut State) -> ResponseTemplate {
    let path = req.url.path().to_string();
    match req.method.as_str() {
        "PUT" => {
            let etag = etag_of(&req.body);
            st.objects.insert(path.clone(), req.body.clone());
            st.modified.insert(path, Utc::now());
            ResponseTemplate::new(200).insert_header("ETag", etag.as_str())
        }
        "GET" if query(req, "list-type").is_some() => {
            let prefix = query(req, "prefix").unwrap_or_default();
            // path is /bucket, keys are stored as /bucket/key
            let bucket = path.trim_end_matches('/');
            let mut body = String::from(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult><IsTruncated>false</IsTruncated>",
            );
            for (object_path, obj) in &st.objects {
                if let Some(key) =
                    object_path.strip_prefix(bucket).map(|k| k.trim_start_matches('/'))
                    && key.starts_with(&prefix)
                {
                    let modified =
                        st.modified.get(object_path).copied().unwrap_or_default();
                    body.push_str(&format!(
                        "<Contents><Key>{key}</Key><Size>{}</Size><ETag>{}</ETag><LastModified>{}</LastModified></Contents>",
                        obj.len(),
                        etag_of(obj).replace('"', "&quot;"),
                        modified.to_rfc3339_opts(SecondsFormat::Millis, true)
                    ));
                }
            }
            body.push_str("</ListBucketResult>");
            xml(200, &body)
        }
        "GET" | "HEAD" => {
            let Some(obj) = st.objects.get(&path) else {
                return xml(
                    404,
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NoSuchKey</Code></Error>",
                );
            };
            let etag = etag_of(obj);
            if header(req, "if-none-match").is_some_and(|v| v == etag) {
                return ResponseTemplate::new(304).insert_header("ETag", etag.as_str());
            }
            let total = obj.len();
            let modified = http_date(st.modified.get(&path).copied().unwrap_or_default());
            let base = ResponseTemplate::new(200)
                .insert_header("ETag", etag.as_str())
                .insert_header("Last-Modified", modified.as_str());
            if req.method == "HEAD" {
                return base.insert_header("Content-Length", total.to_string().as_str());
            }
            match parse_range(header(req, "range").as_deref(), total) {
                Some((a, b)) => ResponseTemplate::new(206)
                    .insert_header("ETag", etag.as_str())
                    .insert_header("Last-Modified", modified.as_str())
                    .insert_header(
                        "Content-Range",
                        format!("bytes {a}-{b}/{total}").as_str(),
                    )
                    .set_body_bytes(obj[a..=b].to_vec()),
                None => base.set_body_bytes(obj.clone()),
            }
        }
        "DELETE" => {
            st.objects.remove(&path);
            st.modified.remove(&path);
            ResponseTemplate::new(204)
        }
        "POST" if query(req, "delete").is_some() => {
            // DeleteObjects: <Delete><Object><Key>k</Key></Object>...</Delete>
            let body = String::from_utf8_lossy(&req.body).to_string();
            let bucket = path.trim_end_matches('/');
            for key in
                body.split("<Key>").skip(1).filter_map(|s| s.split("</Key>").next())
            {
                let object_path = format!("{bucket}/{key}");
                st.objects.remove(&object_path);
                st.modified.remove(&object_path);
            }
            xml(
                200,
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><DeleteResult></DeleteResult>",
            )
        }
        _ => ResponseTemplate::new(501),
    }
}

fn parse_range(value: Option<&str>, total: usize) -> Option<(usize, usize)> {
    let spec = value?.strip_prefix("bytes=")?;
    let (a, b) = spec.split_once('-')?;
    let a: usize = a.parse().ok()?;
    if total == 0 || a >= total {
        return None;
    }
    let b: usize = if b.is_empty() { total - 1 } else { b.parse().ok()? };
    if b < a {
        return None;
    }
    Some((a, b.min(total - 1)))
}
