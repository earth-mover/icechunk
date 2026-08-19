//! Throttle-signal plumbing for `object_store`-backed storage.
//!
//! `object_store` clients are frozen at construction while the
//! governor arrives per call inside the `StorageContext`, so per-operation
//! attachment is impossible here.
//! Instead each storage owns a [`ThrottleSink`]: every storage method
//! registers its caller's governor, and a [`GovernedHttpConnector`]
//! installed at client build time broadcasts every throttled HTTP attempt —
//! observed below `object_store`'s retry loop — to all still-live
//! registrants. Over-broad only when one storage serves multiple governors,
//! which is acceptable for fire-and-forget signals.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use icechunk_storage::{IoClass, IoGovernor};

/// Broadcast registry connecting per-call governors to this storage's
/// construction-frozen HTTP clients.
///
/// Entries are weak: a dropped governor unregisters itself, and dead
/// entries are pruned on every registration.
#[derive(Debug, Default)]
pub struct ThrottleSink {
    governors: Mutex<Vec<Weak<dyn IoGovernor>>>,
}

impl ThrottleSink {
    /// Register `governor` to receive throttle broadcasts. Deduplicates by
    /// instance.
    pub fn register(&self, governor: &Arc<dyn IoGovernor>) {
        let mut governors = self.lock();
        governors.retain(|w| w.strong_count() > 0);
        if !governors.iter().any(|w| std::ptr::addr_eq(w.as_ptr(), Arc::as_ptr(governor)))
        {
            governors.push(Arc::downgrade(governor));
        }
    }

    /// Report one throttled HTTP attempt to every live registered governor.
    pub fn record_throttle(&self, class: IoClass) {
        for governor in self.lock().iter().filter_map(Weak::upgrade) {
            governor.record_throttle(class);
        }
    }

    fn lock(&self) -> MutexGuard<'_, Vec<Weak<dyn IoGovernor>>> {
        self.governors.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[cfg(test)]
    fn registered(&self) -> usize {
        self.lock().len()
    }
}

#[cfg(any(feature = "s3", feature = "gcs", feature = "azure", feature = "http"))]
mod connector {
    use std::sync::Arc;

    use async_trait::async_trait;
    use icechunk_storage::{Asset, Direction, IoClass};
    use object_store::{
        ClientOptions,
        client::{
            HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpService,
            ReqwestConnector,
        },
    };

    use super::ThrottleSink;

    /// HTTP statuses reported as throttle signals. 503 is `SlowDown` /
    /// `ServerBusy` on S3-compatible and Azure stores; 429 is the rate-limit
    /// response (R2, GCS); 408 and 499 are timeout/overload responses some
    /// S3-compatible stores send under pressure (Tigris sends 499).
    const THROTTLE_CODES: &[u16] = &[408, 429, 499, 503];

    /// [`HttpConnector`] wrapping [`ReqwestConnector`] so every built client
    /// reports throttled attempts to the storage's [`ThrottleSink`].
    ///
    /// A builder-level hook so it composes safely with the custom
    /// header plumbing.
    #[derive(Debug)]
    pub(crate) struct GovernedHttpConnector {
        inner: ReqwestConnector,
        sink: Arc<ThrottleSink>,
    }

    impl GovernedHttpConnector {
        pub(crate) fn new(sink: Arc<ThrottleSink>) -> Self {
            Self { inner: ReqwestConnector::default(), sink }
        }
    }

    impl HttpConnector for GovernedHttpConnector {
        fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
            let inner = self.inner.connect(options)?;
            Ok(HttpClient::new(GovernedHttpService {
                inner,
                sink: Arc::clone(&self.sink),
            }))
        }
    }

    /// The wrapper sits below `object_store`'s retry loop, so it sees every
    /// attempt's status, including the ones the loop is about to retry.
    #[derive(Debug)]
    struct GovernedHttpService {
        inner: HttpClient,
        sink: Arc<ThrottleSink>,
    }

    /// The request method is all the wrapper knows about the operation, so
    /// signals classify by HTTP direction with [`Asset::Other`]; governors
    /// consume per-direction aggregates only.
    fn direction(method: &http::Method) -> Direction {
        match *method {
            http::Method::GET
            | http::Method::HEAD
            | http::Method::OPTIONS
            | http::Method::TRACE => Direction::Read,
            _ => Direction::Write,
        }
    }

    #[async_trait]
    impl HttpService for GovernedHttpService {
        async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
            let direction = direction(req.method());
            let res = self.inner.execute(req).await;
            if let Ok(response) = &res
                && THROTTLE_CODES.contains(&response.status().as_u16())
            {
                // FIXME: we don't know the asset here
                self.sink.record_throttle(IoClass { direction, asset: Asset::Other });
            }
            res
        }
    }

    #[cfg(test)]
    mod tests {
        use super::super::tests::RecordingGovernor;
        use super::*;
        use icechunk_storage::IoGovernor;
        use object_store::client::HttpResponseBody;

        /// Inner service answering every request with a fixed status.
        #[derive(Debug)]
        struct StaticStatusService(u16);

        #[async_trait]
        impl HttpService for StaticStatusService {
            async fn call(&self, _req: HttpRequest) -> Result<HttpResponse, HttpError> {
                Ok(http::Response::builder()
                    .status(self.0)
                    .body(HttpResponseBody::from(Vec::new()))
                    .expect("valid response"))
            }
        }

        fn request(method: http::Method) -> HttpRequest {
            http::Request::builder()
                .method(method)
                .uri("http://store.example/object")
                .body(object_store::client::HttpRequestBody::empty())
                .expect("valid request")
        }

        async fn respond(status: u16, method: http::Method) -> Vec<IoClass> {
            let governor = Arc::new(RecordingGovernor::default());
            let dyn_governor: Arc<dyn IoGovernor> = Arc::clone(&governor) as _;
            let sink = Arc::new(ThrottleSink::default());
            sink.register(&dyn_governor);
            let service = GovernedHttpService {
                inner: HttpClient::new(StaticStatusService(status)),
                sink,
            };
            let response = service.call(request(method)).await;
            let response = response.unwrap();
            // pass-through: the wrapper never alters the response
            assert_eq!(response.status().as_u16(), status);
            governor.throttles()
        }

        #[icechunk_macros::tokio_test]
        async fn test_throttle_statuses_record_per_direction() {
            use Direction::{Read, Write};
            let class = |direction| IoClass { direction, asset: Asset::Other };
            assert_eq!(respond(503, http::Method::GET).await, vec![class(Read)]);
            assert_eq!(respond(429, http::Method::PUT).await, vec![class(Write)]);
            assert_eq!(respond(499, http::Method::POST).await, vec![class(Write)]);
            assert_eq!(respond(408, http::Method::HEAD).await, vec![class(Read)]);
        }

        #[icechunk_macros::tokio_test]
        async fn test_non_throttle_statuses_record_nothing() {
            for status in [200u16, 206, 304, 404, 500] {
                assert_eq!(respond(status, http::Method::GET).await, vec![]);
            }
        }

        /// The wrapped connector still builds a working client from plain
        /// options (nothing performs I/O here).
        #[test]
        fn test_connector_builds() {
            let connector = GovernedHttpConnector::new(Arc::default());
            assert!(connector.connect(&ClientOptions::default()).is_ok());
        }
    }
}

#[cfg(any(feature = "s3", feature = "gcs", feature = "azure", feature = "http"))]
pub(crate) use connector::GovernedHttpConnector;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use icechunk_storage::{
        Asset, Direction, GovernorFactory, IoClass, IoGovernor, IoPermit, MemoryPermit,
        UnlimitedGovernorConfig,
    };

    use super::*;

    #[derive(Debug, Default)]
    pub(super) struct RecordingGovernor {
        throttles: Mutex<Vec<IoClass>>,
    }

    impl RecordingGovernor {
        pub(super) fn throttles(&self) -> Vec<IoClass> {
            self.throttles.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl IoGovernor for RecordingGovernor {
        async fn reserve_memory(
            &self,
            _class: IoClass,
            _expected_total: Option<u64>,
        ) -> MemoryPermit {
            MemoryPermit::noop()
        }

        async fn acquire(
            &self,
            _class: IoClass,
            _expected_bytes: Option<u64>,
        ) -> IoPermit {
            IoPermit::noop()
        }

        fn factory(&self) -> Arc<dyn GovernorFactory> {
            Arc::new(UnlimitedGovernorConfig {})
        }

        fn record_throttle(&self, class: IoClass) {
            self.throttles.lock().unwrap().push(class);
        }

        fn as_arc_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
            self
        }
    }

    const CLASS: IoClass = IoClass { direction: Direction::Read, asset: Asset::Other };

    fn governor() -> (Arc<RecordingGovernor>, Arc<dyn IoGovernor>) {
        let concrete = Arc::new(RecordingGovernor::default());
        let as_dyn: Arc<dyn IoGovernor> = Arc::clone(&concrete) as _;
        (concrete, as_dyn)
    }

    #[test]
    fn test_register_dedups_by_instance() {
        let sink = ThrottleSink::default();
        let (concrete, governor) = governor();
        sink.register(&governor);
        sink.register(&governor);
        assert_eq!(sink.registered(), 1);
        sink.record_throttle(CLASS);
        assert_eq!(concrete.throttles(), vec![CLASS]);
    }

    #[test]
    fn test_broadcast_reaches_every_registrant() {
        let sink = ThrottleSink::default();
        let (concrete_a, governor_a) = governor();
        let (concrete_b, governor_b) = governor();
        sink.register(&governor_a);
        sink.register(&governor_b);
        sink.record_throttle(CLASS);
        assert_eq!(concrete_a.throttles(), vec![CLASS]);
        assert_eq!(concrete_b.throttles(), vec![CLASS]);
    }

    #[test]
    fn test_dropped_governors_are_pruned_and_skipped() {
        let sink = ThrottleSink::default();
        let (concrete_a, governor_a) = governor();
        let (concrete_b, governor_b) = governor();
        sink.register(&governor_a);
        sink.register(&governor_b);
        drop((concrete_b, governor_b));
        // dead entry skipped by broadcast
        sink.record_throttle(CLASS);
        assert_eq!(concrete_a.throttles(), vec![CLASS]);
        // and pruned by the next registration
        sink.register(&governor_a);
        assert_eq!(sink.registered(), 1);
    }
}
