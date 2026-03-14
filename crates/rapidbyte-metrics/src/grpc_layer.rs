//! Custom tower layer for gRPC RED metrics.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use opentelemetry::KeyValue;
use tonic::{Code, Status};
use tower::{Layer, Service};

use crate::instruments;

/// Tower layer that records gRPC request metrics.
#[derive(Debug, Clone, Default)]
pub struct GrpcMetricsLayer;

impl<S> Layer<S> for GrpcMetricsLayer {
    type Service = GrpcMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcMetricsService { inner }
    }
}

/// Tower service wrapper that records per-request metrics.
#[derive(Debug, Clone)]
pub struct GrpcMetricsService<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for GrpcMetricsService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: std::fmt::Debug,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let method = req.uri().path().to_owned();
        let start = Instant::now();
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            let result = inner.call(req).await;
            let elapsed = start.elapsed().as_secs_f64();
            let status = match &result {
                Ok(response) => grpc_status_label(response),
                Err(_) => "error",
            };
            let labels = [
                KeyValue::new(crate::labels::METHOD, method),
                KeyValue::new(crate::labels::STATUS, status),
            ];

            instruments::grpc::request_duration().record(elapsed, &labels);

            result
        })
    }
}

fn grpc_status_label<Body>(response: &http::Response<Body>) -> &'static str {
    if let Some(status) = Status::from_header_map(response.headers()) {
        return grpc_code_label(status.code());
    }

    match response.status() {
        http::StatusCode::BAD_REQUEST => grpc_code_label(Code::Internal),
        http::StatusCode::UNAUTHORIZED => grpc_code_label(Code::Unauthenticated),
        http::StatusCode::FORBIDDEN => grpc_code_label(Code::PermissionDenied),
        http::StatusCode::NOT_FOUND => grpc_code_label(Code::Unimplemented),
        http::StatusCode::TOO_MANY_REQUESTS
        | http::StatusCode::BAD_GATEWAY
        | http::StatusCode::SERVICE_UNAVAILABLE
        | http::StatusCode::GATEWAY_TIMEOUT => grpc_code_label(Code::Unavailable),
        _ => grpc_code_label(Code::Ok),
    }
}

const fn grpc_code_label(code: Code) -> &'static str {
    match code {
        Code::Ok => "ok",
        Code::Cancelled => "cancelled",
        Code::Unknown => "unknown",
        Code::InvalidArgument => "invalid_argument",
        Code::DeadlineExceeded => "deadline_exceeded",
        Code::NotFound => "not_found",
        Code::AlreadyExists => "already_exists",
        Code::PermissionDenied => "permission_denied",
        Code::ResourceExhausted => "resource_exhausted",
        Code::FailedPrecondition => "failed_precondition",
        Code::Aborted => "aborted",
        Code::OutOfRange => "out_of_range",
        Code::Unimplemented => "unimplemented",
        Code::Internal => "internal",
        Code::Unavailable => "unavailable",
        Code::DataLoss => "data_loss",
        Code::Unauthenticated => "unauthenticated",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::global;
    use opentelemetry_sdk::metrics::data::Histogram;
    use opentelemetry_sdk::metrics::{
        InMemoryMetricExporter, InMemoryMetricExporterBuilder, PeriodicReader, SdkMeterProvider,
    };
    use std::collections::BTreeMap;
    use std::convert::Infallible;
    use std::sync::LazyLock;
    use tokio::sync::Mutex;
    use tower::ServiceExt;

    static METRIC_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    fn metric_test_provider() -> (SdkMeterProvider, InMemoryMetricExporter) {
        let exporter = InMemoryMetricExporterBuilder::new().build();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        (provider, exporter)
    }

    fn request_duration_labels(exporter: &InMemoryMetricExporter) -> Vec<BTreeMap<String, String>> {
        let mut labels = Vec::new();
        let metrics = exporter.get_finished_metrics().unwrap_or_default();
        for resource_metrics in metrics {
            for scope_metrics in resource_metrics.scope_metrics {
                for metric in scope_metrics.metrics {
                    if metric.name != "grpc.request.duration" {
                        continue;
                    }
                    if let Some(histogram) = metric.data.as_any().downcast_ref::<Histogram<f64>>() {
                        for point in &histogram.data_points {
                            let attrs = point
                                .attributes
                                .iter()
                                .map(|kv| (kv.key.as_str().to_owned(), kv.value.to_string()))
                                .collect();
                            labels.push(attrs);
                        }
                    }
                }
            }
        }
        labels
    }

    fn assert_layer<L: tower::Layer<tower::util::BoxService<(), (), ()>>>(_l: &L) {}

    #[test]
    fn layer_compiles_with_tower() {
        let layer = GrpcMetricsLayer;
        assert_layer(&layer);
    }

    #[tokio::test]
    async fn layer_uses_grpc_status_header_for_failed_responses() {
        let _guard = METRIC_TEST_LOCK.lock().await;
        let (provider, exporter) = metric_test_provider();
        global::set_meter_provider(provider.clone());

        let service =
            GrpcMetricsLayer.layer(tower::service_fn(|_req: http::Request<()>| async move {
                Ok::<_, Infallible>(tonic::Status::permission_denied("nope").into_http())
            }));

        let response = service
            .oneshot(http::Request::new(()))
            .await
            .expect("service should respond");
        assert_eq!(response.status(), http::StatusCode::OK);

        let _ = provider.force_flush();
        let labels = request_duration_labels(&exporter);
        assert!(
            labels.iter().any(|attrs| {
                attrs
                    .get(crate::labels::METHOD)
                    .is_some_and(|method| method == "/")
                    && attrs
                        .get(crate::labels::STATUS)
                        .is_some_and(|status| status == "permission_denied")
            }),
            "expected grpc.request.duration label set with permission_denied, got {labels:?}"
        );
    }
}
