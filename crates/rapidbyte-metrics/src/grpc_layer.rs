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

    fn assert_layer<L: tower::Layer<tower::util::BoxService<(), (), ()>>>(_l: &L) {}

    #[test]
    fn layer_compiles_with_tower() {
        let layer = GrpcMetricsLayer;
        assert_layer(&layer);
    }

    #[test]
    fn grpc_status_label_uses_grpc_status_header_for_failed_responses() {
        let response = tonic::Status::permission_denied("nope").into_http();
        assert_eq!(grpc_status_label(&response), "permission_denied");
    }

    #[test]
    fn grpc_status_label_falls_back_to_http_status_when_grpc_status_is_absent() {
        let response = http::Response::builder()
            .status(http::StatusCode::SERVICE_UNAVAILABLE)
            .body(())
            .unwrap();
        assert_eq!(grpc_status_label(&response), "unavailable");
    }
}
