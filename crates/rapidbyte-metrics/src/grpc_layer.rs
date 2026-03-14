//! Custom tower layer for gRPC RED metrics.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use opentelemetry::KeyValue;
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
                Ok(_) => "ok",
                Err(_) => "error",
            };
            let labels = [
                KeyValue::new("method", method),
                KeyValue::new("status", status),
            ];

            instruments::grpc::request_duration().record(elapsed, &labels);

            result
        })
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
}
