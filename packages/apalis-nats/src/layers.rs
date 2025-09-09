use std::time::Duration;

use apalis_core::request::Request;
use tower::{Layer, Service};

use crate::NatsContext;

/// A layer that automatically sends periodic Progress acknowledgements to extend `ack_wait`
/// while a job is running. The heartbeat stops when the handler returns or panics.
#[derive(Clone, Debug)]
pub struct ProgressHeartbeatLayer {
    interval: Duration,
}

impl ProgressHeartbeatLayer {
    /// Create a new heartbeat layer with the given interval. The interval must be less
    /// than the consumer `ack_wait`.
    pub fn new(interval: Duration) -> Self {
        Self { interval }
    }
}

impl<S> Layer<S> for ProgressHeartbeatLayer {
    type Service = ProgressHeartbeatService<S>;

    fn layer(&self, service: S) -> Self::Service {
        ProgressHeartbeatService {
            service,
            interval: self.interval,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ProgressHeartbeatService<S> {
    service: S,
    interval: Duration,
}

impl<S, Req> Service<Request<Req, NatsContext>> for ProgressHeartbeatService<S>
where
    S: Service<Request<Req, NatsContext>> + Send + Clone + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    S::Response: Send + 'static,
    Req: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = futures::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Req, NatsContext>) -> Self::Future {
        let mut inner = self.service.clone();
        let interval = self.interval;

        let fut = async move {
            // Start heartbeat (if this request carries a real NATS message)
            let _guard = request.parts.context.start_progress_heartbeat(interval);
            inner.call(request).await
        };

        Box::pin(fut)
    }
}

