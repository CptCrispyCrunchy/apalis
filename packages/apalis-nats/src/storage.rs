use apalis_core::backend::Backend;
use apalis_core::codec::json::JsonCodec;
use apalis_core::codec::Codec;
use apalis_core::error::Error;
use apalis_core::layers::{Ack, AckLayer};
use apalis_core::poller::Poller;
use apalis_core::request::{Parts, Request};
use apalis_core::response::Response;
use apalis_core::storage::Storage;
use apalis_core::task::attempt::Attempt;
use apalis_core::task::namespace::Namespace;
use apalis_core::task::task_id::TaskId;
use apalis_core::worker::{Context as WorkerContext, Worker};
use async_nats::jetstream::{self, consumer, stream};
use async_nats::{Client, ConnectError, HeaderMap};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::channel::mpsc::{self, Sender};
use futures::stream::BoxStream;
use futures::{SinkExt, StreamExt, TryStreamExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::json;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

#[cfg(feature = "otel")]
use opentelemetry::trace::{Span as OtelSpan, SpanKind, Status, Tracer};
#[cfg(feature = "otel")]
use opentelemetry::{global, Context as OtelContext, KeyValue};
#[cfg(feature = "otel")]
use opentelemetry_nats::{NatsHeaderExtractor, NatsHeaderInjector};
#[cfg(feature = "otel")]
use tracing::Span;
#[cfg(feature = "otel")]
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Priority levels for jobs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Priority {
    /// High priority jobs
    High,
    /// Medium priority jobs
    Medium,
    /// Low priority jobs
    Low,
}

impl fmt::Display for Priority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Priority::High => write!(f, "high"),
            Priority::Medium => write!(f, "medium"),
            Priority::Low => write!(f, "low"),
        }
    }
}

impl Default for Priority {
    fn default() -> Self {
        Priority::Medium
    }
}

/// Configuration for NATS storage
#[derive(Debug, Clone)]
pub struct Config {
    /// The namespace for all streams (e.g., "apalis")
    pub namespace: String,
    /// Maximum number of delivery attempts before moving to DLQ
    pub max_deliver: i64,
    /// Ack wait time (how long to wait for a job to be acknowledged)
    pub ack_wait: Duration,
    /// Number of replicas for streams
    pub num_replicas: usize,
    /// Enable dead letter queue
    pub enable_dlq: bool,
    /// Maximum number of pending acknowledgments per consumer
    pub max_ack_pending: i64,
    /// Enable OpenTelemetry tracing
    #[cfg(feature = "otel")]
    pub enable_tracing: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            namespace: "apalis".to_string(),
            max_deliver: 5,
            ack_wait: Duration::from_secs(30),
            num_replicas: 1,
            enable_dlq: true,
            max_ack_pending: 100, // Allow up to 100 unacknowledged messages per consumer
            #[cfg(feature = "otel")]
            enable_tracing: true,
        }
    }
}

/// NATS poll error
#[derive(Debug, Error)]
pub enum NatsPollError {
    /// NATS client error
    #[error("NATS error: {0}")]
    Nats(String),
    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    /// Storage error
    #[error("Storage error: {0}")]
    Storage(String),
}

// Implementation for all NATS error types
impl From<async_nats::Error> for NatsPollError {
    fn from(err: async_nats::Error) -> Self {
        NatsPollError::Nats(err.to_string())
    }
}

/// Job wrapper for NATS
#[derive(Debug, Clone, Serialize, Deserialize)]
struct NatsJob<T> {
    pub id: TaskId,
    pub data: T,
    pub priority: Priority,
    pub attempts: Attempt,
    pub created_at: DateTime<Utc>,
    pub namespace: Namespace,
}

/// Context for NATS jobs
#[derive(Debug, Clone, Default)]
pub struct NatsContext {
    pub(crate) message: Option<Arc<jetstream::Message>>,
    #[cfg(feature = "otel")]
    trace_context: Option<OtelContext>,
}

impl NatsContext {
    /// Create a new context with a message
    pub fn with_message(message: jetstream::Message) -> Self {
        #[cfg(feature = "otel")]
        {
            // Extract trace context from message headers
            let trace_context = global::get_text_map_propagator(|propagator| {
                propagator.extract(&NatsHeaderExtractor::new(
                    message.headers.as_ref().unwrap_or(&HeaderMap::new()),
                ))
            });

            Self {
                message: Some(Arc::new(message)),
                trace_context: Some(trace_context),
            }
        }

        #[cfg(not(feature = "otel"))]
        Self {
            message: Some(Arc::new(message)),
        }
    }

    /// Get the underlying NATS message
    pub fn message(&self) -> Option<&jetstream::Message> {
        self.message.as_ref().map(|m| m.as_ref())
    }

    /// Get the OpenTelemetry trace context
    #[cfg(feature = "otel")]
    pub fn trace_context(&self) -> Option<&OtelContext> {
        self.trace_context.as_ref()
    }
}

/// Queue info for NATS
#[derive(Debug, Clone)]
pub struct NatsQueueInfo {
    /// The stream name
    pub stream: String,
    /// Number of pending messages
    pub pending: u64,
}

/// NATS storage implementation
pub struct NatsStorage<T> {
    client: Client,
    pub(crate) jetstream: jetstream::Context,
    pub(crate) config: Config,
    _phantom: PhantomData<T>,
}

impl<T> fmt::Debug for NatsStorage<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NatsStorage")
            .field("config", &self.config)
            .finish()
    }
}

impl<T> Clone for NatsStorage<T> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            jetstream: self.jetstream.clone(),
            config: self.config.clone(),
            _phantom: PhantomData,
        }
    }
}

/// Connect to NATS with basic URL
///
/// For simple connections without authentication.
///
/// # Example
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = apalis_nats::connect("nats://localhost:4222").await?;
/// # Ok(())
/// # }
/// ```
pub async fn connect(url: impl async_nats::ToServerAddrs) -> Result<Client, ConnectError> {
    async_nats::connect(url).await
}

/// Connect to NATS with credentials file
///
/// Authenticates using a `.creds` file containing JWT and NKey seed.
///
/// # Example
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = apalis_nats::connect_with_credentials(
///     "nats://connect.ngs.global",
///     "path/to/my.creds"
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn connect_with_credentials(
    url: impl async_nats::ToServerAddrs,
    creds_path: impl AsRef<std::path::Path>,
) -> Result<Client, ConnectError> {
    async_nats::ConnectOptions::with_credentials_file(creds_path.as_ref())
        .await?
        .connect(url)
        .await
}

/// Connect to NATS with username and password
///
/// # Example
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = apalis_nats::connect_with_user_pass(
///     "nats://localhost:4222",
///     "myuser",
///     "mypassword"
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn connect_with_user_pass(
    url: impl async_nats::ToServerAddrs,
    user: impl Into<String>,
    password: impl Into<String>,
) -> Result<Client, ConnectError> {
    async_nats::ConnectOptions::with_user_and_password(user.into(), password.into())
        .connect(url)
        .await
}

/// Connect to NATS with custom options
///
/// Provides full control over connection configuration including authentication,
/// client name, and other advanced options.
///
/// # Example
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = apalis_nats::connect_with_options(
///     "nats://localhost:4222",
///     async_nats::ConnectOptions::new()
///         .name("my-worker")
///         .credentials_file("path/to/my.creds").await?
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn connect_with_options(
    url: impl async_nats::ToServerAddrs,
    options: async_nats::ConnectOptions,
) -> Result<Client, ConnectError> {
    options.connect(url).await
}

impl<T> NatsStorage<T>
where
    T: Serialize + DeserializeOwned + Send + 'static,
{
    /// Create a new NATS storage instance
    pub async fn new(client: Client) -> Result<Self, NatsPollError> {
        Self::new_with_config(client, Config::default()).await
    }

    /// Create a new NATS storage instance with custom config
    pub async fn new_with_config(client: Client, config: Config) -> Result<Self, NatsPollError> {
        let jetstream = jetstream::new(client.clone());

        // Create streams for each priority level
        for priority in [Priority::High, Priority::Medium, Priority::Low] {
            let stream_name = format!("{}_{}", config.namespace, priority);
            let subject = format!("{}.{}", config.namespace, priority);

            let stream_config = stream::Config {
                name: stream_name.clone(),
                subjects: vec![subject],
                // Message retention settings
                max_age: Duration::from_secs(7 * 24 * 60 * 60), // 7 days
                storage: stream::StorageType::File,
                num_replicas: config.num_replicas,
                // Work queue optimizations
                retention: stream::RetentionPolicy::WorkQueue, // Automatically remove acknowledged messages
                discard: stream::DiscardPolicy::Old, // When stream is full, discard old messages
                duplicate_window: Duration::from_secs(120), // Prevent duplicate messages within 2 minutes
                ..Default::default()
            };

            // Create or update stream
            match jetstream.get_or_create_stream(stream_config).await {
                Ok(_) => log::info!("Stream {} ready", stream_name),
                Err(e) => {
                    log::error!("Failed to create stream {}: {}", stream_name, e);
                    return Err(NatsPollError::Nats(e.to_string()));
                }
            }
        }

        // Create DLQ stream if enabled
        if config.enable_dlq {
            let dlq_stream_name = format!("{}_dlq", config.namespace);
            let dlq_subject = format!("{}.dlq", config.namespace);

            let dlq_config = stream::Config {
                name: dlq_stream_name.clone(),
                subjects: vec![dlq_subject],
                max_age: Duration::from_secs(30 * 24 * 60 * 60), // 30 days
                storage: stream::StorageType::File,
                num_replicas: config.num_replicas,
                ..Default::default()
            };

            match jetstream.get_or_create_stream(dlq_config).await {
                Ok(_) => log::info!("DLQ stream {} ready", dlq_stream_name),
                Err(e) => {
                    log::error!("Failed to create DLQ stream {}: {}", dlq_stream_name, e);
                    return Err(NatsPollError::Nats(e.to_string()));
                }
            }
        }

        Ok(Self {
            client,
            jetstream,
            config,
            _phantom: PhantomData,
        })
    }

    /// Get the stream name for a priority level
    pub(crate) fn get_stream_name(&self, priority: Priority) -> String {
        format!("{}_{}", self.config.namespace, priority)
    }

    /// Get the subject for a priority level
    fn get_subject(&self, priority: Priority) -> String {
        format!("{}.{}", self.config.namespace, priority)
    }

    /// Push a job with a specific priority
    ///
    /// Jobs are published to priority-specific streams and will be processed
    /// in priority order (High -> Medium -> Low).
    ///
    /// # Example
    /// ```no_run
    /// # use apalis_nats::{NatsStorage, Priority};
    /// # async fn example(storage: NatsStorage<String>) -> Result<(), Box<dyn std::error::Error>> {
    /// storage.push_with_priority("urgent_task".to_string(), Priority::High).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn push_with_priority(
        &self,
        job: T,
        priority: Priority,
    ) -> Result<TaskId, NatsPollError> {
        #[cfg(feature = "otel")]
        let mut _span = if self.config.enable_tracing {
            let tracer = global::tracer("apalis-nats");
            let span = tracer
                .span_builder("job.push")
                .with_kind(SpanKind::Producer)
                .with_attributes(vec![
                    KeyValue::new("job.priority", priority.to_string()),
                    KeyValue::new("job.namespace", self.config.namespace.clone()),
                ])
                .start(&tracer);
            Some(span)
        } else {
            None
        };

        let task_id = TaskId::new();
        let nats_job = NatsJob {
            id: task_id.clone(),
            data: job,
            priority,
            attempts: Attempt::new(),
            created_at: Utc::now(),
            namespace: Namespace::from(self.config.namespace.clone()),
        };

        let payload = serde_json::to_vec(&nats_job)?;
        let subject = self.get_subject(priority);

        // Prepare headers with OpenTelemetry trace context
        let mut headers = HeaderMap::new();

        #[cfg(feature = "otel")]
        if self.config.enable_tracing {
            // Inject current trace context into headers
            let cx = Span::current().context();
            global::get_text_map_propagator(|propagator| {
                let mut injector = NatsHeaderInjector::new(headers.clone());
                propagator.inject_context(&cx, &mut injector);
                headers = injector.into();
            });
        }

        // Publish with headers
        self.jetstream
            .publish_with_headers(subject, headers, Bytes::from(payload))
            .await
            .map_err(|e| NatsPollError::Nats(e.to_string()))?
            .await
            .map_err(|e| NatsPollError::Nats(e.to_string()))?;

        #[cfg(feature = "otel")]
        if let Some(ref mut span) = _span {
            use OtelSpan;
            span.set_attribute(KeyValue::new("job.id", task_id.to_string()));
            span.set_status(Status::Ok);
        }

        Ok(task_id)
    }

    /// Push a job with a specific priority and trace context
    ///
    /// This method allows manual specification of the OpenTelemetry trace context
    /// that will be propagated to the job consumer. This is useful when you need
    /// to link job processing to a specific trace.
    ///
    /// Only available when the `otel` feature is enabled.
    #[cfg(feature = "otel")]
    pub async fn push_with_priority_and_context(
        &self,
        job: T,
        priority: Priority,
        context: &OtelContext,
    ) -> Result<TaskId, NatsPollError> {
        let tracer = global::tracer("apalis-nats");
        let mut span = tracer
            .span_builder("job.push")
            .with_kind(SpanKind::Producer)
            .with_attributes(vec![
                KeyValue::new("job.priority", priority.to_string()),
                KeyValue::new("job.namespace", self.config.namespace.clone()),
            ])
            .start_with_context(&tracer, context);

        let task_id = TaskId::new();
        let nats_job = NatsJob {
            id: task_id.clone(),
            data: job,
            priority,
            attempts: Attempt::new(),
            created_at: Utc::now(),
            namespace: Namespace::from(self.config.namespace.clone()),
        };

        let payload = serde_json::to_vec(&nats_job)?;
        let subject = self.get_subject(priority);

        // Prepare headers with provided trace context
        let mut headers = HeaderMap::new();

        if self.config.enable_tracing {
            global::get_text_map_propagator(|propagator| {
                let mut injector = NatsHeaderInjector::new(headers.clone());
                propagator.inject_context(context, &mut injector);
                headers = injector.into();
            });
        }

        // Publish with headers
        self.jetstream
            .publish_with_headers(subject, headers, Bytes::from(payload))
            .await
            .map_err(|e| NatsPollError::Nats(e.to_string()))?
            .await
            .map_err(|e| NatsPollError::Nats(e.to_string()))?;

        use OtelSpan;
        span.set_attribute(KeyValue::new("job.id", task_id.to_string()));
        span.set_status(Status::Ok);

        Ok(task_id)
    }

    /// Create or get a shared consumer for a specific priority
    async fn get_or_create_consumer(
        &self,
        priority: Priority,
    ) -> Result<consumer::Consumer<consumer::pull::Config>, NatsPollError> {
        let stream_name = self.get_stream_name(priority);
        // Use a shared consumer name for all workers of the same priority
        // This ensures work queue semantics - each message delivered to only one worker
        let consumer_name = format!("{}_{}_consumer", self.config.namespace, priority);

        let config = consumer::pull::Config {
            name: Some(consumer_name.clone()),
            durable_name: Some(consumer_name.clone()), // Make consumer durable
            // Work queue settings - ensure only one worker gets each message
            ack_policy: consumer::AckPolicy::Explicit,
            ack_wait: self.config.ack_wait,
            max_deliver: self.config.max_deliver,
            filter_subject: self.get_subject(priority),
            // Critical for work queue behavior - only deliver to one consumer
            deliver_policy: consumer::DeliverPolicy::All,
            // Control message delivery
            max_ack_pending: self.config.max_ack_pending,
            // Replay policy - start from beginning or new messages only
            replay_policy: consumer::ReplayPolicy::Instant,
            // Inactive threshold - remove consumer if inactive
            inactive_threshold: Duration::from_secs(300), // 5 minutes
            ..Default::default()
        };

        let stream = self
            .jetstream
            .get_stream(stream_name)
            .await
            .map_err(|e| NatsPollError::Nats(e.to_string()))?;
        stream
            .get_or_create_consumer(&consumer_name, config)
            .await
            .map_err(|e| NatsPollError::Nats(e.to_string()))
    }
}

impl<T> Storage for NatsStorage<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Job = T;
    type Error = NatsPollError;
    type Context = NatsContext;
    type Compact = Vec<u8>;

    async fn push_request(
        &mut self,
        req: Request<Self::Job, Self::Context>,
    ) -> Result<Parts<Self::Context>, Self::Error> {
        let task_id = self
            .push_with_priority(req.args, Priority::default())
            .await?;
        let mut parts = Parts::default();
        parts.task_id = task_id;
        parts.context = NatsContext::default();
        parts.namespace = Some(Namespace::from(self.config.namespace.clone()));
        Ok(parts)
    }

    async fn push_raw_request(
        &mut self,
        _req: Request<Self::Compact, Self::Context>,
    ) -> Result<Parts<Self::Context>, Self::Error> {
        // For now, we don't support raw requests directly
        Err(NatsPollError::Storage(
            "Raw requests not supported".to_string(),
        ))
    }

    async fn schedule_request(
        &mut self,
        _request: Request<Self::Job, Self::Context>,
        _on: i64,
    ) -> Result<Parts<Self::Context>, Self::Error> {
        // Scheduling is not yet implemented for NATS
        Err(NatsPollError::Storage(
            "Scheduling not yet implemented".to_string(),
        ))
    }

    async fn len(&mut self) -> Result<i64, Self::Error> {
        let mut total = 0u64;

        for priority in [Priority::High, Priority::Medium, Priority::Low] {
            let stream_name = self.get_stream_name(priority);
            match self.jetstream.get_stream(stream_name).await {
                Ok(mut stream) => {
                    if let Ok(info) = stream.info().await {
                        total += info.state.messages;
                    }
                }
                Err(_) => continue,
            }
        }

        Ok(total as i64)
    }

    async fn fetch_by_id(
        &mut self,
        _job_id: &TaskId,
    ) -> Result<Option<Request<Self::Job, Self::Context>>, Self::Error> {
        // NATS streams don't support direct lookup by ID
        Ok(None)
    }

    async fn update(&mut self, _job: Request<Self::Job, Self::Context>) -> Result<(), Self::Error> {
        // NATS streams don't support in-place updates
        Err(NatsPollError::Storage("Updates not supported".to_string()))
    }

    async fn reschedule(
        &mut self,
        _job: Request<Self::Job, Self::Context>,
        _wait: Duration,
    ) -> Result<(), Self::Error> {
        // Rescheduling would require republishing to the stream
        Err(NatsPollError::Storage(
            "Rescheduling not yet implemented".to_string(),
        ))
    }

    async fn is_empty(&mut self) -> Result<bool, Self::Error> {
        Ok(self.len().await? == 0)
    }

    async fn vacuum(&mut self) -> Result<usize, Self::Error> {
        // NATS streams automatically clean up acknowledged messages
        Ok(0)
    }
}

impl<T, C, Res> Ack<T, Res, C> for NatsStorage<T>
where
    T: Sync + Send + Serialize + DeserializeOwned + 'static,
    C: Codec<Compact = Vec<u8>> + Send + 'static,
    Res: Serialize + Sync + Send + 'static,
{
    type Context = NatsContext;
    type AckError = NatsPollError;

    async fn ack(
        &mut self,
        ctx: &Self::Context,
        response: &Response<Res>,
    ) -> Result<(), Self::AckError> {
        // Get the NATS message from context
        if let Some(msg) = ctx.message() {
            match &response.inner {
                Ok(_) => {
                    // Job succeeded - acknowledge the message
                    msg.ack()
                        .await
                        .map_err(|e| NatsPollError::Nats(e.to_string()))?;
                    log::debug!("Acknowledged message for task {}", response.task_id);
                }
                Err(e) => {
                    // Check if we should move to DLQ
                    let info = msg.info().map_err(|e| NatsPollError::Nats(e.to_string()))?;
                    let should_dlq = match e {
                        Error::Abort(_) => true, // Non-transient errors go to DLQ
                        _ => {
                            // Check if we've exceeded max deliveries
                            info.delivered as i64 >= self.config.max_deliver
                        }
                    };

                    if should_dlq && self.config.enable_dlq {
                        // Move to DLQ by publishing to DLQ stream
                        let dlq_subject = format!("{}.dlq", self.config.namespace);

                        // Determine DLQ reason
                        let dlq_reason = match e {
                            Error::Abort(_) => "abort_error",
                            _ => "max_deliver_exceeded",
                        };

                        // Create DLQ message with metadata
                        let dlq_job = json!({
                            "original_task_id": response.task_id.to_string(),
                            "error": e.to_string(),
                            "attempts": format!("{:?}", response.attempt),
                            "delivered_count": info.delivered,
                            "timestamp": chrono::Utc::now().to_rfc3339(),
                            "dlq_reason": dlq_reason,
                            "payload": msg.payload.clone(),
                        });

                        // Publish to DLQ
                        let body = serde_json::to_vec(&dlq_job)
                            .map_err(|e| NatsPollError::Serialization(e))?;
                        self.jetstream
                            .publish(dlq_subject, body.into())
                            .await
                            .map_err(|e| NatsPollError::Nats(e.to_string()))?
                            .await
                            .map_err(|e| NatsPollError::Nats(e.to_string()))?;

                        // Acknowledge the original message to remove it
                        msg.ack()
                            .await
                            .map_err(|e| NatsPollError::Nats(e.to_string()))?;

                        log::warn!(
                            "Moved task {} to DLQ after {} deliveries",
                            response.task_id,
                            info.delivered
                        );
                    } else {
                        // Check error type to determine acknowledgment strategy
                        match e {
                            Error::Abort(_) => {
                                // Non-transient error - terminate to prevent redelivery
                                msg.ack_with(jetstream::AckKind::Term)
                                    .await
                                    .map_err(|e| NatsPollError::Nats(e.to_string()))?;
                                log::warn!(
                                    "Terminated message for task {} due to abort error",
                                    response.task_id
                                );
                            }
                            _ => {
                                // Transient error - negative acknowledge for retry
                                msg.ack_with(jetstream::AckKind::Nak(None))
                                    .await
                                    .map_err(|e| NatsPollError::Nats(e.to_string()))?;
                                log::debug!(
                                    "Nacked message for task {} for retry (attempt {})",
                                    response.task_id,
                                    info.delivered
                                );
                            }
                        }
                    }
                }
            }
        } else {
            log::warn!("No NATS message in context for task {}", response.task_id);
        }
        Ok(())
    }
}

impl<T> Backend<Request<T, NatsContext>> for NatsStorage<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Stream = BoxStream<'static, Result<Option<Request<T, NatsContext>>, Error>>;
    type Layer =
        AckLayer<Sender<(NatsContext, Response<Vec<u8>>)>, T, NatsContext, JsonCodec<Vec<u8>>>;
    type Codec = JsonCodec<Vec<u8>>;

    fn poll(self, worker: &Worker<WorkerContext>) -> Poller<Self::Stream, Self::Layer> {
        let _worker_id = worker.id().to_string();

        // Create channels for job streaming and acknowledgments
        let (mut job_tx, job_rx) =
            mpsc::channel::<Result<Option<Request<T, NatsContext>>, Error>>(10);
        let (ack_tx, mut ack_rx) = mpsc::channel::<(NatsContext, Response<Vec<u8>>)>(10);

        // Create the AckLayer with the sender
        let layer = AckLayer::new(ack_tx);

        // Clone storage for the ack task
        let mut ack_storage = self.clone();

        // Spawn dedicated ack handling task
        #[cfg(feature = "tokio-comp")]
        tokio::spawn(async move {
            while let Some((ctx, resp)) = ack_rx.next().await {
                if let Err(e) = <NatsStorage<T> as Ack<T, Vec<u8>, JsonCodec<Vec<u8>>>>::ack(
                    &mut ack_storage,
                    &ctx,
                    &resp,
                )
                .await
                {
                    log::error!("Failed to acknowledge message: {}", e);
                }
            }
        });

        // Spawn the fetch loop (no select!, no always-ready branch)
        #[cfg(feature = "tokio-comp")]
        tokio::spawn(async move {
            loop {
                let mut job_found = false;
                // Try to fetch a job from each priority level in order
                for priority in [Priority::High, Priority::Medium, Priority::Low] {
                    // Use shared consumer for work queue semantics
                    if let Ok(consumer) = self.get_or_create_consumer(priority).await {
                        if let Ok(mut batch) = consumer.fetch().max_messages(1).messages().await {
                            if let Ok(Some(msg)) = batch.try_next().await {
                                match serde_json::from_slice::<NatsJob<T>>(&msg.payload) {
                                    Ok(job) => {
                                        let ctx = NatsContext::with_message(msg);
                                        let request = Request::new_with_ctx(job.data, ctx);
                                        // Send job to worker
                                        if job_tx.send(Ok(Some(request))).await.is_err() {
                                            return; // Channel closed, exit task
                                        }
                                        job_found = true;
                                        break; // Break the for loop to restart from high priority
                                    }
                                    Err(e) => {
                                        // Malformed payload: log and terminate to avoid endless redelivery
                                        log::error!("Failed to deserialize job payload: {}", e);
                                        if let Err(ack_err) =
                                            msg.ack_with(jetstream::AckKind::Term).await
                                        {
                                            log::error!(
                                                "Failed to term malformed message: {}",
                                                ack_err
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Apply backoff based on whether we found a job
                if job_found {
                    // Short wait when actively processing
                    tokio::time::sleep(Duration::from_millis(10)).await;
                } else {
                    // Longer wait when no jobs available
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        });

        // Return the job stream as a boxed stream
        let stream = job_rx.boxed();

        Poller::new_with_layer(
            stream,
            async {
                loop {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
            },
            layer,
        )
    }
}
