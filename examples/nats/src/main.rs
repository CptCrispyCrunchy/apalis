use apalis::prelude::*;
use apalis_nats::{Config, NatsStorage, Priority};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{info, instrument};

// OpenTelemetry imports for tracing example
#[cfg(feature = "otel")]
use opentelemetry::global;
#[cfg(feature = "otel")]
use opentelemetry::trace::{TraceContextExt, Tracer};
#[cfg(feature = "otel")]
use opentelemetry::KeyValue;
#[cfg(feature = "otel")]
use opentelemetry_otlp::WithExportConfig;
#[cfg(feature = "otel")]
use opentelemetry_sdk::{
    runtime::TokioCurrentThread,
    trace::{self, RandomIdGenerator, Sampler},
    Resource,
};
#[cfg(feature = "otel")]
use tracing_opentelemetry::OpenTelemetryLayer;
#[cfg(feature = "otel")]
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Email {
    to: String,
    subject: String,
    body: String,
    created_at: DateTime<Utc>,
}

#[instrument(skip(email), fields(email.to = %email.to, email.subject = %email.subject))]
async fn send_email(email: Email) -> Result<(), Error> {
    info!(
        "📧 Sending email to: {}, subject: {}",
        email.to, email.subject
    );

    // Simulate email sending
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Access trace context if available
    #[cfg(feature = "otel")]
    if let Some(nats_ctx) = _ctx.data_opt::<apalis_nats::NatsContext>() {
        if let Some(trace_ctx) = nats_ctx.trace_context() {
            info!("Processing job with trace context from producer");
            // The span is automatically linked to the parent context
        }
    }

    info!("✅ Email sent successfully to: {}", email.to);
    Ok(())
}

#[instrument(skip(storage))]
async fn produce_jobs(storage: &NatsStorage<Email>) -> Result<(), Box<dyn std::error::Error>> {
    info!("🚀 Producing jobs...");

    // Push high priority emails
    for i in 1..=3 {
        let email = Email {
            to: format!("urgent-user-{}@example.com", i),
            subject: format!("🔴 URGENT: Important notification #{}", i),
            body: "This is a high priority message".to_string(),
            created_at: Utc::now(),
        };

        let task_id = storage.push_with_priority(email, Priority::High).await?;
        info!("  📤 High priority job queued: {}", task_id);
    }

    // Push medium priority emails
    for i in 1..=5 {
        let email = Email {
            to: format!("user-{}@example.com", i),
            subject: format!("🟡 Regular notification #{}", i),
            body: "This is a medium priority message".to_string(),
            created_at: Utc::now(),
        };

        let task_id = storage.push_with_priority(email, Priority::Medium).await?;
        info!("  📤 Medium priority job queued: {}", task_id);
    }

    // Push low priority emails
    for i in 1..=7 {
        let email = Email {
            to: format!("newsletter-{}@example.com", i),
            subject: format!("🟢 Newsletter #{}", i),
            body: "This is a low priority newsletter".to_string(),
            created_at: Utc::now(),
        };

        let task_id = storage.push_with_priority(email, Priority::Low).await?;
        info!("  📤 Low priority job queued: {}", task_id);
    }

    info!("✅ All jobs produced successfully!");
    Ok(())
}

#[cfg(feature = "otel")]
fn init_telemetry() -> Result<(), Box<dyn std::error::Error>> {
    // Configure OpenTelemetry with OTLP exporter
    let otlp_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string());

    info!(
        "📡 Configuring OpenTelemetry with endpoint: {}",
        otlp_endpoint
    );

    let otlp_exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint(otlp_endpoint);

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(otlp_exporter)
        .with_trace_config(
            trace::config()
                .with_sampler(Sampler::AlwaysOn)
                .with_id_generator(RandomIdGenerator::default())
                .with_resource(Resource::new(vec![
                    KeyValue::new("service.name", "apalis-nats-example"),
                    KeyValue::new("service.version", "0.1.0"),
                ])),
        )
        .install_batch(TokioCurrentThread)?;

    // Set global tracer provider
    global::set_tracer_provider(tracer.provider().unwrap().clone());

    // Configure tracing subscriber with OpenTelemetry layer
    let otel_layer = OpenTelemetryLayer::new(tracer);

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(otel_layer)
        .init();

    info!("✅ OpenTelemetry tracing initialized");
    Ok(())
}

#[cfg(not(feature = "otel"))]
fn init_telemetry() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize basic tracing without OpenTelemetry
    tracing_subscriber::fmt().with_target(false).init();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing (with or without OpenTelemetry)
    init_telemetry()?;

    info!("🎯 Starting NATS JetStream job queue example");

    #[cfg(feature = "otel")]
    info!("🔍 OpenTelemetry tracing is ENABLED");

    #[cfg(not(feature = "otel"))]
    info!("⚠️ OpenTelemetry tracing is DISABLED (enable with --features otel)");

    // Connect to NATS
    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    info!("📡 Connecting to NATS at: {}", nats_url);

    let client = apalis_nats::connect(&nats_url).await?;
    info!("✅ Connected to NATS successfully");

    // Configure storage
    let config = Config {
        namespace: "apalis_example".to_string(),
        max_deliver: 3,
        ack_wait: Duration::from_secs(30),
        num_replicas: 1,
        enable_dlq: true,
        ..Default::default()
    };

    #[cfg(feature = "otel")]
    {
        config.enable_tracing = true;
    }

    // Create storage
    let storage = NatsStorage::new_with_config(client, config).await?;
    info!("📦 NATS storage initialized");

    // Produce some jobs
    produce_jobs(&storage).await?;

    // Give a moment for jobs to be persisted
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Create and run worker
    info!("👷 Starting worker...");

    let worker = WorkerBuilder::new("email-worker")
        .concurrency(2)
        .backend(storage.clone())
        .build_fn(send_email);

    // Run worker with monitoring
    Monitor::new()
        .register(worker)
        .shutdown_timeout(Duration::from_secs(5))
        .run_with_signal(async {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to listen for ctrl+c");
            info!("🛑 Shutting down gracefully...");
            Ok(())
        })
        .await?;

    // Shutdown OpenTelemetry provider
    #[cfg(feature = "otel")]
    {
        info!("📊 Flushing OpenTelemetry traces...");
        global::shutdown_tracer_provider();
    }

    info!("👋 Goodbye!");
    Ok(())
}
