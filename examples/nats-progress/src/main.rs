use apalis::prelude::*;
use apalis_nats::{Config, NatsContext, NatsStorage, ProgressHeartbeatLayer};
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{info, Level};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LongTask {
    id: String,
    steps: u32,
    step_delay_secs: u64,
    #[serde(default)]
    panic_at_step: Option<u32>,
}

// Demonstrates automatic heartbeats extending ack_wait during long processing
async fn handle_with_heartbeat(
    job: LongTask,
    ctx: NatsContext,
    done: Data<Arc<tokio::sync::Notify>>,
) -> Result<(), Error> {
    info!("starting long task {} (heartbeat)", job.id);
    let _hb = ctx.start_progress_heartbeat(Duration::from_secs(15));

    for i in 1..=job.steps {
        info!("task {}: working step {}/{}", job.id, i, job.steps);
        tokio::time::sleep(Duration::from_secs(job.step_delay_secs)).await;
        if let Some(panic_at) = job.panic_at_step {
            if i == panic_at {
                panic!("Intentional panic for demo at step {}", i);
            }
        }
    }
    info!("completed long task {}", job.id);
    done.notify_one();
    Ok(())
}

// Demonstrates manual progress calls to extend ack_wait
async fn handle_with_manual_progress(
    job: LongTask,
    ctx: NatsContext,
    done: Data<Arc<tokio::sync::Notify>>,
) -> Result<(), Error> {
    info!("starting long task {} (manual progress)", job.id);
    for i in 1..=job.steps {
        info!("task {}: working step {}/{}", job.id, i, job.steps);
        tokio::time::sleep(Duration::from_secs(job.step_delay_secs)).await;
        // Extend ack_wait to avoid redelivery
        ctx.progress().await?;
        if let Some(panic_at) = job.panic_at_step {
            if i == panic_at {
                panic!("Intentional panic for demo at step {}", i);
            }
        }
    }
    info!("completed long task {}", job.id);
    done.notify_one();
    Ok(())
}

// Demonstrates relying on a ProgressHeartbeatLayer (no explicit progress in handler)
async fn handle_no_progress(
    job: LongTask,
    done: Data<Arc<tokio::sync::Notify>>,
) -> Result<(), Error> {
    tracing::info!("starting long task {} (layer)", job.id);
    for i in 1..=job.steps {
        tracing::info!("task {}: working step {}/{}", job.id, i, job.steps);
        tokio::time::sleep(Duration::from_secs(job.step_delay_secs)).await;
        if let Some(panic_at) = job.panic_at_step {
            if i == panic_at {
                panic!("Intentional panic for demo at step {}", i);
            }
        }
    }
    tracing::info!("completed long task {}", job.id);
    done.notify_one();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Logging
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("info,apalis=info,apalis_nats=debug"))
        .unwrap();
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_env_filter(filter)
        .with_target(true)
        .init();

    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    info!("connecting to NATS at {}", nats_url);
    let client = apalis_nats::connect(&nats_url).await?;

    // Namespace: configurable or unique per run to avoid DLQ accumulation
    let namespace = std::env::var("NATS_NAMESPACE").unwrap_or_else(|_| {
        format!(
            "apalis_progress_example_{}",
            uuid::Uuid::new_v4().to_string().replace('-', "_")
        )
    });

    // Optionally purge any existing streams for the namespace
    if std::env::var("NATS_PURGE").ok().as_deref() == Some("1") {
        let js = async_nats::jetstream::new(client.clone());
        for name in [
            format!("{}_high", &namespace),
            format!("{}_medium", &namespace),
            format!("{}_low", &namespace),
            format!("{}_dlq", &namespace),
        ] {
            let _ = js.delete_stream(name).await;
        }
    }

    // Configure ack_wait larger than heartbeat/progress interval
    let config = Config {
        namespace: namespace.clone(),
        ack_wait: Duration::from_secs(60), // allow at least 60s between progress updates
        max_deliver: 5,
        enable_dlq: true,
        ..Default::default()
    };
    let storage = NatsStorage::new_with_config(client, config).await?;

    // Determine if we should enqueue a panic demo job
    let panic_at_env = std::env::var("PANIC_AT_STEP").ok();
    let mut is_panic_demo = false;
    if let Some(step_str) = panic_at_env.as_ref() {
        if let Ok(step) = step_str.parse::<u32>() {
            is_panic_demo = true;
            // Push panic job first with shorter steps so it runs promptly
            let panic_job = LongTask {
                id: "long-panic".to_string(),
                steps: step.max(3),
                step_delay_secs: 2,
                panic_at_step: Some(step),
            };
            let panic_id = storage.clone().push(panic_job).await?;
            info!("queued panic demo task with id {:?} (PANIC_AT_STEP={})", panic_id, step);
        }
    }

    // Push a long-running task (~3 minutes total)
    let job = LongTask {
        id: "long-1".to_string(),
        steps: 12,
        step_delay_secs: 15,
        panic_at_step: None,
    };
    let task_id = storage.clone().push(job).await?;
    info!("queued long task with id {:?}", task_id);

    // Choose handler variant via env var (default: layer)
    // modes: layer | heartbeat | manual
    let mode = std::env::var("PROGRESS_MODE").unwrap_or_else(|_| "layer".into());
    info!("starting worker in {} mode", mode);

    let monitor = Monitor::new();
    // Notify used to stop the monitor automatically when the success job completes
    let done = Arc::new(tokio::sync::Notify::new());
    let monitor = match mode.as_str() {
        "manual" => monitor.register(
            WorkerBuilder::new("nats-progress-worker-manual")
                .concurrency(if is_panic_demo { 2 } else { 1 })
                .data(done.clone())
                .catch_panic()
                .backend(storage)
                .build_fn(handle_with_manual_progress),
        ),
        "heartbeat" => monitor.register(
            WorkerBuilder::new("nats-progress-worker-heartbeat")
                .concurrency(if is_panic_demo { 2 } else { 1 })
                .data(done.clone())
                .catch_panic()
                .backend(storage)
                .build_fn(handle_with_heartbeat),
        ),
        _ => monitor.register(
            WorkerBuilder::new("nats-progress-worker-layer")
                .concurrency(if is_panic_demo { 2 } else { 1 })
                .option_layer(Some(ProgressHeartbeatLayer::new(Duration::from_secs(15))))
                .data(done.clone())
                .catch_panic()
                .backend(storage)
                .build_fn(handle_no_progress),
        ),
    };
    if is_panic_demo {
        let wait_secs: u64 = std::env::var("PANIC_WAIT_SECONDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20);
        info!("panic demo mode: running for {}s before verifying DLQ", wait_secs);
        monitor
            .shutdown_timeout(Duration::from_secs(5))
            .run_with_signal(async move {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {},
                    _ = tokio::time::sleep(Duration::from_secs(wait_secs)) => {},
                }
                Ok(())
            })
            .await?;

        // Verify DLQ after run
        let client2 = apalis_nats::connect(&nats_url).await?;
        let js = async_nats::jetstream::new(client2);
        let dlq_stream = format!("{}_dlq", namespace);
        if let Ok(mut stream) = js.get_stream(dlq_stream.clone()).await {
            if let Ok(info) = stream.info().await {
                info!("DLQ {} contains {} message(s)", dlq_stream, info.state.messages);
            }
        } else {
            info!("DLQ stream {} not found", dlq_stream);
        }
    } else {
        monitor
            .shutdown_timeout(Duration::from_secs(5))
            .run_with_signal(async move {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {},
                    _ = done.notified() => {},
                }
                Ok(())
            })
            .await?;
    }

    Ok(())
}
