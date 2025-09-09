use apalis::prelude::*;
use apalis_nats::{Config, NatsStorage};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{info, Level};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Job {
    name: String,
    panic: bool,
}

async fn handler(job: Job) -> Result<(), Error> {
    if job.panic {
        panic!("intentional panic for demo");
    }
    info!("processed {} without panic", job.name);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Tracing
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("info,apalis_nats=debug"))
        .unwrap();
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_env_filter(filter)
        .with_target(true)
        .init();

    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let client = apalis_nats::connect(&nats_url).await?;

    // Unique namespace per run
    let namespace = format!(
        "apalis_catch_panic_demo_{}",
        uuid::Uuid::new_v4().to_string().replace('-', "_")
    );
    let storage = NatsStorage::new_with_config(
        client.clone(),
        Config {
            namespace: namespace.clone(),
            enable_dlq: true,
            ..Default::default()
        },
    )
    .await?;

    // Queue a normal job and a panic job
    storage
        .clone()
        .push(Job { name: "ok".into(), panic: false })
        .await?;
    storage
        .clone()
        .push(Job { name: "will-panic".into(), panic: true })
        .await?;

    // Worker with catch_panic layer -> panics become Error::Abort and are DLQ’d/Terminated
    let worker = WorkerBuilder::new("nats-catch-panic-worker")
        .concurrency(1)
        .catch_panic()
        .backend(storage.clone())
        .build_fn(handler);

    // Run briefly, then stop and inspect DLQ
    let monitor = Monitor::new().register(worker);
    let run = tokio::spawn(async move {
        monitor
            .shutdown_timeout(Duration::from_secs(2))
            .run_with_signal(async {
                tokio::time::sleep(Duration::from_secs(5)).await;
                Ok(())
            })
            .await
            .ok();
    });
    let _ = run.await;

    // Inspect DLQ
    let js = async_nats::jetstream::new(client);
    let dlq_stream = format!("{}_dlq", namespace);
    match js.get_stream(dlq_stream.clone()).await {
        Ok(mut s) => {
            if let Ok(info) = s.info().await {
                println!(
                    "DLQ stream {} contains {} message(s)",
                    dlq_stream, info.state.messages
                );
            }
        }
        Err(e) => {
            println!("No DLQ stream found ({}): {:?}", dlq_stream, e);
        }
    }

    Ok(())
}

