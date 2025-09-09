use apalis::prelude::*;
use apalis_nats::{Config, NatsStorage, Priority};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{info, Level};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Job {
    name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Tracing with a sensible default filter
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

    // Use a unique namespace for a clean demo
    let namespace = format!(
        "apalis_priority_demo_{}",
        uuid::Uuid::new_v4().to_string().replace('-', "_")
    );
    let storage = NatsStorage::new_with_config(
        client,
        Config { namespace: namespace.clone(), ..Default::default() },
    )
    .await?;

    // Track execution order
    let order = Arc::new(Mutex::new(Vec::<String>::new()));
    let order_clone = order.clone();

    async fn handle(job: Job, order: Data<Arc<Mutex<Vec<String>>>>) -> Result<(), Error> {
        info!("processing {}", job.name);
        order.lock().await.push(job.name);
        // Simulate some work
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    }

    // Enqueue: low first, then medium, then high
    for i in 1..=2 {
        storage
            .push_with_priority(Job { name: format!("low-{}", i) }, Priority::Low)
            .await?;
    }
    for i in 1..=2 {
        storage
            .push_with_priority(Job { name: format!("medium-{}", i) }, Priority::Medium)
            .await?;
    }
    for i in 1..=2 {
        storage
            .push_with_priority(Job { name: format!("high-{}", i) }, Priority::High)
            .await?;
    }

    // Build a single-concurrency worker to highlight ordering deterministically
    let worker = WorkerBuilder::new("nats-priority-worker")
        .concurrency(1)
        .data(order_clone)
        .backend(storage.clone())
        .build_fn(handle);

    // Run worker until all 6 jobs complete
    let done = Arc::new(tokio::sync::Notify::new());
    let done_clone = done.clone();
    let order2 = order.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if order2.lock().await.len() >= 6 {
                done_clone.notify_one();
                break;
            }
        }
    });

    Monitor::new()
        .register(worker)
        .shutdown_timeout(Duration::from_secs(2))
        .run_with_signal(async move {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = done.notified() => {},
            }
            Ok(())
        })
        .await?;

    let final_order = order.lock().await.clone();
    println!("\nObserved order: {:?}", final_order);
    println!("Expected: [high-*, high-*] then [medium-*] then [low-*]");
    Ok(())
}

