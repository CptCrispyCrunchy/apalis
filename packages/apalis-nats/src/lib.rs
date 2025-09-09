#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
//! NATS JetStream storage for Apalis jobs.
//! 
//! - Priority queues (high/medium/low)
//! - DLQ routing on abort errors or after max deliveries
//! - At-least-once delivery, configurable retries with backoff
//! - Optional OpenTelemetry W3C trace propagation
//! - Long-running jobs: progress heartbeats to extend `ack_wait`
//!
//! Basic usage
//! ```rust,no_run
//! use apalis::prelude::*;
//! use apalis_nats::NatsStorage;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Clone, Deserialize, Serialize)]
//! struct Email { to: String }
//!
//! async fn send_email(job: Email) -> Result<(), Error> {
//!     println!("Sending email to: {}", job.to);
//!     Ok(())
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = apalis_nats::connect("nats://localhost:4222").await?;
//!     let storage = NatsStorage::new(client).await?;
//!
//!     storage.push(Email { to: "user@example.com".into() }).await?;
//!
//!     let worker = WorkerBuilder::new("email-worker")
//!         .backend(storage.clone())
//!         .build_fn(send_email);
//!
//!     Monitor::new().register(worker).run().await?;
//!     Ok(())
//! }
//! ```
//!
//! Long-running jobs (auto-heartbeat layer)
//! ```rust,no_run
//! use apalis::prelude::*;
//! use apalis_nats::{NatsStorage, ProgressHeartbeatLayer, Config};
//! use serde::{Deserialize, Serialize};
//! use std::time::Duration;
//!
//! #[derive(Debug, Clone, Deserialize, Serialize)]
//! struct Heavy { size: u64 }
//!
//! async fn do_work(job: Heavy) -> Result<(), Error> {
//!     // Handler does not call progress() explicitly; the layer handles it.
//!     tokio::time::sleep(Duration::from_secs(45)).await;
//!     Ok(())
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = apalis_nats::connect("nats://localhost:4222").await?;
//!     let storage = NatsStorage::new_with_config(client, Config { ack_wait: Duration::from_secs(60), ..Default::default() }).await?;
//!
//!     let worker = WorkerBuilder::new("heavy-worker")
//!         .option_layer(Some(ProgressHeartbeatLayer::new(Duration::from_secs(15))))
//!         .backend(storage.clone())
//!         .build_fn(do_work);
//!
//!     Monitor::new().register(worker).run().await?;
//!     Ok(())
//! }
//! ```

mod expose;
mod layers;
mod storage;

pub use async_nats::{Client, ConnectError, ConnectOptions};
pub use storage::{
    connect, connect_with_credentials, connect_with_options, connect_with_user_pass, Config,
    NatsContext, NatsPollError, NatsQueueInfo, NatsStorage, Priority,
};
pub use crate::layers::ProgressHeartbeatLayer;
