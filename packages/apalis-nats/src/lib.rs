#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
//! apalis storage using NATS JetStream as a backend
//! ```rust,no_run
//! use apalis::prelude::*;
//! use apalis_nats::NatsStorage;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Clone, Deserialize, Serialize)]
//! struct Email {
//!     to: String,
//! }
//!
//! async fn send_email(job: Email) -> Result<(), Error> {
//!     // Send the email
//!     println!("Sending email to: {}", job.to);
//!     Ok(())
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let nats_url = std::env::var("NATS_URL")
//!         .unwrap_or_else(|_| "nats://localhost:4222".to_string());
//!     let client = apalis_nats::connect(&nats_url).await.expect("Could not connect");
//!     let mut storage = NatsStorage::new(client).await.expect("Could not create storage");
//!     
//!     // Push a job
//!     let email = Email { to: "user@example.com".to_string() };
//!     storage.push(email).await.expect("Could not push job");
//!     
//!     // Create and run worker
//!     let worker = WorkerBuilder::new("email-worker")
//!         .backend(storage.clone())
//!         .build_fn(send_email);
//!
//!     Monitor::new()
//!         .register(worker)
//!         .run()
//!         .await
//!         .expect("Could not run monitor");
//! }
//! ```

mod expose;
mod storage;

pub use async_nats::{Client, ConnectError, ConnectOptions};
pub use storage::{
    connect, connect_with_credentials, connect_with_options, connect_with_user_pass, Config,
    NatsContext, NatsPollError, NatsQueueInfo, NatsStorage, Priority,
};
