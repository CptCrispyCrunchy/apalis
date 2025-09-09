# apalis-nats

NATS JetStream backend for the Apalis job processing library.

## Features

- **Priority Queues**: Three-level priority system (High, Medium, Low) with separate NATS streams
- **Dead Letter Queue (DLQ)**: Automatic routing of failed jobs after max retries
- **Distributed Tracing**: Full OpenTelemetry support with W3C trace context propagation
- **At-least-once Delivery**: Reliable job processing with configurable retries
- **Horizontal Scaling**: Multiple workers can process jobs concurrently
- **Graceful Shutdown**: Worker monitoring and controlled shutdown

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
apalis = "0.7"
apalis-nats = "0.7"
```

With OpenTelemetry support:

```toml
apalis-nats = { version = "0.7", features = ["otel"] }
```

## Usage

### Basic Example

```rust
use apalis::prelude::*;
use apalis_nats::{NatsStorage, Config};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct EmailJob {
    to: String,
    subject: String,
}

async fn send_email(job: EmailJob) -> Result<(), Error> {
    println!("Sending email to {}: {}", job.to, job.subject);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to NATS
    let client = apalis_nats::connect("nats://localhost:4222").await?;
    
    // Create storage
    let storage = NatsStorage::new(client).await?;
    
    // Push a job
    let job = EmailJob {
        to: "user@example.com".to_string(),
        subject: "Welcome!".to_string(),
    };
    storage.push(job).await?;
    
    // Create and run worker
    let worker = WorkerBuilder::new("email-worker")
        .backend(storage.clone())
        .build_fn(send_email);
    
    Monitor::new()
        .register(worker)
        .run()
        .await?;
    
    Ok(())
}
```

### Authentication

Multiple authentication methods are supported:

```rust
// Basic connection without auth
let client = apalis_nats::connect("nats://localhost:4222").await?;

// With credentials file (.creds)
let client = apalis_nats::connect_with_credentials(
    "nats://connect.ngs.global",
    "path/to/my.creds"
).await?;

// With username and password
let client = apalis_nats::connect_with_user_pass(
    "nats://localhost:4222",
    "myuser",
    "mypassword"
).await?;

// With custom options (NKey, JWT, client name, etc.)
use apalis_nats::ConnectOptions;
let client = apalis_nats::connect_with_options(
    "nats://localhost:4222",
    ConnectOptions::new()
        .name("my-worker")
        .credentials_file("path/to/my.creds").await?
).await?;
```

### Priority Queues

Jobs can be pushed with different priorities:

```rust
use apalis_nats::Priority;

// High priority - processed first
storage.push_with_priority(urgent_job, Priority::High).await?;

// Medium priority (default)
storage.push_with_priority(normal_job, Priority::Medium).await?;

// Low priority - processed when higher queues are empty
storage.push_with_priority(background_job, Priority::Low).await?;
```

### Configuration

```rust
use std::time::Duration;
use apalis_nats::Config;

let config = Config {
    namespace: "my_app".to_string(),
    max_deliver: 5,                        // Max retry attempts
    ack_wait: Duration::from_secs(30),     // Time to process job
    num_replicas: 3,                        // Stream replicas
    enable_dlq: true,                       // Enable dead letter queue
    #[cfg(feature = "otel")]
    enable_tracing: true,                   // Enable OpenTelemetry
};

let storage = NatsStorage::new_with_config(client, config).await?;
```

### OpenTelemetry Tracing

When the `otel` feature is enabled, traces are automatically propagated from producers to consumers:

```rust
// Producer side - trace context is automatically injected
let task_id = storage.push(job).await?;

// Consumer side - trace context is automatically extracted
async fn process_job(job: MyJob, ctx: Context<NatsContext>) -> Result<(), Error> {
    // Access trace context if needed
    if let Some(nats_ctx) = ctx.data_opt::<NatsContext>() {
        if let Some(trace_ctx) = nats_ctx.trace_context() {
            // Trace is linked to parent
        }
    }
    Ok(())
}
```

### Manual Job Control

Access the NATS message context for fine-grained control:

```rust
async fn process_with_control(
    job: MyJob,
    ctx: Context<NatsContext>,
) -> Result<(), Error> {
    if let Some(nats_ctx) = ctx.data_opt::<NatsContext>() {
        if should_retry {
            // Negative acknowledgment - job will be retried
            nats_ctx.nack().await?;
        } else if permanent_failure {
            // Terminate - send to DLQ if configured
            nats_ctx.term().await?;
        } else {
            // Success - acknowledge completion
            nats_ctx.ack().await?;
        }
    }
    Ok(())
}
```

## Architecture

### Stream Organization

The NATS backend creates separate JetStream streams for each priority level:

- `{namespace}_high` - High priority jobs
- `{namespace}_medium` - Medium priority jobs
- `{namespace}_low` - Low priority jobs
- `{namespace}_dlq` - Dead letter queue (if enabled)

### Worker Polling

Workers poll streams in priority order:
1. Check high priority stream
2. If empty, check medium priority
3. If empty, check low priority
4. Sleep briefly if all queues are empty

This ensures high-priority jobs are always processed first while preventing starvation of lower priorities.

## Testing

Run integration tests with Docker:

```bash
cargo test --package apalis-nats
```

The tests use testcontainers to automatically spin up a NATS JetStream instance.

## Requirements

- NATS server with JetStream enabled
- Rust 1.75+

## License

MIT OR Apache-2.0