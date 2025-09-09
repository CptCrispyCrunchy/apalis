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

### DLQ Message Format

When a job is sent to the Dead Letter Queue (DLQ), the crate publishes a JSON object to the `{namespace}.dlq` subject with the following fields:

```json
{
  "original_task_id": "<TaskId as string>",
  "error": "string description of the last error",
  "attempts": "Debug representation of Attempt",
  "delivered_count": 3,
  "timestamp": "RFC3339 timestamp",
  "dlq_reason": "abort_error | max_deliver_exceeded",
  "payload": "<base64-encoded bytes>"
}
```

- original_task_id: The original task ID associated with the message.
- error: The error string returned by the handler on the final attempt.
- attempts: The Attempt debug string capturing retry metadata.
- delivered_count: Number of deliveries recorded by JetStream for this message.
- timestamp: Time the DLQ entry was created.
- dlq_reason: Reason for routing to DLQ.
  - abort_error: The handler returned a non-transient Error::Abort(_), so the job was terminated immediately.
  - max_deliver_exceeded: The message exceeded `max_deliver` attempts and failed again.
- payload: Base64-encoded original message payload as received from NATS (i.e., the serialized NatsJob<T> bytes). This allows reinspection or manual replay if necessary.

Notes:
- The crate publishes to the DLQ first and only then acknowledges the original message. If publish fails, the original message is not acked and will redeliver, ensuring DLQ routing is retried.
- If DLQ is disabled (`enable_dlq = false`), `Error::Abort(_)` results in a Term ack (no redelivery), while other errors use Nak for retry until `max_deliver`.

## Dead Letter Queue (DLQ) Message Format

When jobs fail after maximum retries or encounter non-transient errors, they are moved to the DLQ with the following JSON structure:

| Field | Type | Description |
|-------|------|-------------|
| `original_task_id` | String | The original task ID (ULID format) |
| `error` | String | The error message that caused the failure |
| `attempts` | String | Debug representation of attempt count |
| `delivered_count` | Number | Number of delivery attempts by NATS |
| `timestamp` | String | RFC3339 timestamp when moved to DLQ |
| `payload` | Bytes | Original NATS message payload (serialized `NatsJob<T>`) |

**Note:** The `payload` field contains the exact bytes of the original NATS message, which is the serialized `NatsJob<T>` structure. This allows for offline inspection and potential requeuing of failed jobs. When serialized to JSON, these bytes are base64-encoded by serde_json.

### Example DLQ Message

```json
{
  "original_task_id": "01K4QGM32F0NBKHDG1D89X4212",
  "error": "Connection timeout",
  "attempts": "Attempt(5)",
  "delivered_count": 5,
  "timestamp": "2024-01-15T10:30:45.123Z",
  "payload": [123, 34, 105, 100, 34, ...] // Raw bytes of NatsJob<T>
}
```

### Requeuing Failed Jobs

To requeue a job from the DLQ, you can deserialize the `payload` field back into a `NatsJob<T>` and republish it to the appropriate priority stream.

## Testing

Run integration tests with Docker:

```bash
cargo test --package apalis-nats
```

The tests use testcontainers to automatically spin up a NATS JetStream instance.

## Scheduling

**Note:** Scheduled and delayed jobs are not currently supported in the NATS JetStream backend when using pull consumers. The `schedule_request` and `reschedule` methods will return an error indicating this limitation.

### Alternatives for Scheduling

If you need delayed job execution, consider these alternatives:

1. **Separate Scheduler Service**: Implement a dedicated scheduler that tracks job timings and publishes to NATS at the appropriate time.
2. **NATS Key-Value Store**: Use NATS KV with TTL/expiration to trigger job republishing.
3. **Application-Level Delay**: Handle delays in your job processing logic by checking timestamps and re-enqueueing if needed.

Future versions may implement scheduling support using delayed subjects or JetStream timers, but this requires careful consideration of durability and failure scenarios.

## Requirements

- NATS server with JetStream enabled
- Rust 1.75+

## License

MIT OR Apache-2.0
