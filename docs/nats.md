Here is a reference implementation and design document for a job queue based on NATS, tailored to your requirements.

## 1. Introduction

This document outlines the design for a robust and reliable job queue system built on top of NATS JetStream. The implementation will be in Rust, leveraging the `apalis` crate for the job processing framework and the `async-nats` library for NATS communication. The system will support configurable retries, dead-letter queues (DLQ) for non-transient errors, priority job handling, and comprehensive observability through OpenTelemetry W3C tracing.

### 1.1. Requirements

*   **Robust and Reliable Processing**: Jobs must be persisted and processed in an "at-least-once" delivery model.
*   **Dead-Letter Queue (DLQ)**: Jobs that consistently fail due to non-transient errors should be moved to a DLQ for manual inspection.
*   **Configurable Retries**: The number of retries for a failed job and the backoff strategy should be configurable.
*   **Job Priorities**: The system must support multiple priority levels, ensuring that all levels make progress at different rates.
*   **Rust Implementation**: The producers and consumers (workers) will be written in Rust, using `apalis` as a foundation.
*   **OpenTelemetry (OTel) Tracing**: Full end-to-end distributed tracing using the W3C Trace Context standard is required.

### 1.2. Core Technologies

*   **NATS JetStream**: The persistence layer for the job queue. JetStream provides streaming, message durability, and at-least-once delivery semantics.
*   **Rust**: The programming language for both job producers and consumers.
*   **`async-nats`**: The official asynchronous Rust client for NATS.
*   **`apalis`**: A background job processing library for Rust. We will create a custom storage provider for NATS.
*   **`opentelemetry` and `opentelemetry-nats`**: For implementing distributed tracing.

## 2. System Architecture

The architecture is designed around NATS JetStream as the central message bus. Producers enqueue jobs, and workers (consumers) dequeue and execute them.

  *(A conceptual diagram would be placed here showing the flow from Producer -> NATS Streams (High/Medium/Low Priority) -> Rust Worker -> DLQ Stream)*

### 2.1. NATS JetStream Configuration

#### 2.1.1. Streams for Priorities

NATS JetStream does not have a built-in concept of message priority within a single stream. To address this, we will create a separate NATS stream for each priority level. This is the recommended approach to ensure that higher-priority jobs can be processed before lower-priority ones.

For example, we'll define three streams:

*   `jobs_high`
*   `jobs_medium`
*   `jobs_low`

Each stream will be configured with a `WorkQueuePolicy` to ensure that a job is only processed by one consumer, even when scaled horizontally.

#### 2.1.2. Dead-Letter Queue (DLQ) Stream

A dedicated stream, `jobs_dlq`, will serve as the dead-letter queue. This stream will store jobs that have failed all their processing attempts.

### 2.2. Producer

The producer is any application that needs to offload a task. It will be responsible for:

1.  Serializing the job payload (e.g., to JSON).
2.  Injecting the current OpenTelemetry W3C trace context into the NATS message headers.
3.  Publishing the job to the appropriate priority-specific NATS subject (e.g., `jobs.high`, `jobs.medium`).

### 2.3. Consumer (Worker)

The worker is a Rust application that will:

1.  Fetch jobs from the NATS streams.
2.  Extract the OpenTelemetry trace context from the message headers to continue the trace.
3.  Execute the job logic.
4.  Acknowledge the message on successful completion.
5.  Negatively acknowledge (`Nack`) the message on a transient failure to trigger a redelivery.
6.  Acknowledge with termination (`AckTerm`) on a non-transient failure.

## 3. Detailed Design

### 3.1. Priority Queue Implementation

Our `apalis` worker will need to be aware of the different priority streams. Instead of a single source of jobs, the worker will pull from multiple NATS consumers, prioritizing the higher-level streams.

The worker's main loop will look something like this:

1.  Attempt to fetch a batch of jobs from the `jobs_high` stream. If jobs are available, process them.
2.  If the `jobs_high` stream is empty, attempt to fetch from the `jobs_medium` stream.
3.  If both `jobs_high` and `jobs_medium` are empty, fetch from `jobs_low`.

This ensures that as long as there are high-priority jobs, the worker is busy with them. Lower-priority jobs are processed when higher-priority queues are idle, guaranteeing that all priority levels make progress.

### 3.2. Robust Processing and Retries

NATS JetStream consumers can be configured with a `MaxDeliver` option, which specifies the maximum number of times a message will be delivered. This provides a server-side mechanism for retries.

When a worker processes a job:

*   **Success**: The worker sends an `Ack` to NATS, and the message is removed from the stream.
*   **Transient Failure** (e.g., a temporary network issue, a database deadlock): The worker sends a `Nack`. NATS will then redeliver the message after a configured backoff period.
*   **Non-Transient Failure** (e.g., invalid job arguments, a bug in the code): The worker should recognize this and send an `AckTerm`. This tells NATS to stop redelivering the message.

### 3.3. Dead-Letter Queue (DLQ)

NATS JetStream publishes advisory messages when certain events occur. When a message's delivery count exceeds the `MaxDeliver` setting on its consumer, an advisory is published to a subject like `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<STREAM>.<CONSUMER>`.

We will have a dedicated, lightweight worker that subscribes to these advisories. When it receives a `MAX_DELIVERIES` advisory, it will:

1.  Extract the original message's sequence number from the advisory.
2.  Fetch the full message from the original stream using this sequence number.
3.  Add metadata about the failure (e.g., timestamp, original stream).
4.  Publish this enriched message to the `jobs_dlq` stream for later inspection.

### 3.4. `apalis` NATS Storage Adapter

To integrate with `apalis`, we will need to implement the `Storage` trait. This custom adapter will be responsible for the communication with NATS.

Key components of the adapter:

*   `push`: Publishes a new job to the appropriate NATS subject based on its priority.
*   `consume`: Fetches jobs from the NATS streams, handling the priority polling logic.
*   `ack`: Acknowledges a message in NATS upon successful job completion.

The storage adapter will use the `async-nats` crate internally.

### 3.5. OpenTelemetry Tracing

End-to-end tracing will be achieved by propagating the W3C Trace Context.

1.  **Producer**:
    *   Before publishing a job, use the `opentelemetry::global::get_text_map_propagator` to inject the current `Context` into a `HashMap`.
    *   This `HashMap` is then converted to NATS headers and attached to the message. The `opentelemetry-nats` crate provides a `NatsHeaderInjector` for this purpose.

2.  **Consumer (Worker)**:
    *   Upon receiving a message, use the `NatsHeaderExtractor` from `opentelemetry-nats` to extract the trace context from the message headers.
    *   Create a new tracing span for the job execution, linking it to the parent context extracted from the message. This ensures the trace is connected across services.

## 4. Rust Code Implementation (Conceptual)

Here are some illustrative code snippets.

### 4.1. Producer: Enqueuing a Job

```rust
use async_nats::jetstream;
use opentelemetry::global;
use opentelemetry_nats::NatsHeaderInjector;
use std::collections::HashMap;
use tracing::span;
use opentelemetry::trace::TraceContextExt;

async fn enqueue_job(jetstream_context: &jetstream::Context, job_data: &[u8], priority: &str) -> Result<(), Box<dyn std::error::Error>> {
    let subject = format!("jobs.{}", priority);

    // 1. Inject OpenTelemetry context into headers
    let mut headers = HashMap::new();
    let cx = span::Span::current().context();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_with(&mut NatsHeaderInjector::new(&mut headers), &cx);
    });

    // 2. Publish the job with headers
    jetstream_context
        .publish_with_headers(subject, headers, job_data.into())
        .await?
        .await?;

    Ok(())
}
```

### 4.2. Consumer: Apalis Worker with NATS Storage (Simplified)

This is a conceptual outline of how the `apalis` worker and NATS storage might be structured.

```rust
use apalis::prelude::*;
use async_nats::{jetstream, Message};
use opentelemetry::global;
use opentelemetry_nats::NatsHeaderExtractor;
use tracing_opentelemetry::OpenTelemetrySpanExt;

// --- Custom NATS Storage for Apalis ---
#[derive(Clone)]
pub struct NatsStorage {
    // ... NATS client and stream details
}

// ... Implementation of `Storage` trait for NatsStorage ...

// --- The Job Runner ---
async fn process_email_job(job: EmailJob, _ctx: JobContext) {
    // Job logic goes here...
    println!("Processing email to: {}", job.to);
}

// --- Worker Setup ---
#[tokio::main]
async fn main() {
    // OTel setup...

    let storage = NatsStorage::new(/* ... */).await;

    Monitor::new()
        .register(
            WorkerBuilder::new("email-worker")
                .backend(storage)
                .build_fn(process_email_job)
        )
        .run()
        .await
        .unwrap();
}

// --- Inside the NATS message consumption logic ---
async fn on_nats_message(msg: Message) {
    // 1. Extract OTel context and create a span
    let parent_cx = global::get_text_map_propagator(|propagator| {
        propagator.extract(&NatsHeaderExtractor::new(&msg.headers))
    });
    let span = tracing::info_span!("process_job");
    span.set_parent(parent_cx);

    // 2. Enter the span for the duration of the job
    let _enter = span.enter();

    // ... deserialize and execute the job using apalis ...
}
```

## 5. Conclusion

This design provides a comprehensive blueprint for a high-performance, reliable, and observable job queue system using NATS and Rust. By leveraging separate streams for priorities, utilizing NATS JetStream's built-in reliability features, and integrating with `apalis` and OpenTelemetry, this architecture meets all the specified requirements. The separation of concerns between job streams, the DLQ, and priority handling ensures a scalable and maintainable system.│[2025-09-08][19:53:06][app_lib::email::commands][INFO] email_seed_mailbox: completed, inserted=200             │

## 6. Practical Examples

### 6.1 Priority Queues Example

The snippet below enqueues Low, then Medium, then High priority messages and shows that a single-concurrency worker processes High first, then Medium, then Low.

```rust
use apalis::prelude::*;
use apalis_nats::{Config, NatsStorage, Priority};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Job { name: String }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = apalis_nats::connect("nats://localhost:4222").await?;
    let storage = NatsStorage::<Job>::new_with_config(
        client,
        Config { namespace: "nats_priority_demo".into(), ..Default::default() }
    ).await?;

    // Enqueue in reverse order to show priority wins
    storage.push_with_priority(Job { name: "low-1".into() }, Priority::Low).await?;
    storage.push_with_priority(Job { name: "low-2".into() }, Priority::Low).await?;
    storage.push_with_priority(Job { name: "medium-1".into() }, Priority::Medium).await?;
    storage.push_with_priority(Job { name: "medium-2".into() }, Priority::Medium).await?;
    storage.push_with_priority(Job { name: "high-1".into() }, Priority::High).await?;
    storage.push_with_priority(Job { name: "high-2".into() }, Priority::High).await?;

    async fn handle(job: Job) -> Result<(), Error> {
        println!("processing {}", job.name);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    }

    let worker = WorkerBuilder::new("priority-worker")
        .concurrency(1)
        .backend(storage.clone())
        .build_fn(handle);

    Monitor::new().register(worker).run().await?;
    Ok(())
}
```

See also: `examples/nats-priority` for a runnable version.

### 6.2 Catch-Panic Example

Enable the `catch-panic` feature on `apalis` to convert panics into `Error::Abort` so the backend can Term/DLQ deterministically rather than redeliver.

```toml
# Cargo.toml
[dependencies]
apalis = { version = "0.7", features = ["catch-panic"] }
apalis-nats = "0.7"
```

```rust
use apalis::prelude::*;
use apalis_nats::{Config, NatsStorage};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Job { name: String, panic: bool }

async fn handler(job: Job) -> Result<(), Error> {
    if job.panic { panic!("boom"); }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = apalis_nats::connect("nats://localhost:4222").await?;
    let storage = NatsStorage::new_with_config(
        client,
        Config { enable_dlq: true, ..Default::default() }
    ).await?;

    storage.push(Job { name: "ok".into(), panic: false }).await?;
    storage.push(Job { name: "will-panic".into(), panic: true }).await?;

    let worker = WorkerBuilder::new("catch-panic-worker")
        .catch_panic()
        .backend(storage.clone())
        .build_fn(handler);

    Monitor::new().register(worker).run().await?;
    Ok(())
}
```

See also: `examples/nats-catch-panic` for a runnable version that inspects the DLQ after execution.
