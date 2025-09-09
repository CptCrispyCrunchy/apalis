Title: Fix examples/nats Config construction

Summary
- Ensure `examples/nats/src/main.rs` builds by completing the `Config` struct literal and guarding otel-specific fields behind the feature.

Context
- File: `examples/nats/src/main.rs`
- Current code constructs `Config` with only: `namespace`, `max_deliver`, `ack_wait`, `num_replicas`, `enable_dlq`. The `Config` in the crate also includes `max_ack_pending` and conditionally `enable_tracing`.

Detailed Steps
1) Update the `Config` construction to use `..Default::default()` to populate unspecified fields:
   ```rust
   let mut config = Config {
       namespace: "apalis_example".to_string(),
       max_deliver: 3,
       ack_wait: Duration::from_secs(30),
       num_replicas: 1,
       enable_dlq: true,
       ..Default::default()
   };
   ```

2) Under `#[cfg(feature = "otel")]`, set `config.enable_tracing = true;` (as already present) and ensure it compiles only when the feature is enabled.

3) Build the example:
   - `cargo build --example nats`
   - With otel: `cargo build --example nats --features apalis-nats/otel`

Acceptance Criteria
- Example compiles under default features and with `--features apalis-nats/otel`.
- No missing field errors for `Config` in the example.
