# apalis-nats Crate Review

This document summarizes the current state of the `packages/apalis-nats` crate, highlights issues, and proposes actionable fixes.

## High‑Priority Issues
- Ack loop starvation: `poll` uses a biased `select!` with an always‑ready branch (`_ = async {}`), so acks can be starved. Move acks to a dedicated task.
- Potential panic on DLQ publish: `serde_json::to_vec(&dlq_job).unwrap()` can panic in `Ack::ack`. Map the error instead of unwrapping.
- Async runtime mismatch: Feature `async-std-comp` is defined but the code unconditionally uses `tokio::spawn` and `tokio::time::sleep`. Either gate with features or drop `async-std-comp`.

## Design Gaps (OK to Document or Implement)
- Scheduling/rescheduling: `Storage::schedule_request` and `reschedule` return "not yet implemented". If out of scope, document clearly in README; otherwise implement republish-based scheduling.
- DLQ payload shape: DLQ embeds raw bytes as `payload`. Document that it is the original message bytes (not decoded JSON).

## Polish & Consistency
- Unused config field: `Config::flow_control` is never applied to consumer settings; wire it up (if supported) or remove.
- README path: `Cargo.toml` uses `readme = "../../README.md"`, which won’t package on crates.io. Prefer the crate-local `README.md`.
- Minor clippy nits: Likely unused imports (`SinkExt`), etc. Run `cargo clippy --all-features` and address warnings.

## Suggested Fixes
- Dedicated ack loop (in `poll`):
  ```rust
  // after creating ack_rx
  let mut ack_storage = self.clone();
  tokio::spawn(async move {
      while let Some((ctx, resp)) = ack_rx.next().await {
          if let Err(e) = <NatsStorage<T> as Ack<T, Vec<u8>, JsonCodec<Vec<u8>>>>::ack(&mut ack_storage, &ctx, &resp).await {
              log::error!("ack failed: {}", e);
          }
      }
  });
  // remove ack handling from select! and drop the always-ready `_ = async {}` branch
  ```
- Remove `unwrap()` when publishing DLQ:
  ```rust
  let body = serde_json::to_vec(&dlq_job)?;
  self.jetstream.publish(dlq_subject, body.into()).await?; // then .await on the pub ack
  ```
- Feature-gate runtime usage:
  ```rust
  #[cfg(feature = "tokio-comp")] tokio::spawn(task);
  #[cfg(feature = "async-std-comp")] async_std::task::spawn(task);

  #[cfg(feature = "tokio-comp")] tokio::time::sleep(d).await;
  #[cfg(feature = "async-std-comp")] async_std::task::sleep(d).await;
  ```
- Update crate metadata and cleanups:
  - `Cargo.toml`: set `readme = "README.md"` for the crate.
  - Remove `flow_control` or implement if applicable for pull consumers.

## Validation
- Run: `cargo clippy --all-features -p apalis-nats` and `cargo test -p apalis-nats` (tests use `testcontainers` to spin up NATS with `-js`).
- Manually verify DLQ behavior by forcing `Error::Abort` and exceeding `max_deliver`.

## Status
Core functionality (push, priority ordering, ack/nack/term, DLQ, optional OTEL) appears sound with the fixes above. Addressing the ack loop and panic risk is recommended before release.
