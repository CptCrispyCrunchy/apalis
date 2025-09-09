Title: Scheduling and rescheduling plan for JetStream backend

Summary
- Clarify current limitation: `schedule_request` and `reschedule` are not implemented for the JetStream pull-consumer backend.
- Provide two paths: document as unsupported for now, or implement a minimal republish-after-delay approach with clear caveats.

Context
- File: `packages/apalis-nats/src/storage.rs` methods `schedule_request` and `reschedule` return errors.
- JetStream itself does not have per-message delayed delivery on pull consumers without introducing additional mechanisms (e.g., a delayed subject pattern, KV/Stream timers, or server-side advisories).

Option A: Document as unsupported
1) Update `packages/apalis-nats/README.md` with a “Scheduling” section:
   - State that delayed/scheduled jobs are not supported in the current version when using pull consumers.
   - Suggest alternatives: a separate scheduler service, or using NATS KV with expirations to trigger republish.
2) Keep `schedule_request` and `reschedule` returning a clear error message: "Scheduling not yet implemented".

Option B: Minimal delay via republish (best-effort)
1) Implement `reschedule(job, wait)` to:
   - Extract the original job payload and metadata.
   - Spawn a background task that `sleep(wait)` and then republishes the job to the appropriate subject.
   - Ack or term the original message depending on semantics (likely ack after scheduling to avoid redelivery).
   - Caveat: delayed job is not persisted until republish happens; if the worker dies during sleep, the job is lost.
2) Implement `schedule_request(request, on_timestamp)` similarly by computing `delay = on_timestamp - now` and following the above pattern.
3) Document the caveats and recommend production users consider a durable scheduler approach.

Code Sketch (illustrative)
```rust
// in reschedule
let payload = serde_json::to_vec(&NatsJob { /* reconstruct */ })?;
let subject = self.get_subject(priority);
#[cfg(feature = "tokio-comp")]
tokio::spawn({
    let js = self.jetstream.clone();
    async move {
        tokio::time::sleep(wait).await;
        let _ = js.publish(subject, Bytes::from(payload)).await.and_then(|pa| pa.await);
    }
});
// ack the original message to prevent immediate redelivery
```

Recommendation
- Prefer Option A initially (document unsupported) to avoid surprising semantics. Revisit a robust scheduler later using a dedicated delayed subject or NATS KV/JetStream timers.

Acceptance Criteria
- If documenting: README contains a clear Scheduling section with limitations and guidance.
- If implementing: functions compile, tests cover basic reschedule behavior, and documentation includes caveats.

