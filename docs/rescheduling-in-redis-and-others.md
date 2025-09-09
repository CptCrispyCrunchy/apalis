Title: Rescheduling in Redis and other backends (when, why, how different from retries)

This note explains how “rescheduling” works in other Apalis storage implementations (e.g., Redis), when it happens, why you would use it, and how it differs from normal retries.

What is rescheduling?
- Rescheduling explicitly asks the backend to run a job again in the future at a specific time or after a delay. It is a deliberate operation, not an error path.
- In APIs, this appears as `schedule_request` (run at a timestamp) and `reschedule` (run after a wait/delay).

How is it different from retries?
- Retries are automatic redeliveries initiated by the backend after a failure. They are bounded by policies like `max_deliver`, backoff, and error type (e.g., transient vs. abort).
- Rescheduling is an app-driven time shift regardless of success/failure of the current run. Typical reasons:
  - Rate-limiting/bursty load: push out non-urgent work.
  - External dependency windowing: re-run after a partner window opens.
  - Business rule: not-before or exact-at execution semantics.
- Operationally:
  - Retry timing is managed by the backend’s retry/backoff policy.
  - Reschedule timing is explicitly set by the caller (exact timestamp or relative delay).

Redis backend behavior (typical)
- Storage: Redis maintains two structures:
  - A “ready” queue (e.g., a list) for immediate jobs.
  - A “scheduled/delayed” sorted set keyed by wake-up time (score: epoch millis/seconds).
- schedule_request (run at time T):
  - Enqueues the job into the delayed ZSET with score = T.
  - A background poller (or Lua script) periodically scans the head of the ZSET and moves due jobs to the ready queue atomically.
- reschedule (delay D):
  - Rewrites the job’s due time in the delayed ZSET to now + D.
- When does it happen?
  - Either upon explicit API call (user code decides), or from handlers that want to defer work without failing the job.
- Why not use retries?
  - Retries are tied to failures and backoff policy (e.g., exponential). If a handler succeeds but wants the job to run later, reschedule is semantically correct. It avoids marking the current attempt as a failure and avoids unbounded backoff interaction.
- How different from acks/nacks?
  - Acks/nacks are delivery outcomes for a single attempt (success, retry soon, or terminate). Rescheduling sets the next eligible time independently of the current attempt’s outcome.

Other backends (high level)
- SQL (e.g., Postgres):
  - Uses a table column for “run_at”/“available_at” with an index; rescheduling updates that timestamp. A poller queries for rows with `run_at <= now()` and claims them.
- Cron backend:
  - “Rescheduling” is built-in via cron expressions. Jobs are produced at schedule times; rescheduling means changing the cron spec.
- NATS JetStream (current crate):
  - Pull consumers don’t have native per-message delay. Implementing durable reschedule requires auxiliary mechanisms (e.g., delayed streams or KV with expirations). Hence, this crate currently documents scheduling/rescheduling as unsupported.

Operational guidance
- Use rescheduling when you need precise control over when a job becomes eligible again and want to keep failure metrics clean.
- Use retries when transient failures occur and you want the backend to re-deliver automatically based on retry policy.
- In Redis/SQL, prefer rescheduling over nack-based delays for predictable timing and to avoid burning retry counters.

