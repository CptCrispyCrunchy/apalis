Title: Lint, build, and test validation plan

Summary
- Run formatting, linting, and tests for `apalis-nats`, including DLQ and priority behavior, after implementing the other tasks.

Commands
- Format: `cargo fmt`
- Clippy (crate): `cargo clippy -p apalis-nats --all-features -- -D warnings`
- Clippy (workspace, optional): `cargo clippy --all-features`
- Tests (crate): `cargo test -p apalis-nats -- --nocapture`
- Example (nats): `cargo build --example nats`

Notes
- Integration tests use `testcontainers` to spin up a NATS server (`-js`). No external env vars required.
- If running locally without Docker, ensure Docker is available for `testcontainers` to function.
 

Acceptance Criteria
- `cargo fmt --check` yields no diffs.
- `cargo clippy -p apalis-nats --all-features` has no warnings (or only accepted allow-listed ones).
- `cargo test -p apalis-nats` passes all tests.
- Example builds with and without `apalis-nats/otel`.
