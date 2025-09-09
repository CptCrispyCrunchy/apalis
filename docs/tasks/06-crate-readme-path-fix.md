Title: Point crate readme to local README.md

Summary
- Set `readme = "README.md"` in `packages/apalis-nats/Cargo.toml` so crates.io includes the correct file from the crate directory.

Context
- File: `packages/apalis-nats/Cargo.toml`
- Current value: `readme = "../../README.md"` which is not packaged with the crate and will be rejected or ignored by crates.io.

Detailed Steps
1) Edit `packages/apalis-nats/Cargo.toml`:
   - Change the `readme` field to `"README.md"`.
2) Verify that `packages/apalis-nats/README.md` is present (it is).
3) Optionally run `cargo package -p apalis-nats` locally to validate packaging (skipped in CI by default).

Acceptance Criteria
- `cargo metadata -p apalis-nats` reflects `readme = "README.md"`.
- `cargo package -p apalis-nats` succeeds locally (optional) and includes the crate-local README.

