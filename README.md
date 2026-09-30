<img src="https://docs.rustclamp.com/assets/rustclamp-logo.png" alt="RustClamp logo" width="160">

# rustclamp-messaging

Messaging component of [RustClamp](https://github.com/rustclamp/rustclamp):
transport-neutral contracts for messages that cross process boundaries. Local
typed events stay in-process values; an application maps an event to a
versioned message explicitly when it must leave the process. Broker delivery,
retries and worker execution belong to their integrations. Companion crate, not
a standalone framework.

## Install

Not published to crates.io yet (`publish = false`). Depend on it from git, Rust 1.96.1+:

```toml
[dependencies]
rustclamp-messaging = { git = "https://github.com/rustclamp/messaging" }
```

## Example

```rust
use rustclamp_messaging::{MessageEnvelope, TopicBus};
use serde_json::json;

let bus = TopicBus::new(16)?;
let orders = bus.subscribe("orders")?;
bus.publish_to("orders", MessageEnvelope::new("order.created", 1, json!({ "id": 7 })))
    .await?;
let message = orders.recv().await?; // awaitable
```

## Main API

- `MessageEnvelope`: message type, schema version, message/correlation/causation
  ids, optional Unix-millisecond deadline, JSON payload. `new` generates ids;
  `caused_by` carries correlation lineage.
- `MessageBus` trait (`publish`, `receive`) with `MessageBusCapability` for Kernel
  composition; `InMemoryMessageBus` is a bounded process-local queue for tests
  and development. A full queue returns the envelope to the publisher
  (`PublishError`).
- `TopicBus`: `subscribe(topic)` returns a `Subscription` (awaitable `recv`);
  `publish_to(topic, message)` fans out to every live subscriber. A full
  subscriber queue rejects the publish for all of them.

Depends on `rustclamp-core`, `serde`, `serde_json`. Feature flags: none. See
[CHANGELOG.md](CHANGELOG.md).

## Documentation

<https://docs.rustclamp.com>

## Development

```sh
cargo fmt --all -- --check
cargo clippy --offline --locked --all-targets --all-features -- -D warnings
cargo test --offline --locked --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --offline --locked --no-deps --all-features
```

Coordinated checkout, architecture checks and release policy: see the
[facade contributor guide](https://github.com/rustclamp/rustclamp/blob/main/CONTRIBUTING.md).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Unless you state otherwise, any
contribution you submit for inclusion is dual licensed as above, without
additional terms or conditions.
