# rustclamp-messaging

Transport-neutral contracts for messages that cross process boundaries.
Local typed events remain in-process values; applications map an event to a
versioned message explicitly when it must be delivered to another process.

The first contract is `MessageEnvelope`, which carries a stable message type
and schema version, message identity, correlation and causation identities,
an optional Unix-millisecond execution deadline, and a JSON payload. Broker
delivery, retries, and worker execution belong to their respective integrations.

The `MessageBus` capability is qualified through Core. `InMemoryMessageBus`
provides a bounded process-local queue for tests and development; a full queue
returns the original envelope so the publisher can retry or apply policy.

`TopicBus` adds topics: `subscribe(topic)` returns a `Subscription` with an
awaitable `recv`, and `publish_to(topic, message)` fans out a copy to every
live subscriber. A full subscriber queue rejects the publish for all of them.
`MessageEnvelope::new` and `caused_by` generate ids and carry lineage.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Unless you state otherwise, any
contribution you submit for inclusion is dual licensed as above, without
additional terms or conditions.
