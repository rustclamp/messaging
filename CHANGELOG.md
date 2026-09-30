# Changelog

## Unreleased

- Add `TopicBus`: topic-addressed fan-out with per-subscriber bounded queues and an awaitable `Subscription::recv` (#2).
- Add `MessageEnvelope::new` / `caused_by`, which generate ids and carry correlation lineage (#2).
- Carry an optional execution deadline in the message envelope.
- Add a transport-neutral, serializable message envelope contract.
- Add a bounded in-memory `MessageBus` implementation for tests and development.
