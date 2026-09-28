# rustclamp-messaging

Transport-neutral contracts for messages that cross process boundaries.
Local typed events remain in-process values; applications map an event to a
versioned message explicitly when it must be delivered to another process.

The first contract is `MessageEnvelope`, which carries a stable message type
and schema version, message identity, correlation and causation identities,
and a JSON payload. Broker delivery, retries, and worker execution belong to
their respective integrations.
