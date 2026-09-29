//! Versioned contracts for messages that cross process boundaries.
//!
//! Local typed events are not messages. Applications should map an event to a
//! [`MessageEnvelope`] explicitly when work must cross a process boundary.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::future::{Future, ready};
use std::pin::Pin;
use std::sync::{
    Arc, Mutex,
    mpsc::{Receiver, SyncSender, TryRecvError, TrySendError, sync_channel},
};
use std::{error::Error, fmt};

/// A serialized message with stable identity and execution lineage.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MessageEnvelope {
    /// Unique identity for this delivery-independent message.
    pub id: String,
    /// Stable semantic message name, such as `orders.order-created`.
    pub name: String,
    /// Schema version for the payload associated with `name`.
    pub schema_version: u32,
    /// Identity shared by messages produced during one request or workflow.
    pub correlation_id: String,
    /// Identity of the message that directly caused this message, if any.
    pub causation_id: Option<String>,
    /// Optional Unix timestamp in milliseconds after which work must not start.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_unix_ms: Option<u64>,
    /// Versioned application data; transport metadata belongs to the transport.
    pub payload: Value,
}

/// A boxed asynchronous result returned by a message bus.
pub type BusFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Capability for publishing and receiving cross-process message envelopes.
pub trait MessageBus: Send + Sync {
    /// Publishes one envelope or reports bounded-queue backpressure.
    fn publish(&self, message: MessageEnvelope) -> BusFuture<'_, Result<(), PublishError>>;

    /// Receives one immediately available message; returns `None` when empty.
    fn receive(&self) -> BusFuture<'_, Result<Option<MessageEnvelope>, BusError>>;
}

/// Typed message-bus capability marker for Kernel composition.
pub struct MessageBusCapability;

impl rustclamp_core::Capability for MessageBusCapability {
    type Value = dyn MessageBus;

    const ID: rustclamp_core::CapabilityId = rustclamp_core::CapabilityId::new("messaging.bus");
}

/// Failure while publishing or receiving from a message bus.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BusError {
    /// All queue receivers have been dropped.
    Closed,
    /// The shared in-memory receiver lock was poisoned.
    Poisoned,
    /// The requested in-memory queue capacity was zero.
    InvalidCapacity,
}

impl fmt::Display for BusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("message queue is closed"),
            Self::Poisoned => f.write_str("message queue lock is poisoned"),
            Self::InvalidCapacity => f.write_str("message queue capacity must be positive"),
        }
    }
}

impl Error for BusError {}

/// A failed publish that retains the message for retry or inspection.
#[derive(Debug)]
pub enum PublishError {
    /// The bounded queue is full; the envelope is returned to the caller.
    QueueFull(MessageEnvelope),
    /// The queue has closed; the envelope is returned to the caller.
    Closed(MessageEnvelope),
}

impl fmt::Display for PublishError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueFull(_) => f.write_str("message queue is full"),
            Self::Closed(_) => f.write_str("message queue is closed"),
        }
    }
}

impl Error for PublishError {}

/// Bounded process-local transport for tests and development compositions.
#[derive(Clone)]
pub struct InMemoryMessageBus {
    sender: SyncSender<MessageEnvelope>,
    receiver: Arc<Mutex<Receiver<MessageEnvelope>>>,
}

impl InMemoryMessageBus {
    /// Creates a queue with a fixed positive message capacity.
    pub fn new(capacity: usize) -> Result<Self, BusError> {
        if capacity == 0 {
            return Err(BusError::InvalidCapacity);
        }
        let (sender, receiver) = sync_channel(capacity);
        Ok(Self {
            sender,
            receiver: Arc::new(Mutex::new(receiver)),
        })
    }
}

impl MessageBus for InMemoryMessageBus {
    fn publish(&self, message: MessageEnvelope) -> BusFuture<'_, Result<(), PublishError>> {
        let result = self.sender.try_send(message).map_err(|error| match error {
            TrySendError::Full(message) => PublishError::QueueFull(message),
            TrySendError::Disconnected(message) => PublishError::Closed(message),
        });
        Box::pin(ready(result))
    }

    fn receive(&self) -> BusFuture<'_, Result<Option<MessageEnvelope>, BusError>> {
        let result = self
            .receiver
            .lock()
            .map_err(|_| BusError::Poisoned)
            .and_then(|receiver| match receiver.try_recv() {
                Ok(message) => Ok(Some(message)),
                Err(TryRecvError::Empty) => Ok(None),
                Err(TryRecvError::Disconnected) => Err(BusError::Closed),
            });
        Box::pin(ready(result))
    }
}
