//! Versioned contracts for messages that cross process boundaries.
//!
//! Local typed events are not messages. Applications should map an event to a
//! [`MessageEnvelope`] explicitly when work must cross a process boundary.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::future::{Future, ready};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{
    Arc, Mutex, Weak,
    mpsc::{Receiver, SyncSender, TryRecvError, TrySendError, sync_channel},
};
use std::task::{Context, Poll, Waker};
use std::time::{SystemTime, UNIX_EPOCH};
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

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

// ponytail: time + process counter, unique within one process and practically
// across them; swap for UUIDv7 when a uuid dependency is acceptable.
fn generate_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!("{nanos:x}-{:x}", NEXT_ID.fetch_add(1, Ordering::Relaxed))
}

impl MessageEnvelope {
    /// Creates a root message with generated `id` and `correlation_id`.
    pub fn new(name: impl Into<String>, schema_version: u32, payload: Value) -> Self {
        let id = generate_id();
        Self {
            correlation_id: id.clone(),
            id,
            name: name.into(),
            schema_version,
            causation_id: None,
            deadline_unix_ms: None,
            payload,
        }
    }

    /// Creates a message caused by `parent`, sharing its correlation and deadline.
    pub fn caused_by(
        parent: &MessageEnvelope,
        name: impl Into<String>,
        schema_version: u32,
        payload: Value,
    ) -> Self {
        Self {
            id: generate_id(),
            name: name.into(),
            schema_version,
            correlation_id: parent.correlation_id.clone(),
            causation_id: Some(parent.id.clone()),
            deadline_unix_ms: parent.deadline_unix_ms,
            payload,
        }
    }
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

#[derive(Default)]
struct SubscriptionState {
    queue: VecDeque<MessageEnvelope>,
    waker: Option<Waker>,
}

type Topics = HashMap<String, Vec<Weak<Mutex<SubscriptionState>>>>;

/// Topic-addressed fan-out over bounded per-subscriber queues.
///
/// Every live [`Subscription`] to a topic receives its own copy of each
/// message published to it. Unlike [`InMemoryMessageBus`], consumers do not
/// compete for messages.
#[derive(Clone)]
pub struct TopicBus {
    capacity: usize,
    topics: Arc<Mutex<Topics>>,
}

impl TopicBus {
    /// Creates a bus whose subscriptions each buffer up to `capacity` messages.
    pub fn new(capacity: usize) -> Result<Self, BusError> {
        if capacity == 0 {
            return Err(BusError::InvalidCapacity);
        }
        Ok(Self {
            capacity,
            topics: Arc::default(),
        })
    }

    /// Subscribes to `topic`; messages published from now on are delivered.
    /// Dropping the subscription unsubscribes it.
    pub fn subscribe(&self, topic: &str) -> Result<Subscription, BusError> {
        let state = Arc::<Mutex<SubscriptionState>>::default();
        self.topics
            .lock()
            .map_err(|_| BusError::Poisoned)?
            .entry(topic.to_owned())
            .or_default()
            .push(Arc::downgrade(&state));
        Ok(Subscription { state })
    }

    /// Delivers `message` to every live subscriber of `topic`.
    ///
    /// All-or-nothing: if any subscriber's queue is full nothing is delivered
    /// and the envelope is returned in [`PublishError::QueueFull`]. Publishing
    /// to a topic without subscribers succeeds and drops the message.
    pub fn publish_to(
        &self,
        topic: &str,
        message: MessageEnvelope,
    ) -> BusFuture<'_, Result<(), PublishError>> {
        Box::pin(ready(self.deliver(topic, message)))
    }

    // Matches `PublishError`, which returns the envelope by value.
    #[allow(clippy::result_large_err)]
    fn deliver(&self, topic: &str, message: MessageEnvelope) -> Result<(), PublishError> {
        // The topic-map lock is held throughout so concurrent publishers
        // cannot both pass the capacity check for the same queue.
        let Ok(mut topics) = self.topics.lock() else {
            return Err(PublishError::Closed(message));
        };
        let Some(subscribers) = topics.get_mut(topic) else {
            return Ok(());
        };
        subscribers.retain(|weak| weak.strong_count() > 0);
        let live: Vec<_> = subscribers.iter().filter_map(Weak::upgrade).collect();
        let mut guards = Vec::with_capacity(live.len());
        for state in &live {
            let Ok(guard) = state.lock() else {
                return Err(PublishError::Closed(message));
            };
            if guard.queue.len() >= self.capacity {
                return Err(PublishError::QueueFull(message));
            }
            guards.push(guard);
        }
        for guard in &mut guards {
            guard.queue.push_back(message.clone());
            if let Some(waker) = guard.waker.take() {
                waker.wake();
            }
        }
        Ok(())
    }
}

/// One subscriber's view of a topic; see [`TopicBus::subscribe`].
pub struct Subscription {
    state: Arc<Mutex<SubscriptionState>>,
}

impl Subscription {
    /// Waits for the next message on the topic.
    pub fn recv(&self) -> impl Future<Output = Result<MessageEnvelope, BusError>> + '_ {
        std::future::poll_fn(|cx: &mut Context<'_>| {
            let Ok(mut state) = self.state.lock() else {
                return Poll::Ready(Err(BusError::Poisoned));
            };
            match state.queue.pop_front() {
                Some(message) => Poll::Ready(Ok(message)),
                None => {
                    state.waker = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Arc;
    use std::task::Wake;
    use std::thread::{self, Thread};
    use std::time::Duration;

    struct Unpark(Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        let mut future = Box::pin(future);
        let waker = Waker::from(Arc::new(Unpark(thread::current())));
        let mut cx = Context::from_waker(&waker);
        loop {
            if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
                return value;
            }
            thread::park();
        }
    }

    fn message(n: u64) -> MessageEnvelope {
        MessageEnvelope::new("t.event", 1, json!(n))
    }

    #[test]
    fn constructors_generate_unique_ids_and_lineage() {
        let a = message(1);
        let b = message(2);
        assert_ne!(a.id, b.id);
        assert_eq!(a.correlation_id, a.id);
        let mut root = a.clone();
        root.deadline_unix_ms = Some(9);
        let child = MessageEnvelope::caused_by(&root, "t.child", 1, json!(null));
        assert_eq!(child.correlation_id, root.correlation_id);
        assert_eq!(child.causation_id.as_deref(), Some(root.id.as_str()));
        assert_eq!(child.deadline_unix_ms, Some(9));
        assert_ne!(child.id, root.id);
    }

    #[test]
    fn fan_out_reaches_every_subscriber_of_the_topic_only() {
        let bus = TopicBus::new(4).unwrap();
        let (a, b, other) = (
            bus.subscribe("x").unwrap(),
            bus.subscribe("x").unwrap(),
            bus.subscribe("y").unwrap(),
        );
        let sent = message(1);
        block_on(bus.publish_to("x", sent.clone())).unwrap();
        assert_eq!(block_on(a.recv()).unwrap(), sent);
        assert_eq!(block_on(b.recv()).unwrap(), sent);
        assert!(other.state.lock().unwrap().queue.is_empty());
    }

    #[test]
    fn recv_awaits_a_later_publish() {
        let bus = TopicBus::new(1).unwrap();
        let sub = bus.subscribe("x").unwrap();
        let publisher = bus.clone();
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            block_on(publisher.publish_to("x", message(7))).unwrap();
        });
        assert_eq!(block_on(sub.recv()).unwrap().payload, json!(7));
        handle.join().unwrap();
    }

    #[test]
    fn full_subscriber_rejects_publish_for_all_and_drop_unsubscribes() {
        let bus = TopicBus::new(1).unwrap();
        let slow = bus.subscribe("x").unwrap();
        let fast = bus.subscribe("x").unwrap();
        block_on(bus.publish_to("x", message(1))).unwrap();
        block_on(fast.recv()).unwrap();
        assert!(matches!(
            block_on(bus.publish_to("x", message(2))),
            Err(PublishError::QueueFull(_))
        ));
        assert!(fast.state.lock().unwrap().queue.is_empty());
        drop(slow);
        block_on(bus.publish_to("x", message(3))).unwrap();
        assert_eq!(block_on(fast.recv()).unwrap().payload, json!(3));
    }
}
