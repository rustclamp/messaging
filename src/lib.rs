//! Versioned contracts for messages that cross process boundaries.
//!
//! Local typed events are not messages. Applications should map an event to a
//! [`MessageEnvelope`] explicitly when work must cross a process boundary.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use serde::{Deserialize, Serialize};
use serde_json::Value;

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
    /// Versioned application data; transport metadata belongs to the transport.
    pub payload: Value,
}
