//! The room model of noise.
//!
//! A room is a label and a random secret. Its members receive the secret
//! when they join, and the room id, the gossip topic and the keys that
//! seal the room's messages derive from it. A message is signed by its
//! author and then sealed for the room.
//!
//! This crate does no I/O and reads no clock: callers pass the time in.
//! A received message reaches a caller only as a [`Verified`], which
//! [`Room::open`] returns after it decrypted the message and checked its
//! signature.

mod ids;
mod message;
mod room;

pub use ids::{MessageId, ParseHexError, RoomId, RoomSecret, Timestamp, TopicId};
pub use iroh_base::{PublicKey, SecretKey, Signature};
pub use message::{Body, Invalid, MAX_SIGNED_SIZE, Member, Message, SignedMessage, Verified};
pub use room::{MAX_SEALED_SIZE, Room};
