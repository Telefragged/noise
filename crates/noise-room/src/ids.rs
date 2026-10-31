use std::fmt;
use std::str::FromStr;

use data_encoding::HEXLOWER;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Implements hex `Display`, `FromStr` and serde for a 32-byte newtype.
macro_rules! hex_bytes {
    ($name:ident) => {
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&HEXLOWER.encode(&self.0))
            }
        }

        impl FromStr for $name {
            type Err = ParseHexError;

            fn from_str(s: &str) -> Result<Self, ParseHexError> {
                let bytes = HEXLOWER.decode(s.as_bytes()).map_err(|_| ParseHexError)?;
                Ok(Self(bytes.try_into().map_err(|_| ParseHexError)?))
            }
        }

        /// Hex in a human-readable format such as a message's JSON, the
        /// 32 bytes in a binary one such as a join frame.
        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                if serializer.is_human_readable() {
                    serializer.serialize_str(&self.to_string())
                } else {
                    self.0.serialize(serializer)
                }
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                if deserializer.is_human_readable() {
                    let s = String::deserialize(deserializer)?;
                    s.parse().map_err(serde::de::Error::custom)
                } else {
                    Ok(Self(<[u8; 32]>::deserialize(deserializer)?))
                }
            }
        }
    };
}

/// A string is not 64 lowercase hex characters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("expected 64 lowercase hex characters")]
pub struct ParseHexError;

/// The BLAKE3 hash of a message's bytes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageId(pub(crate) [u8; 32]);
hex_bytes!(MessageId);

impl fmt::Debug for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MessageId({self})")
    }
}

/// A room's id, which derives from the room secret and does not reveal it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RoomId(pub(crate) [u8; 32]);
hex_bytes!(RoomId);

impl fmt::Debug for RoomId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RoomId({self})")
    }
}

/// A room's random secret. Members receive it when they join, and the
/// room id, the gossip topic and the keys that seal the room's messages
/// derive from it.
#[derive(Clone, PartialEq, Eq)]
pub struct RoomSecret(pub(crate) [u8; 32]);
hex_bytes!(RoomSecret);

impl RoomSecret {
    #[must_use]
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub fn id(&self) -> RoomId {
        RoomId(self.derive("noise room id v1"))
    }

    /// The id of the room's gossip topic.
    #[must_use]
    pub fn topic(&self) -> TopicId {
        TopicId(self.derive("noise gossip topic v1"))
    }

    /// A BLAKE3 key derivation from the secret, for `context`.
    pub(crate) fn derive(&self, context: &str) -> [u8; 32] {
        blake3::derive_key(context, &self.0)
    }
}

impl fmt::Debug for RoomSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RoomSecret(..)")
    }
}

/// The id of a room's gossip topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TopicId(pub(crate) [u8; 32]);

impl TopicId {
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Milliseconds since the Unix epoch, as the author's clock read them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(pub u64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_round_trip_through_hex_json_and_postcard() {
        let id = MessageId([7; 32]);
        assert_eq!(id.to_string().parse::<MessageId>().expect("parse"), id);
        let json = serde_json::to_string(&id).expect("serialize");
        assert_eq!(serde_json::from_str::<MessageId>(&json).expect("json"), id);
        let encoded = postcard::to_stdvec(&id).expect("encode");
        assert_eq!(encoded.len(), 32);
        assert_eq!(
            postcard::from_bytes::<MessageId>(&encoded).expect("decode"),
            id
        );
        assert!("07".parse::<MessageId>().is_err());
    }

    #[test]
    fn the_derived_values_depend_on_the_secret_and_differ() {
        let (one, two) = (RoomSecret([1; 32]), RoomSecret([2; 32]));
        assert_eq!(one.topic(), RoomSecret([1; 32]).topic());
        assert_ne!(one.topic(), two.topic());
        assert_ne!(one.id(), two.id());
        assert_ne!(one.id().0, one.topic().0);
    }
}
