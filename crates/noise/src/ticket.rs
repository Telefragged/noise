//! The join ticket: what a session needs to join a room.

use anyhow::{Context, Result, anyhow};
use data_encoding::BASE64URL_NOPAD;
use iroh::EndpointAddr;
use noise_net::Token;
use serde::{Deserialize, Serialize};

/// The start of every ticket's text.
const PREFIX: &str = "noise";

/// The member that issued the ticket, with its addresses, and the
/// ticket's single-use token. The joining session connects to the member,
/// which iroh authenticates by its key, and redeems the token there.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ticket {
    pub inviter: EndpointAddr,
    pub token: Token,
}

impl Ticket {
    /// The ticket as text: [`PREFIX`] and its postcard encoding in
    /// URL-safe base64.
    ///
    /// # Errors
    ///
    /// When the ticket cannot be encoded.
    pub fn encode(&self) -> Result<String> {
        let bytes = postcard::to_stdvec(self)?;
        Ok(format!("{PREFIX}{}", BASE64URL_NOPAD.encode(&bytes)))
    }

    /// The ticket in `text`, which [`Ticket::encode`] wrote.
    ///
    /// # Errors
    ///
    /// When `text` is not a ticket.
    pub fn decode(text: &str) -> Result<Self> {
        let encoded = text
            .trim()
            .strip_prefix(PREFIX)
            .ok_or_else(|| anyhow!("a ticket starts with {PREFIX:?}"))?;
        let bytes = BASE64URL_NOPAD
            .decode(encoded.as_bytes())
            .context("the ticket is not base64")?;
        postcard::from_bytes(&bytes).context("the ticket is malformed")
    }
}

#[cfg(test)]
mod tests {
    use noise_room::SecretKey;

    use super::*;

    #[test]
    fn a_ticket_round_trips_through_its_text() {
        let ticket = Ticket {
            inviter: EndpointAddr::from(SecretKey::from_bytes(&[1; 32]).public()),
            token: [7; 16],
        };
        let text = ticket.encode().expect("encode");
        assert!(text.starts_with(PREFIX));
        assert_eq!(
            Ticket::decode(&format!("  {text}\n")).expect("decode"),
            ticket
        );
        assert!(Ticket::decode("other").is_err());
        assert!(Ticket::decode("noise!!!").is_err());
    }
}
