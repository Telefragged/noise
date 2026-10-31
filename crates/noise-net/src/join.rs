//! The join protocol: a joining session redeems the single-use token of a
//! ticket with the member that issued the ticket, and receives the room.
//!
//! The joining session opens a stream and sends the token. The member
//! replies with the room's label, secret and members, or with why it
//! refuses. Each frame is its 4-byte length and its postcard encoding.

use anyhow::{Context, Result, anyhow, bail};
use noise_room::{Member, RoomId, RoomSecret};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// The ALPN of the join protocol.
pub const JOIN_ALPN: &[u8] = b"noise/join/0";

/// The largest frame: an admission with the member list of a large room.
const MAX_FRAME_SIZE: usize = 1 << 20;

/// The single-use secret of a ticket.
pub type Token = [u8; 16];

/// What a joining session receives: the room and its members.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Admission {
    pub label: String,
    pub secret: RoomSecret,
    pub members: Vec<Member>,
}

/// A redeemed token: the admission for the joining session, and the
/// sealed announcement of the admission that the node sends on the room's
/// topic.
#[derive(Debug)]
pub struct Redeemed {
    pub room: RoomId,
    pub admission: Admission,
    pub announcement: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
enum Reply {
    Admitted(Admission),
    Refused(String),
}

/// Sends `token` and returns the admission.
///
/// # Errors
///
/// When the member refuses the token, or a stream fails.
pub(crate) async fn request(
    mut recv: impl AsyncRead + Unpin,
    mut send: impl AsyncWrite + Unpin,
    token: Token,
) -> Result<Admission> {
    write(&mut send, &token).await?;
    send.shutdown().await?;
    match read(&mut recv).await? {
        Reply::Admitted(admission) => Ok(admission),
        Reply::Refused(reason) => bail!("the inviter refused the ticket: {reason}"),
    }
}

/// Reads a token, and replies with the admission that `redeem` returns
/// for it, or with why `redeem` refuses it. Returns what `redeem` returned.
///
/// # Errors
///
/// When a stream fails or the peer breaks the protocol.
pub(crate) async fn respond(
    mut recv: impl AsyncRead + Unpin,
    mut send: impl AsyncWrite + Unpin,
    redeem: impl FnOnce(Token) -> Result<Redeemed, String>,
) -> Result<Option<Redeemed>> {
    let token: Token = read(&mut recv).await?;
    let (reply, redeemed) = match redeem(token) {
        Ok(redeemed) => (Reply::Admitted(redeemed.admission.clone()), Some(redeemed)),
        Err(reason) => (Reply::Refused(reason), None),
    };
    write(&mut send, &reply).await?;
    send.shutdown().await?;
    Ok(redeemed)
}

async fn write(send: &mut (impl AsyncWrite + Unpin), value: &impl Serialize) -> Result<()> {
    let encoded = postcard::to_stdvec(value)?;
    let len = u32::try_from(encoded.len())
        .ok()
        .filter(|len| *len as usize <= MAX_FRAME_SIZE)
        .ok_or_else(|| anyhow!("a frame of {} bytes is too large", encoded.len()))?;
    send.write_u32(len).await?;
    send.write_all(&encoded).await?;
    Ok(())
}

async fn read<T: DeserializeOwned>(recv: &mut (impl AsyncRead + Unpin)) -> Result<T> {
    let len = recv.read_u32().await? as usize;
    if len > MAX_FRAME_SIZE {
        bail!("a frame of {len} bytes is too large");
    }
    let mut encoded = vec![0; len];
    recv.read_exact(&mut encoded).await?;
    postcard::from_bytes(&encoded).context("a malformed frame")
}

#[cfg(test)]
mod tests {
    use noise_room::SecretKey;
    use tokio::io::{duplex, split};

    use super::*;

    fn admission() -> Admission {
        Admission {
            label: "ci".into(),
            secret: RoomSecret::from_bytes([3; 32]),
            members: vec![Member {
                key: SecretKey::from_bytes(&[1; 32]).public(),
                name: "vetle".into(),
                status: String::new(),
            }],
        }
    }

    /// Runs a join over in-memory pipes, with `redeem` on the member's
    /// side.
    async fn join(
        token: Token,
        redeem: impl FnOnce(Token) -> Result<Redeemed, String>,
    ) -> (Result<Admission>, Result<Option<Redeemed>>) {
        let (near, far) = duplex(4096);
        let (near_recv, near_send) = split(near);
        let (far_recv, far_send) = split(far);
        tokio::join!(
            request(near_recv, near_send, token),
            respond(far_recv, far_send, redeem),
        )
    }

    #[tokio::test]
    async fn a_redeemed_token_returns_the_admission() {
        let (joined, redeemed) = join([7; 16], |token| {
            assert_eq!(token, [7; 16]);
            Ok(Redeemed {
                room: RoomSecret::from_bytes([3; 32]).id(),
                admission: admission(),
                announcement: vec![1, 2, 3],
            })
        })
        .await;
        assert_eq!(joined.expect("join"), admission());
        assert_eq!(
            redeemed.expect("respond").expect("redeemed").announcement,
            vec![1, 2, 3]
        );
    }

    #[tokio::test]
    async fn a_refused_token_returns_the_reason() {
        let (joined, redeemed) = join([7; 16], |_| Err("the ticket was used".into())).await;
        let err = joined.expect_err("an error").to_string();
        assert!(err.contains("the ticket was used"), "{err}");
        assert!(redeemed.expect("respond").is_none());
    }
}
