//! The chunks that carry a sealed message in gossip messages of a fixed
//! size.
//!
//! A chunk is a header (the BLAKE3 hash of the sealed message, the
//! chunk's index, and the chunk count) and a slice of the sealed message.

use std::collections::HashMap;

use noise_room::{MAX_SEALED_SIZE, PublicKey};

/// The hash of the sealed message, the index, and the count.
pub const HEADER_SIZE: usize = 32 + 2 + 2;

/// The bytes that iroh-gossip's own framing of a message needs, rounded
/// up. Its limit covers the whole frame, and a broadcast over it closes
/// the connection to the neighbor.
const GOSSIP_OVERHEAD: usize = 64;

/// The most messages that a [`Reassembly`] collects at once for the chunks
/// that one neighbor delivered first.
pub const MAX_PARTIAL: usize = 16;

/// The most data one chunk carries when iroh-gossip limits a message to
/// `max_message_size` bytes.
#[must_use]
pub fn chunk_data_size(max_message_size: usize) -> usize {
    max_message_size - GOSSIP_OVERHEAD - HEADER_SIZE
}

/// Splits `sealed`, a sealed message, into the chunks to broadcast.
#[must_use]
pub fn split(sealed: &[u8], max_message_size: usize) -> Vec<Vec<u8>> {
    let slices: Vec<&[u8]> = sealed.chunks(chunk_data_size(max_message_size)).collect();
    let count = u16::try_from(slices.len()).unwrap_or(u16::MAX);
    let id = blake3::hash(sealed);
    (0..count)
        .zip(slices)
        .map(|(index, slice)| {
            [
                id.as_bytes(),
                &index.to_be_bytes()[..],
                &count.to_be_bytes(),
                slice,
            ]
            .concat()
        })
        .collect()
}

/// A parsed chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Chunk<'a> {
    id: [u8; 32],
    index: u16,
    count: u16,
    data: &'a [u8],
}

/// The chunk in a gossip message, or `None` when it is malformed.
#[must_use]
pub fn parse(payload: &[u8]) -> Option<Chunk<'_>> {
    Some(Chunk {
        id: payload.get(..32)?.try_into().ok()?,
        index: u16::from_be_bytes(payload.get(32..34)?.try_into().ok()?),
        count: u16::from_be_bytes(payload.get(34..36)?.try_into().ok()?),
        data: payload.get(HEADER_SIZE..)?,
    })
}

/// The chunks of messages that have not all arrived.
#[derive(Debug)]
pub struct Reassembly {
    max_count: u16,
    partial: HashMap<[u8; 32], Partial>,
    /// The number of messages started so far, which orders them by age.
    started: u64,
}

#[derive(Debug)]
struct Partial {
    slices: Vec<Option<Vec<u8>>>,
    missing: usize,
    started: u64,
    /// The neighbor that delivered the message's first chunk.
    from: PublicKey,
}

impl Reassembly {
    #[must_use]
    pub fn new(max_message_size: usize) -> Self {
        let max_data = MAX_SEALED_SIZE.div_ceil(chunk_data_size(max_message_size));
        Self {
            max_count: u16::try_from(max_data).unwrap_or(u16::MAX),
            partial: HashMap::new(),
            started: 0,
        }
    }

    /// Adds `chunk`, which the neighbor `from` delivered, and returns the
    /// sealed message when the chunk completes it.
    ///
    /// A neighbor's first chunk of a message starts it, and the
    /// reassembly collects at most [`MAX_PARTIAL`] messages that one
    /// neighbor started: another one drops that neighbor's oldest. The
    /// chunks of one message may arrive from several neighbors.
    pub fn add(&mut self, chunk: Chunk<'_>, from: PublicKey) -> Option<Vec<u8>> {
        if chunk.count == 0 || chunk.count > self.max_count || chunk.index >= chunk.count {
            return None;
        }
        if !self.partial.contains_key(&chunk.id) {
            self.drop_oldest(from);
            self.started += 1;
            let partial = Partial {
                slices: vec![None; usize::from(chunk.count)],
                missing: usize::from(chunk.count),
                started: self.started,
                from,
            };
            self.partial.insert(chunk.id, partial);
        }
        let partial = self.partial.get_mut(&chunk.id)?;
        if partial.slices.len() != usize::from(chunk.count) {
            // The chunks of one message disagree on the count.
            self.partial.remove(&chunk.id);
            return None;
        }
        let slot = &mut partial.slices[usize::from(chunk.index)];
        if slot.is_none() {
            *slot = Some(chunk.data.to_vec());
            partial.missing -= 1;
        }
        if partial.missing > 0 {
            return None;
        }
        let sealed: Vec<u8> = self
            .partial
            .remove(&chunk.id)?
            .slices
            .into_iter()
            .flatten()
            .flatten()
            .collect();
        (blake3::hash(&sealed).as_bytes() == &chunk.id).then_some(sealed)
    }

    /// Drops the oldest message that `from` started, when it started
    /// [`MAX_PARTIAL`].
    fn drop_oldest(&mut self, from: PublicKey) {
        let started: Vec<([u8; 32], u64)> = self
            .partial
            .iter()
            .filter(|(_, partial)| partial.from == from)
            .map(|(id, partial)| (*id, partial.started))
            .collect();
        if started.len() < MAX_PARTIAL {
            return;
        }
        if let Some((oldest, _)) = started.iter().min_by_key(|(_, started)| *started) {
            self.partial.remove(oldest);
        }
    }
}

#[cfg(test)]
mod tests {
    use noise_room::SecretKey;

    use super::*;

    const MAX: usize = 4096;

    fn neighbor(n: u8) -> PublicKey {
        SecretKey::from_bytes(&[n; 32]).public()
    }

    /// Bytes that stand for a sealed message of `len` bytes.
    fn sealed(len: usize, seed: u8) -> Vec<u8> {
        (0..len)
            .map(|n| {
                u8::try_from(n % 256)
                    .expect("fits in a byte")
                    .wrapping_add(seed)
            })
            .collect()
    }

    fn reassemble(chunks: &[Vec<u8>]) -> Option<Vec<u8>> {
        let mut reassembly = Reassembly::new(MAX);
        chunks
            .iter()
            .find_map(|payload| reassembly.add(parse(payload).expect("parse"), neighbor(1)))
    }

    #[test]
    fn a_small_message_is_one_chunk() {
        let message = sealed(10, 0);
        let chunks = split(&message, MAX);
        assert_eq!(chunks.len(), 1);
        assert_eq!(reassemble(&chunks), Some(message));
    }

    #[test]
    fn a_message_at_the_chunk_size_is_one_chunk_of_the_full_size() {
        let message = sealed(chunk_data_size(MAX), 0);
        let chunks = split(&message, MAX);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), MAX - GOSSIP_OVERHEAD);
        assert_eq!(reassemble(&chunks), Some(message));
    }

    #[test]
    fn a_large_message_arrives_in_any_order_from_any_neighbor() {
        let message = sealed(MAX_SEALED_SIZE, 0);
        let mut chunks = split(&message, MAX);
        assert!(chunks.len() > 10);
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.len() <= MAX - GOSSIP_OVERHEAD)
        );
        chunks.reverse();
        let mut reassembly = Reassembly::new(MAX);
        let complete = chunks.iter().enumerate().find_map(|(n, payload)| {
            let from = neighbor(u8::try_from(n % 3).expect("fits in a byte"));
            reassembly.add(parse(payload).expect("parse"), from)
        });
        assert_eq!(complete, Some(message));
    }

    #[test]
    fn a_chunk_from_another_message_under_the_same_hash_is_dropped() {
        let (one, two) = (sealed(10_000, 0), sealed(10_000, 1));
        let mut chunks = split(&one, MAX);
        let other = split(&two, MAX);
        chunks[1][HEADER_SIZE..].copy_from_slice(&other[1][HEADER_SIZE..]);
        assert_eq!(reassemble(&chunks), None);
    }

    #[test]
    fn a_neighbor_past_the_limit_loses_its_oldest_message() {
        let split_all: Vec<Vec<Vec<u8>>> = (0..=MAX_PARTIAL)
            .map(|seed| {
                split(
                    &sealed(10_000, u8::try_from(seed).expect("fits in a byte")),
                    MAX,
                )
            })
            .collect();
        let mut reassembly = Reassembly::new(MAX);
        for chunks in &split_all {
            assert!(
                reassembly
                    .add(parse(&chunks[0]).expect("parse"), neighbor(1))
                    .is_none()
            );
        }
        // Another neighbor's messages do not count against neighbor 1.
        let other = split(&sealed(10_000, 99), MAX);
        reassembly.add(parse(&other[0]).expect("parse"), neighbor(2));
        let rest = |chunks: &[Vec<u8>], reassembly: &mut Reassembly| {
            chunks[1..]
                .iter()
                .find_map(|payload| reassembly.add(parse(payload).expect("parse"), neighbor(1)))
        };
        assert!(rest(&split_all[0], &mut reassembly).is_none(), "dropped");
        assert!(rest(&split_all[MAX_PARTIAL], &mut reassembly).is_some());
        assert!(rest(&other, &mut reassembly).is_some());
    }

    #[test]
    fn a_chunk_with_an_impossible_count_is_dropped() {
        let chunks = split(&sealed(10, 0), MAX);
        let chunk = parse(&chunks[0]).expect("parse");
        let mut reassembly = Reassembly::new(MAX);
        let too_many = Chunk {
            count: 1000,
            ..chunk
        };
        assert!(reassembly.add(too_many, neighbor(1)).is_none());
        let beyond = Chunk { index: 1, ..chunk };
        assert!(reassembly.add(beyond, neighbor(1)).is_none());
    }
}
