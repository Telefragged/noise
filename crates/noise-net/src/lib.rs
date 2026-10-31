//! The iroh layer of noise: the gossip topics of the rooms, the chunks
//! that carry messages in them, the join protocol, and the admission of
//! peers.

pub mod chunk;
mod inbound;
mod node;
#[cfg(test)]
mod tests;

pub use node::{NOT_A_MEMBER, NetEvent, Node, Session};

/// Locks `mutex`. A thread that panicked while it held the lock leaves
/// data that every user here can still read, so the poison is ignored.
fn lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
