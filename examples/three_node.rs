//! Three-node in-process raf cluster.
//!
//! Wires three `Raf` nodes together through `InProcessNetwork`,
//! drives an election on node 1, then submits a couple of writes
//! through the leader and one through a follower (which is
//! expected to fail).
//!
//! Run with:
//!
//!     cargo run --example three_node

use std::collections::BTreeMap;
use std::io;
use std::time::Duration;

use raf::InProcessNetwork;
use raf::MemStorage;
use raf::Membership;
use raf::Raf;
use raf::WriteRequest;

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let membership = Membership::new(vec![1, 2, 3]);
    let network = InProcessNetwork::new();

    // Construct three nodes, all sharing the same routing
    // network. Each gets its own `MemStorage` (initialised with
    // a single term/cmd seed entry — see `MemStorage::new`).
    let mut nodes: BTreeMap<u64, Raf> = BTreeMap::new();
    for id in [1u64, 2, 3] {
        let raf = Raf::new(id, membership.clone(), MemStorage::new(), network.clone());
        network.insert(id, raf.clone())?;
        nodes.insert(id, raf);
    }

    let leader = nodes[&1].clone();

    // Drive an election on node 1. There is no election timer
    // yet, so the application has to call `elect()` itself.
    leader.elect()?;

    // Wait briefly for the cluster to converge: votes are
    // exchanged, a quorum is reached, and per-peer replication
    // state is initialised.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Submit two writes through the (hopefully) established
    // leader and print the committed log indices.
    for app_id in [42u64, 43] {
        match leader.write(WriteRequest { id: app_id }).await {
            Ok(reply) => println!("committed app_id={app_id} at log index {}", reply.index),
            Err(e) => println!("write app_id={app_id} failed: {e}"),
        }
    }

    // A write addressed to a follower must be rejected.
    let follower = nodes[&2].clone();
    match follower.write(WriteRequest { id: 99 }).await {
        Ok(reply) => println!("unexpected: follower committed at index {}", reply.index),
        Err(e) => println!("follower rejected write as expected: {e}"),
    }

    Ok(())
}
