//! Three-node in-process raf cluster.
//!
//! Wires three `Raf` nodes together through `InProcessNetwork`,
//! subscribes to node metrics, drives an election on node 1, then
//! submits a couple of writes through the leader and one through a
//! follower (which is expected to fail).
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
use raf::Metrics;
use raf::NodeRole;
use raf::Raf;
use raf::WriteRequest;
use tokio::sync::watch;

const WAIT: Duration = Duration::from_secs(1);

struct StderrLogger;

static LOGGER: StderrLogger = StderrLogger;

impl log::Log for StderrLogger {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.level() <= log::Level::Info
    }

    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            eprintln!("{} {} - {}", record.level(), record.target(), record.args());
        }
    }

    fn flush(&self) {}
}

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    init_stderr_logger()?;

    let membership = Membership::new(vec![1, 2, 3]);
    let network = InProcessNetwork::new();

    // Construct three nodes, all sharing the same routing
    // network. Each gets its own `MemStorage` (initialised with
    // a single term/cmd seed entry — see `MemStorage::new`).
    let mut nodes: BTreeMap<u64, Raf> = BTreeMap::new();
    let mut metrics: BTreeMap<u64, watch::Receiver<Metrics>> = BTreeMap::new();
    for id in [1u64, 2, 3] {
        let raf = Raf::new(id, membership.clone(), MemStorage::new(), network.clone());
        metrics.insert(id, raf.metrics());
        network.insert(id, raf.clone());
        nodes.insert(id, raf);
    }

    for (id, metrics_rx) in metrics.iter_mut() {
        wait_for_metrics(
            metrics_rx,
            |snapshot| snapshot.next_log_slot > 0,
            format!("timed out waiting for initial metrics from node {id}"),
        )
        .await?;
    }
    print_cluster_metrics("initial metrics", &metrics);

    let leader = nodes[&1].clone();

    // Drive an election on node 1. There is no election timer
    // yet, so the application has to call `elect()` itself.
    leader.elect()?;

    // Wait until node 1 reports that it has reached election quorum.
    let leader_snapshot = wait_for_role(metrics.get_mut(&1).unwrap(), NodeRole::Leader).await?;
    log::info!("node 1 established as leader at term {}", leader_snapshot.term);
    print_cluster_metrics("after election", &metrics);

    // Submit two writes through the (hopefully) established
    // leader and print the committed log indices.
    for app_id in [42u64, 43] {
        let reply = tokio::time::timeout(WAIT, leader.write(WriteRequest { id: app_id }))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, format!("timed out writing app_id={app_id}")))??;

        log::info!("committed app_id={app_id} at log index {}", reply.index);
        wait_for_committed(metrics.get_mut(&1).unwrap(), reply.index).await?;
        print_cluster_metrics("after leader write", &metrics);
    }

    // A write addressed to a follower must be rejected.
    let follower = nodes[&2].clone();
    let follower_result = tokio::time::timeout(WAIT, follower.write(WriteRequest { id: 99 }))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "timed out waiting for follower write"))?;
    match follower_result {
        Ok(reply) => log::warn!("unexpected: follower committed at index {}", reply.index),
        Err(e) => log::info!("follower rejected write as expected: {e}"),
    }
    print_cluster_metrics("final metrics", &metrics);

    Ok(())
}

fn init_stderr_logger() -> Result<(), io::Error> {
    log::set_logger(&LOGGER).map_err(|e| io::Error::other(e.to_string()))?;
    log::set_max_level(log::LevelFilter::Info);
    Ok(())
}

async fn wait_for_role(metrics: &mut watch::Receiver<Metrics>, role: NodeRole) -> Result<Metrics, io::Error> {
    wait_for_metrics(
        metrics,
        |snapshot| snapshot.role == role,
        format!("timed out waiting for role {role:?}"),
    )
    .await
}

async fn wait_for_committed(metrics: &mut watch::Receiver<Metrics>, index: u64) -> Result<Metrics, io::Error> {
    wait_for_metrics(
        metrics,
        |snapshot| snapshot.committed >= index,
        format!("timed out waiting for committed index {index}"),
    )
    .await
}

async fn wait_for_metrics(
    metrics: &mut watch::Receiver<Metrics>,
    mut predicate: impl FnMut(&Metrics) -> bool,
    timeout_message: String,
) -> Result<Metrics, io::Error> {
    tokio::time::timeout(WAIT, async {
        loop {
            let snapshot = metrics.borrow().clone();
            if predicate(&snapshot) {
                return Ok(snapshot);
            }

            metrics.changed().await.map_err(|_| io::Error::other("metrics channel closed"))?;
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, timeout_message))?
}

fn print_cluster_metrics(label: &str, metrics: &BTreeMap<u64, watch::Receiver<Metrics>>) {
    log::info!("--- {label} ---");
    for metrics_rx in metrics.values() {
        print_metrics(&metrics_rx.borrow());
    }
}

fn print_metrics(metrics: &Metrics) {
    let replications = metrics
        .replications
        .values()
        .map(|replication| {
            format!(
                "{}:matched={} end={} inflight={}",
                replication.target, replication.matched, replication.end, replication.inflight
            )
        })
        .collect::<Vec<_>>()
        .join(", ");

    let replications = if replications.is_empty() {
        "none".to_string()
    } else {
        replications
    };

    log::info!(
        "node={} role={:?} term={} committed={} next_term_slot={} next_log_slot={} votes={:?} replications=[{}]",
        metrics.id,
        metrics.role,
        metrics.term,
        metrics.committed,
        metrics.next_term_slot,
        metrics.next_log_slot,
        metrics.granted_votes,
        replications
    );
}
