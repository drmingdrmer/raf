use std::io;
use std::ops::Range;

pub struct ClockState {
    /// The current clock value, as of the last update.
    pub len: u64,
    /// The clock values of all known peers, as of the last update.
    pub peer_clocks: Vec<u64>,
}

pub trait ClockStorage: Send + Sync + 'static {
    fn update(&mut self, since: u64, clocks: &[u64]) -> impl Future<Output = io::Result<()>> + Send;

    fn read(&self, range: Range<u64>) -> impl Future<Output = io::Result<Vec<u64>>> + Send;
}
