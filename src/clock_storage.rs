use std::io;
use std::ops::Range;

use crate::Index;
use crate::clock::Clock;

pub struct ClockState {
    pub len: u64,
    pub clocks: Vec<Clock>,
}

pub trait ClockStorage: Send + Sync + 'static {
    fn update(&mut self, since: u64, clocks: &[Clock]) -> impl Future<Output = io::Result<()>> + Send;

    fn read(&self, range: Range<u64>) -> impl Future<Output = io::Result<ClockState>> + Send;

    fn last(&self) -> impl Future<Output = io::Result<Option<(Index, Clock)>>> + Send {
        async move {
            let state = self.read(0..0).await?;
            if state.len == 0 {
                Ok(None)
            } else {
                let index = state.len - 1;
                let state = self.read(index..index + 1).await?;
                Ok(Some((index, state.clocks[0])))
            }
        }
    }
}
