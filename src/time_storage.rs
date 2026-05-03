use std::io;
use std::ops::Range;

pub struct ClockState {
    pub len: u64,
    pub clocks: Vec<u64>,
}

pub trait ClockStorage: Send + Sync + 'static {
    fn update(&mut self, since: u64, clocks: &[u64]) -> impl Future<Output = io::Result<()>> + Send;

    fn read(&self, range: Range<u64>) -> impl Future<Output = io::Result<ClockState>> + Send;

    fn last(&self) -> impl Future<Output = io::Result<u64>> + Send {
        async move {
            let state = self.read(0..0).await?;
            if state.len == 0 {
                Ok(0)
            } else {
                let state = self.read(state.len - 1..state.len).await?;
                Ok(state.clocks[0])
            }
        }
    }
}
