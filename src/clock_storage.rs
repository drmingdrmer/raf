use std::io;
use std::ops::Range;

use crate::Index;
use crate::clock::Clock;

pub struct ClockState {
    pub len: u64,
    pub clocks: Vec<Clock>,
}

#[derive(Debug, Clone)]
pub struct ClockArray {
    clocks: Vec<Clock>,
}

impl ClockArray {
    pub fn new(clocks: Vec<Clock>) -> Self {
        Self { clocks }
    }

    pub fn len(&self) -> u64 {
        self.clocks.len() as u64
    }

    pub fn update(&mut self, since: u64, clocks: &[Clock]) {
        while self.len() < since {
            self.clocks.push(self.len());
        }

        let end = since + clocks.len() as u64;
        if self.clocks.len() < end as usize {
            self.clocks.resize(end as usize, 0);
        }
        self.clocks[since as usize..end as usize].copy_from_slice(clocks);
    }

    pub fn read(&self, range: Range<u64>) -> ClockState {
        let len = self.len();
        let clocks = if range.start < len {
            let end = range.end.min(len);
            self.clocks[range.start as usize..end as usize].to_vec()
        } else {
            Vec::new()
        };
        ClockState { len, clocks }
    }
    
    pub fn last(&self) -> Option<(Index, Clock)> {
        if self.clocks.is_empty() {
            None
        } else {
            let index = self.len() - 1;
            Some((index, self.clocks[index as usize]))
        }
    }
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
