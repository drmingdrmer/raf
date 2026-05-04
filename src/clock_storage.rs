use std::io;
use std::ops::Range;

use crate::ArrayChunk;
use crate::Index;
use crate::clock::Clock;

pub type ClockChunk = ArrayChunk<Clock>;

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

    pub fn update(&mut self, since: u64, clocks: &[Clock]) -> u64 {
        while self.len() < since {
            self.clocks.push(self.len());
        }

        let end = since + clocks.len() as u64;
        if self.clocks.len() < end as usize {
            self.clocks.resize(end as usize, 0);
        }
        self.clocks[since as usize..end as usize].copy_from_slice(clocks);

        self.clocks.len() as u64
    }

    pub fn read(&self, range: Range<u64>) -> ClockChunk {
        let len = self.len();
        let clocks = if range.start < len {
            let end = range.end.min(len);
            self.clocks[range.start as usize..end as usize].to_vec()
        } else {
            Vec::new()
        };
        ClockChunk { len, entries: clocks }
    }

    pub fn read_one(&self, index: Index) -> Option<Clock> {
        self.clocks.get(index as usize).cloned()
    }

    pub fn last(&self) -> Option<(Index, Clock)> {
        let last = self.clocks.last()?;

        let index = self.len() - 1;
        Some((index, last.clone()))
    }
}
