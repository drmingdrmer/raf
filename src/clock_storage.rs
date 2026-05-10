use std::ops::Range;

use crate::ArrayChunk;
use crate::LogIndex;
use crate::log_id::LogId;
use crate::term::Term;

pub type ClockChunk = ArrayChunk<Term>;

#[derive(Debug, Clone)]
pub struct TermArray {
    clocks: Vec<Term>,
}

impl TermArray {
    pub fn new(clocks: Vec<Term>) -> Self {
        Self { clocks }
    }

    pub fn len(&self) -> u64 {
        self.clocks.len() as u64
    }

    // fill `index` for entry `index` for `[since, len)`
    pub fn fill_gap(&mut self, since: u64) {
        for i in since..self.len() {
            self.clocks.push(i);
        }
    }

    pub fn update(&mut self, since: u64, clocks: &[Term]) -> u64 {
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

    pub fn read_one(&self, index: LogIndex) -> Term {
        *self.clocks.get(index as usize).unwrap()
    }

    pub fn last(&self) -> LogId {
        let last = self.clocks.last().unwrap();

        let index = self.len() - 1;
        LogId::new(*last, index)
    }
}
