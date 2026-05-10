use std::ops::Range;

use crate::ArrayChunk;
use crate::LogIndex;
use crate::log_id::LogId;
use crate::term::Term;

pub type TermChunk = ArrayChunk<Term>;

#[derive(Debug, Clone)]
pub struct TermArray {
    terms: Vec<Term>,
}

impl TermArray {
    pub fn new(terms: Vec<Term>) -> Self {
        Self { terms }
    }

    pub fn len(&self) -> u64 {
        self.terms.len() as u64
    }

    // fill term `index` for entry `index` for `[since, len)`
    pub fn fill_gap(&mut self, since: u64) {
        for i in since..self.len() {
            self.terms.push(i);
        }
    }

    pub fn update(&mut self, since: u64, terms: &[Term]) -> u64 {
        while self.len() < since {
            self.terms.push(self.len());
        }

        let end = since + terms.len() as u64;
        if self.terms.len() < end as usize {
            self.terms.resize(end as usize, 0);
        }
        self.terms[since as usize..end as usize].copy_from_slice(terms);

        self.terms.len() as u64
    }

    pub fn read(&self, range: Range<u64>) -> TermChunk {
        let len = self.len();
        let terms = if range.start < len {
            let end = range.end.min(len);
            self.terms[range.start as usize..end as usize].to_vec()
        } else {
            Vec::new()
        };
        TermChunk { len, entries: terms }
    }

    pub fn read_one(&self, index: LogIndex) -> Term {
        *self.terms.get(index as usize).unwrap()
    }

    pub fn last(&self) -> LogId {
        let last = self.terms.last().unwrap();

        let index = self.len() - 1;
        LogId::new(*last, index)
    }
}
