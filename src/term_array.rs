use std::ops::Range;

use crate::ArrayChunk;
use crate::LogIndex;
use crate::term::Term;

pub type TermChunk = ArrayChunk<Term>;

#[derive(Debug, Clone)]
pub struct TermArray {
    terms: Vec<Term>,
}

impl TermArray {
    pub fn new(mut terms: Vec<Term>) -> Self {
        if terms.is_empty() {
            terms.push(0);
        }

        Self { terms }
    }

    pub fn terms_len(&self) -> u64 {
        self.terms.len() as u64
    }

    // fill term `index` for entry `index` for `[since, len)`
    pub fn fill_terms_gap(&mut self, since: u64) {
        let start = since.min(self.terms_len()) as usize;
        for index in start..self.terms.len() {
            self.terms[index] = index as Term;
        }
    }

    pub fn update_terms(&mut self, since: u64, terms: &[Term]) -> u64 {
        while self.terms_len() < since {
            self.terms.push(self.terms_len());
        }

        let end = since + terms.len() as u64;
        if self.terms.len() < end as usize {
            self.terms.resize(end as usize, 0);
        }
        self.terms[since as usize..end as usize].copy_from_slice(terms);

        self.terms.len() as u64
    }

    pub fn read_terms(&self, range: Range<u64>) -> TermChunk {
        let len = self.terms_len();
        let terms = if range.start < len {
            let end = range.end.min(len);
            self.terms[range.start as usize..end as usize].to_vec()
        } else {
            Vec::new()
        };
        TermChunk { len, entries: terms }
    }

    pub fn read_one_term(&self, index: LogIndex) -> Term {
        *self.terms.get(index as usize).unwrap()
    }

    pub fn last_term(&self) -> Term {
        *self.terms.last().unwrap()
    }
}
