use std::ops::Range;

use crate::ArrayChunk;
use crate::term::Term;

/// Term entries returned together with the source term-array length.
pub type TermChunk = ArrayChunk<Term>;

#[derive(Debug, Clone)]
/// In-memory array of log terms.
pub struct TermArray {
    /// Stored term entries.
    terms: Vec<Term>,
}

impl TermArray {
    /// Create a term array, inserting term zero at index zero if needed.
    pub fn new(mut terms: Vec<Term>) -> Self {
        if terms.is_empty() {
            terms.push(0);
        }

        Self { terms }
    }

    /// Return the number of stored terms.
    pub fn terms_len(&self) -> u64 {
        self.terms.len() as u64
    }

    /// Overwrite terms starting at `since`, extending the array if needed.
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

    /// Read a term range and return the source length with the entries.
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
}
