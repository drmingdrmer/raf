#[derive(Debug)]
/// A contiguous slice read from an array-like storage object.
pub struct ArrayChunk<T> {
    /// Total length of the source array at the time of the read.
    pub len: u64,

    /// Entries returned for the requested range.
    pub entries: Vec<T>,
}

impl<T> ArrayChunk<T> {
    /// Build a chunk with the source length and returned entries.
    pub fn new(len: u64, entries: Vec<T>) -> Self {
        Self { len, entries }
    }
}
