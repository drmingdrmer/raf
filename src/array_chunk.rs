#[derive(Debug)]
pub struct ArrayChunk<T> {
    pub len: u64,
    pub entries: Vec<T>,
}

impl<T> ArrayChunk<T> {
    pub fn new(len: u64, entries: Vec<T>) -> Self {
        Self { len, entries }
    }
}
