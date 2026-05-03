#[derive(Debug)]
pub struct ArrayChunk<T> {
    pub len: u64,
    pub entries: Vec<T>,
}
