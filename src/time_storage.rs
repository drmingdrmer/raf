use std::io;

pub trait TimeStorage: Send + Sync + 'static {
    fn update_time(&mut self, since: u64, clocks: &[u64]) -> impl Future<Output = io::Result<()>> + Send;
}
