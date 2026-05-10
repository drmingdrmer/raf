#[derive(Debug, Clone)]
/// Opaque command placeholder stored in the replicated log.
pub struct Cmd {}

impl Cmd {
    /// Return an empty command used for reserved or no-op slots.
    pub const fn empty() -> Self {
        Cmd {}
    }
}
