use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("worker error: {0}")]
    Worker(String),

    #[error("server error: {0}")]
    Server(String),

    #[error("{0}")]
    Internal(String),
}

impl Error {
    pub fn worker(msg: impl Into<String>) -> Self {
        Self::Worker(msg.into())
    }

    pub fn server(msg: impl Into<String>) -> Self {
        Self::Server(msg.into())
    }
}
