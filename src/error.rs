#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("rpc: {0}")]
    Status(#[from] tonic::Status),

    #[error("transport: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("deserialize: {0}")]
    Deserialize(String),

    #[error("auth: {0}")]
    Auth(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_tonic_status() {
        let status = tonic::Status::not_found("missing");
        let err: Error = status.into();
        assert!(matches!(err, Error::Status(s) if s.code() == tonic::Code::NotFound));
    }

    #[test]
    fn error_display() {
        let err = Error::Deserialize("bad value".into());
        assert_eq!(err.to_string(), "deserialize: bad value");

        let err = Error::Auth("token expired".into());
        assert_eq!(err.to_string(), "auth: token expired");
    }
}
