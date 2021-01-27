use std::io::Error as IoError;
use cdrs_tokio::error::Error as CDRSError;

use mqtt_codec::error::{EncodeError, DecodeError};

#[derive(Debug)]
pub enum HerdMQError {
  FormatError,
  TimeoutError,
  MPSCError,
  NotAuthorized,
  EncodeError(EncodeError),
  DecodeError(DecodeError),
  IoError(IoError),
  CustomError(String),
  CDRSError(CDRSError)
}

impl From<EncodeError> for HerdMQError {
  fn from(error: EncodeError) -> Self {
    HerdMQError::EncodeError(error)
  }
}

impl From<DecodeError> for HerdMQError {
  fn from(error: DecodeError) -> Self {
    HerdMQError::DecodeError(error)
  }
}

impl From<IoError> for HerdMQError {
  fn from(error: IoError) -> Self {
    HerdMQError::IoError(error)
  }
}

impl From<&str> for HerdMQError {
  fn from(error: &str) -> Self {
    HerdMQError::CustomError(error.to_owned())
  }
}

impl From<CDRSError> for HerdMQError {
  fn from(error: CDRSError) -> Self {
    HerdMQError::CDRSError(error)
  }
}