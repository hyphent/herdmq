use cdrs_tokio::{
  authenticators::NoneAuthenticator,
  cluster::{
    session::{ Session },
    TcpConnectionPool
  },
  load_balancing::RoundRobin
};

use crate::error::HerdMQError;

#[derive(Debug, Clone)]
pub struct ClientCredential {
  pub client_id: String,
  pub username: Option<String>,
  pub password: Option<String>
}

pub type Result<T> = std::result::Result<T, HerdMQError>;
pub type Error = HerdMQError;
pub type StorageSession = Session<RoundRobin<TcpConnectionPool<NoneAuthenticator>>>;