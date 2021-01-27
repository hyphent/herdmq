use cdrs_tokio::query::*;

use crate::types::{Result, StorageSession};

pub mod packet;
pub mod retain;
pub mod subscription;
mod utils;

static CREATE_KEYSPACE_QUERY: &'static str = r#"
  CREATE KEYSPACE IF NOT EXISTS herdmq
    WITH REPLICATION = {
      'class': 'SimpleStrategy',
      'replication_factor': 1
    };
"#;

pub async fn initialize(session: &StorageSession) -> Result<()> {
  session.query(CREATE_KEYSPACE_QUERY).await.map(|_| (()))?;
  subscription::initialize(session).await?;
  retain::initialize(session).await?;
  packet::initialize(session).await?;
  Ok(())
}