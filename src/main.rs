mod broker;
mod error;
mod types;
mod connection;
mod storage;
mod trie;
mod authenticator;

use tokio::runtime;

use crate::{
  broker::Broker,
  storage::Storage,
  authenticator::Authenticator,
  types::ClientCredential
};

fn authenticate_connection(client_credential: &ClientCredential) -> bool {
  true
}

fn authenticate_subscribe(client_credential: &ClientCredential, topic: &str) -> bool {
  topic.split('/').collect::<Vec<&str>>()[0] != "$herdmq"
}

fn authenticate_publish(client_credential: &ClientCredential, topic: &str) -> bool {
  topic.split('/').collect::<Vec<&str>>()[0] != "$herdmq"
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
  let rt = runtime::Runtime::new()?;

  rt.block_on(async {
    let broker_url = std::env::var("BROKER_URL").ok().unwrap_or("0.0.0.0:1883".to_owned());

    let broker_id = "hello";
    let authenticator = Authenticator::new(authenticate_connection, authenticate_subscribe, authenticate_publish);
    let storage = Storage::new(Vec::from(["127.0.0.1:9042", "127.0.0.1:9043"])).await.unwrap();

    let broker = Broker::new(broker_id, authenticator, storage);
    broker.run(broker_url).await.unwrap()
  });
  Ok(())
}
