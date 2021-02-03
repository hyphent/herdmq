use mqtt_codec::types::SubscriptionConfig;

use cdrs_tokio::{
  authenticators::NoneAuthenticator,
  cluster::{
    session,
    ClusterTcpConfig, 
    NodeTcpConfigBuilder,
  },
  load_balancing::RoundRobin
};

use crate::types::{Result, StorageSession};

mod query;

pub struct Storage {
  session: StorageSession
}

impl Storage {
  pub async fn new(urls: Vec<&str>) -> Result<Storage> {
    // make connection
    let mut nodes = Vec::new();
    for url in &urls {
      let auth = NoneAuthenticator;
      nodes.push(NodeTcpConfigBuilder::new(url, auth).build());
    }
    let cluster_config = ClusterTcpConfig(nodes);

    println!("Connecting to {:?} for CDRS...", urls);
    let mut session = session::new(&cluster_config, RoundRobin::new()).await?;

    // prepare database
    query::initialize(&mut session).await?;
    println!("CDRS connection to {:?} successful", urls);

    Ok(Storage { session })
  }

  // Finds existing subscriptions given the topic name and returns a vector of client_id and subscription config.
  pub async fn find_subscriptions(&self, topic: &str) -> Result<Vec<(String, SubscriptionConfig)>> {
    query::subscription::find_subscriptions(topic, &self.session).await
  }

  // Find existing subscriptions for a client
  pub async fn find_client_subscriptions(&self, client_id: &str) -> Result<Vec<(String, u8)>> {
    query::subscription::find_client_subscriptions(client_id, &self.session).await
  }

  // Creates new subscriptions for the client with the given options.
  pub async fn store_subscriptions(&self, client_id: &str, subscriptions: &Vec<SubscriptionConfig>) -> Result<()> {
    query::subscription::store_subscriptions(client_id, subscriptions, &self.session).await
  }

  // Remove existing subscriptions given the topic name for the client.
  pub async fn remove_subscriptions(&self, client_id: &str, topics: &Vec<String>) -> Result<()> {
    query::subscription::remove_subscriptions(client_id, topics, &self.session).await
  }
  
  // Remove all the subscriptions for the given client;
  pub async fn remove_all_subscriptions(&self, client_id: &str) -> Result<()> {
    query::subscription::remove_all_subscriptions(client_id, &self.session).await
  }

  // Get the retain message for the given topic
  pub async fn get_retain_message(&self, topic: &str) -> Result<Vec<(String, String)>> {
    query::retain::get_retain_message(topic, &self.session).await
  }

  // Store a retain message for the given topic
  pub async fn store_retain_message(&self, topic: &str, message: &str) -> Result<()> {
    query::retain::store_retain_message(topic, message, &self.session).await
  }

  // Remove retain message for the given topic
  pub async fn remove_retain_message(&self, topic: &str) -> Result<()> {
    query::retain::remove_retain_message(topic, &self.session).await
  }

  // Get all the packets for the client
  pub async fn get_packets_for_client(&self, client_id: &str) -> Result<Vec<(u16, String, String)>> {
    query::packet::get_packets_for_client(client_id, &self.session).await
  }

  // Store a new packet for the client
  pub async fn store_packet_for_client(&self, client_id: &str, topic: &str, message: &str, packet_id: u16) -> Result<()> {
    query::packet::store_packet_for_client(client_id, topic, message, packet_id, &self.session).await
  }

  // Remove packet for the client
  pub async fn remove_packet_for_client(&self, client_id: &str, packet_id: u16) -> Result<()> {
    query::packet::remove_packet_for_client(client_id, packet_id, &self.session).await
  }

  // Remove all packets for the client
  pub async fn remove_all_packets_for_client(&self, client_id: &str) -> Result<()> {
    query::packet::remove_all_packets_for_client(client_id, &self.session).await
  }
}
