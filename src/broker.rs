use std::{
  collections::{HashMap, HashSet},
  sync::Arc
};

use tokio::{
  net::{ToSocketAddrs, TcpListener},
  sync::{
    Mutex,
    mpsc::{channel, Sender, Receiver}
  }
};

use rand::Rng;

use mqtt_codec::types::*;

use crate::{
  types::{Result, Error, ClientCredential},
  connection::Connection,
  storage::Storage,
  trie::TopicTrie,
  error::HerdMQError,
  authenticator::Authenticator
};

const CHANNEL_CAPACITY: usize = 100;
// const BROKER_PREFIX: &'static str = "herdmq_";

type Session = (Sender<DecodedPacket>, Option<WillConfig>, ClientCredential);

pub struct Broker<C, S, P>
where 
C: Fn(&ClientCredential) -> bool + Sync + Send + 'static, 
S: Fn(&ClientCredential, &str) -> bool + Sync + Send + 'static,
P: Fn(&ClientCredential, &str) -> bool + Sync + Send + 'static
{
  id: &'static str,
  authenticator: Authenticator<C, S, P>,
  storage: Storage
}

impl <C,S,P> Broker <C, S, P>
where 
C: Fn(&ClientCredential) -> bool + Sync + Send + 'static, 
S: Fn(&ClientCredential, &str) -> bool + Sync + Send + 'static,
P: Fn(&ClientCredential, &str) -> bool + Sync + Send + 'static
{
  pub fn new(id: &'static str, authenticator: Authenticator<C, S, P>, storage: Storage) -> Self {
    Broker {
      id,
      authenticator,
      storage
    }
  }

  async fn listen(self, mut rx: Receiver<(String, BrokerMessage)>) -> Result<()> {
    let broker_handler = BrokerHandler {
      broker_id: Arc::new(self.id),
      authenticator: Arc::new(self.authenticator),
      trie: Arc::new(Mutex::new(TopicTrie::new())),
      subscription_table: Arc::new(Mutex::new(HashMap::new())),
      routing_table: Arc::new(Mutex::new(HashMap::new())),
      sessions: Arc::new(Mutex::new(HashMap::new())),
      storage: Arc::new(self.storage)
    };

    let broker_handler = Arc::new(broker_handler);

    while let Some((client_id, message)) = rx.recv().await {
      let broker_handler = broker_handler.clone();

      tokio::spawn(async move {
        broker_handler.handle_message(client_id, message).await.unwrap();
      });
    }
    Ok(())
  }

  pub async fn run<T: ToSocketAddrs>(self, url: T) -> Result<()> {
    let listener = TcpListener::bind(url).await.unwrap();
    let (tx, rx) = channel(CHANNEL_CAPACITY);

    tokio::spawn(async move {
      self.listen(rx).await.unwrap();
    });

    loop {
      let (socket, addr) = listener.accept().await.unwrap();
      let tx = tx.clone();
      tokio::spawn(async move {
        let connection = Connection::handle_handshake(socket, tx).await.unwrap();
        connection.listen().await.unwrap();
      });
    }
  }
}

pub struct BrokerHandler<C, S, P>
where
C: Fn(&ClientCredential) -> bool + Sync + Send + 'static, 
S: Fn(&ClientCredential, &str) -> bool + Sync + Send + 'static,
P: Fn(&ClientCredential, &str) -> bool + Sync + Send + 'static
{
  broker_id: Arc<&'static str>,
  authenticator: Arc<Authenticator<C, S, P>>,
  sessions: Arc<Mutex<HashMap<String, Session>>>,
  subscription_table: Arc<Mutex<HashMap<String, HashMap<String, u8>>>>,
  routing_table: Arc<Mutex<HashMap<String, HashSet<String>>>>,
  trie: Arc<Mutex<TopicTrie>>,
  storage: Arc<Storage>
}

impl <C, S, P> BrokerHandler<C, S, P>
where
C: Fn(&ClientCredential) -> bool + Sync + Send + 'static, 
S: Fn(&ClientCredential, &str) -> bool + Sync + Send + 'static,
P: Fn(&ClientCredential, &str) -> bool + Sync + Send + 'static
{
  async fn insert_broker_subscription(&self, topic_name: &str, broker_id: &str) -> bool {
    let mut table = self.routing_table.lock().await;
    let set = table.entry(topic_name.to_owned()).or_insert(HashSet::new());
    if !set.contains(broker_id) {
      set.insert(broker_id.to_owned());
      self.trie.lock().await.insert(topic_name, broker_id);
      true
    } else {
      false
    }
  }

  async fn insert_client_subscription(&self, topic_name: &str, client_id: &str, qos: u8) {
    let mut table = self.subscription_table.lock().await;
    let map = table.entry(topic_name.to_owned()).or_insert(HashMap::new());
    map.insert(client_id.to_owned(), qos);
  }

  async fn remove_broker_subscription(&self, topic_name: &str, broker_id: &str) {
    let mut table = self.routing_table.lock().await;
    if let Some(set) = table.get_mut(topic_name) {
      set.remove(broker_id);
      self.trie.lock().await.remove(topic_name, broker_id);
    }
  }

  async fn remove_client_subscription(&self, topic_name: &str, client_id: &str) -> bool {
    let mut table = self.subscription_table.lock().await;
    if let Some(map) = table.get_mut(topic_name) {
      map.remove(client_id);
      map.is_empty()
    } else {
      true
    }
  }

  async fn forward_subscription(&self, topic_name: &str) -> Result<()> {
    // TODO send the subscription to other broker
    Ok(())
  }
  
  async fn forward_packet(&self, packet: &PublishPacket, broker_id: &str) -> Result<()> {
    // TODO forward to packet to appropricate brokers
    Ok(())
  }

  async fn forward_packet_to_clients(&self, packet: &PublishPacket, topic: &str) -> Result<()> {
    if let Some(subscriptions) = self.subscription_table.lock().await.get(topic) {
      // send the message to appropriate topics
      for (cid, qos) in subscriptions.iter() {
        let qos = std::cmp::min(*qos, packet.config.qos);
        let packet_id = match qos {
          0 => None,
          _ => packet.packet_id
        };

        let packet_to_publish = PublishPacket {
          topic_name: packet.topic_name.to_owned(),
          packet_id: packet_id,
          payload: packet.payload.to_owned(),
          config: PublishConfig {
            dup: false,
            qos: qos,
            retain: false
          },
          properties: vec![]
        };

        self.send_to_client(cid, DecodedPacket::Publish(packet_to_publish.clone())).await?;
      }
    }

    Ok(())
  }

  async fn send_to_client(&self, client_id: &str, packet: DecodedPacket) -> Result<bool> {
    let connection_tx = match self.sessions.lock().await.get(client_id) {
      Some((connection_tx, _, _)) => connection_tx.clone(),
      _ => return Ok(false)
    };
    connection_tx.send(packet).await.map_err(|_| HerdMQError::MPSCError)?;
    Ok(true)
  }

  async fn handle_new_connection(&self, client_id: &str, packet: &ConnectPacket) -> Result<()> {
    let properties = vec![
      Property::MaximumQoS(1),
      Property::WildcardSubscriptionAvailable(false)
    ];

    if packet.clean_start {
      self.storage.remove_all_subscriptions(&client_id).await?;
      self.storage.remove_all_packets_for_client(&client_id).await?;

      self.send_to_client(&client_id, DecodedPacket::Connack(ConnackPacket {
        session_present: false,
        reason_code: ReasonCode::Success,
        properties: properties
      })).await?;
    } else {
      let subscriptions = self.storage.find_client_subscriptions(&client_id).await?;
      for (topic_name, qos) in &subscriptions {
        self.insert_client_subscription(topic_name, client_id, *qos).await;
        if self.insert_broker_subscription(topic_name, &self.broker_id).await {
          self.forward_subscription(topic_name).await?;
        }
      }

      self.send_to_client(&client_id, DecodedPacket::Connack(ConnackPacket {
        session_present: subscriptions.len() > 0,
        reason_code: ReasonCode::Success,
        properties: properties
      })).await?;

      let packets = self.storage.get_packets_for_client(&client_id).await?; 
      
      for (packet_id, (topic_name, message)) in &packets {
        self.send_to_client(&client_id, DecodedPacket::Publish(PublishPacket {
          topic_name: topic_name.to_owned(),
          packet_id: Some(*packet_id),
          payload: message.to_owned(),
          config: PublishConfig {
            dup: false,
            qos: 1,
            retain: true
          },
          properties: vec![]
        })).await?;
      }
    }
    Ok(())
  }

  async fn handle_subscribe(&self, client_id: &str, packet: &SubscribePacket) -> Result<()> {
    let mut reason_codes = Vec::new();
    let mut subscriptions = Vec::new();
    let client_credential = match self.sessions.lock().await.get(client_id) {
      Some((_, _, client_credential)) => client_credential.clone(),
      None => return Err(HerdMQError::MPSCError)
    };

    for subscription in packet.subscriptions.iter() {
      if (self.authenticator.subscribe)(&client_credential, &subscription.topic_name) {
        subscriptions.push(subscription.clone());

        self.insert_client_subscription(&subscription.topic_name, client_id, subscription.qos).await;
        if self.insert_broker_subscription(&subscription.topic_name, &self.broker_id).await {
          self.forward_subscription(&subscription.topic_name).await?;
        }

        let reason_code = match subscription.qos {
          0 => ReasonCode::Success,
          _ => ReasonCode::GrantedQoS1,
        };
        reason_codes.push(reason_code);

        // get the retain messages
        let retain_messages = self.storage.get_retain_message(&subscription.topic_name).await?;
        for (topic_name, retain_message) in &retain_messages {
          self.send_to_client(&client_id, DecodedPacket::Publish(PublishPacket {
            topic_name: topic_name.to_owned(),
            packet_id: None,
            payload: retain_message.to_owned(),
            config: PublishConfig {
              dup: false,
              qos: 0,
              retain: true
            },
            properties: vec![]
          })).await?;
        }
      } else {
        reason_codes.push(ReasonCode::NotAuthorized);
      }
    }


    self.storage.store_subscriptions(&client_id, &subscriptions).await?;

    self.send_to_client(&client_id, DecodedPacket::Suback(SubackPacket {
      packet_id: packet.packet_id,
      reason_codes: reason_codes,
      properties: vec![]
    })).await?;

    Ok(())
  }

  async fn handle_publish(&self, client_id: &str, packet: &PublishPacket) -> Result<()> {
    let client_credential = match self.sessions.lock().await.get(client_id) {
      Some((_, _, client_credential)) => client_credential.clone(),
      None => return Err(HerdMQError::MPSCError)
    };

    if (self.authenticator.publish)(&client_credential, &packet.topic_name) {
      // store retain packet
      if packet.config.retain {
        if packet.payload == "" {
          self.storage.remove_retain_message(&packet.topic_name).await?;
        } else {
          self.storage.store_retain_message(&packet.topic_name, &packet.payload).await?;
        }
      }

      // handle qos = 1
      if packet.config.qos > 0 {
        let packet_id = packet.packet_id.ok_or_else(|| Error::FormatError)?;

        // store offline packets
        let subscriptions = self.storage.find_subscriptions(&packet.topic_name).await?;

        for (cid, subscription_config) in subscriptions.iter() {
          if subscription_config.qos > 0 {
            self.storage.store_packet_for_client(&cid, &packet.topic_name, &packet.payload, packet_id).await?;
          }
        }

        // send puback
        self.send_to_client(&client_id, DecodedPacket::Puback(PubackPacket {
          packet_id: packet_id,
          reason_code: ReasonCode::Success,
          properties: vec![]
        })).await?;
      }

      let subscriptions = self.trie.lock().await.get(&packet.topic_name);

      for (topic, broker_ids) in subscriptions {
        // this could potentially happens in other threads
        for broker_id in broker_ids {
          if broker_id == *self.broker_id {
            self.forward_packet_to_clients(&packet, &topic).await?;
          } else {
            self.forward_packet(&packet, &broker_id).await?;
          }
        }
      }
    } else {
      // send puback
      if packet.config.qos > 0 {
        let packet_id = packet.packet_id.ok_or_else(|| Error::FormatError)?;
        self.send_to_client(&client_id, DecodedPacket::Puback(PubackPacket {
          packet_id: packet_id,
          reason_code: ReasonCode::NotAuthorized,
          properties: vec![]
        })).await?;
      }
    }
    Ok(())
  }

  
  async fn handle_message(&self, client_id: String, message: BrokerMessage) -> Result<()> {
    match message {
      BrokerMessage::NewConnection(connection_tx, packet) => {
        let client_credential = ClientCredential {
          client_id: client_id.clone(),
          username: packet.username.clone(),
          password: packet.password.clone()
        };

        self.sessions.lock().await.insert(
          client_id.clone(),
          (connection_tx, packet.will_config.clone(), client_credential.clone())
        );

        if (self.authenticator.connection)(&client_credential) {
          self.handle_new_connection(&client_id, &packet).await?;
        } else {
          self.send_to_client(&client_id, DecodedPacket::Connack(ConnackPacket {
            session_present: false,
            reason_code: ReasonCode::NotAuthorized,
            properties: vec![]
          })).await?;
          self.sessions.lock().await.remove(&client_id);
        }
      }
      BrokerMessage::Publish(packet) => {
        self.handle_publish(&client_id, &packet).await?;
      }
      BrokerMessage::Puback(packet) => {
        self.storage.remove_packet_for_client(&client_id, packet.packet_id).await?;
      }    
      BrokerMessage::Subscribe(packet) => {
        self.handle_subscribe(&client_id, &packet).await?;
      }
      BrokerMessage::Unsubscribe(packet) => {
        self.storage.remove_subscriptions(&client_id, &packet.topic_names).await?;

        for topic in &packet.topic_names {
          if self.remove_client_subscription(topic, &client_id).await {
            self.remove_broker_subscription(topic, &self.broker_id).await;
          }
        }

        self.send_to_client(&client_id, DecodedPacket::Unsuback(UnsubackPacket {
          packet_id: packet.packet_id,
          reason_codes: vec![ReasonCode::Success],
          properties: vec![]
        })).await?;
      }
      BrokerMessage::CloseConnection => {
        let maybe_will_config = match self.sessions.lock().await.get(&client_id) {
          Some((_, Some(will_config), _)) => Some(will_config.clone()),
          _ => None
        };

        let subscriptions = self.storage.find_client_subscriptions(&client_id).await?;
        for (topic_name, _) in &subscriptions {
          if self.remove_client_subscription(topic_name, &client_id).await {
            self.remove_broker_subscription(topic_name, &self.broker_id).await;
          }
        }

        self.sessions.lock().await.remove(&client_id);

        if let Some(will_config) = maybe_will_config {
          let mut packet_id = None;
          
          if will_config.qos > 0 {
            let mut rng = rand::thread_rng();
            packet_id = Some(rng.gen());
          }

          self.handle_publish(&client_id, &PublishPacket {
            topic_name: will_config.topic_name,
            packet_id: packet_id,
            payload: will_config.payload,
            config: PublishConfig {
              dup: false,
              qos: will_config.qos,
              retain: will_config.retain
            },
            properties: vec![]
          }).await?;
        };
      }
    }

    Ok(())
  }
}

#[derive(Debug)]
pub enum BrokerMessage {
  NewConnection(Sender<DecodedPacket>, ConnectPacket),
  Publish(PublishPacket),
  Puback(PubackPacket),
  Subscribe(SubscribePacket),
  Unsubscribe(UnsubscribePacket),
  CloseConnection
}