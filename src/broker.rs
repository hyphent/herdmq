use std::{
  collections::{HashMap, HashSet},
  sync::Arc
};

use tokio::{
  net::{TcpListener, ToSocketAddrs},
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
  cluster::ClusterClient,
  storage::Storage,
  error::HerdMQError,
  authenticator::Authenticator
};

const CHANNEL_CAPACITY: usize = 100;

type Session = (Sender<DecodedPacket>, Option<WillConfig>, ClientCredential);

pub struct Broker<C, S, P>
where 
C: Fn(&ClientCredential) -> bool + Sync + Send + 'static, 
S: Fn(&ClientCredential, &str) -> bool + Sync + Send + 'static,
P: Fn(&ClientCredential, &str) -> bool + Sync + Send + 'static,
{
  id: String,
  authenticator: Authenticator<C, S, P>,
  storage: Storage
}

impl <C, S, P> Broker <C, S, P>
where 
C: Fn(&ClientCredential) -> bool + Sync + Send + 'static, 
S: Fn(&ClientCredential, &str) -> bool + Sync + Send + 'static,
P: Fn(&ClientCredential, &str) -> bool + Sync + Send + 'static,
{
  pub fn new(id: String, authenticator: Authenticator<C, S, P>, storage: Storage) -> Self {
    Broker {
      id,
      authenticator,
      storage
    }
  }

  async fn listen(self, mut rx: Receiver<(String, BrokerMessage)>, cluster_client: Arc<ClusterClient>) -> Result<()> {
    let broker_handler = BrokerHandler {
      broker_id: Arc::new(self.id),
      authenticator: Arc::new(self.authenticator),
      subscription_table: Arc::new(Mutex::new(HashMap::new())),
      sessions: Arc::new(Mutex::new(HashMap::new())),
      storage: Arc::new(self.storage),
      cluster_client
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

  pub async fn run<A: ToSocketAddrs, B: 'static + ToSocketAddrs + Send, T: ToSocketAddrs>(self, url: A, cluster_url: B, seeds: Vec<T>) -> Result<()> {
    let cluster_client = Arc::new(ClusterClient::new(&self.id));
    let cluster_runner = cluster_client.clone();
    let cluster_handler = cluster_client.clone();

    let (tx, rx) = channel(CHANNEL_CAPACITY);
    let cloned_tx = tx.clone();

    tokio::spawn(async move {
      cluster_runner.run(cluster_url).await.unwrap();
    });

    tokio::spawn(async move {
      cluster_handler.handle_message(cloned_tx).await.unwrap();
    });
    
    if seeds.len() > 0 {
      for seed in seeds {
        cluster_client.connect(seed).await?;
      }
    }

    println!("HerdMQ is ready!!");
    let listener = TcpListener::bind(url).await.unwrap();

    tokio::spawn(async move {
      self.listen(rx, cluster_client).await.unwrap();
    });

    loop {
      let (socket, _addr) = listener.accept().await.unwrap();
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
P: Fn(&ClientCredential, &str) -> bool + Sync + Send + 'static,
{
  broker_id: Arc<String>,
  authenticator: Arc<Authenticator<C, S, P>>,
  sessions: Arc<Mutex<HashMap<String, Session>>>,
  subscription_table: Arc<Mutex<HashMap<String, HashMap<String, u8>>>>,
  storage: Arc<Storage>,
  cluster_client: Arc<ClusterClient>
}

impl <C, S, P> BrokerHandler<C, S, P>
where
C: Fn(&ClientCredential) -> bool + Sync + Send + 'static, 
S: Fn(&ClientCredential, &str) -> bool + Sync + Send + 'static,
P: Fn(&ClientCredential, &str) -> bool + Sync + Send + 'static,
{
  async fn insert_client_subscription(&self, topic: &str, client_id: &str, qos: u8) -> bool {
    let mut table = self.subscription_table.lock().await;
    let map = table.entry(topic.to_owned()).or_insert(HashMap::new());
    let new_topic = map.is_empty();
    map.insert(client_id.to_owned(), qos);
    new_topic
  }
  async fn remove_client_subscription(&self, topic: &str, client_id: &str) -> bool {
    let mut table = self.subscription_table.lock().await;
    if let Some(map) = table.get_mut(topic) {
      map.remove(client_id);
      map.is_empty()
    } else {
      true
    }
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
          topic: packet.topic.to_owned(),
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

  async fn handle_new_connection(&self, client_id: &str, packet: &ConnectPacket, client_credential: &ClientCredential) -> Result<()> {
    if !(self.authenticator.connect)(&client_credential) {
      return Err(HerdMQError::NotAuthorized);
    }
    println!("New client connection: {}", client_id);

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
      for (topic, qos) in &subscriptions {
        self.insert_client_subscription(topic, client_id, *qos).await;
        if self.cluster_client.insert_broker_subscription(topic, &self.broker_id).await {
          self.cluster_client.forward_subscription(topic).await?;
        }
      }

      self.send_to_client(&client_id, DecodedPacket::Connack(ConnackPacket {
        session_present: subscriptions.len() > 0,
        reason_code: ReasonCode::Success,
        properties: properties
      })).await?;

      let packets = self.storage.get_packets_for_client(&client_id).await?; 
      
      for (packet_id, topic, message) in &packets {
        self.send_to_client(&client_id, DecodedPacket::Publish(PublishPacket {
          topic: topic.to_owned(),
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
      if (self.authenticator.subscribe)(&client_credential, &subscription.topic) {
        subscriptions.push(subscription.clone());

        self.insert_client_subscription(&subscription.topic, client_id, subscription.qos).await;
        if self.cluster_client.insert_broker_subscription(&subscription.topic, &self.broker_id).await {
          self.cluster_client.forward_subscription(&subscription.topic).await?;
        }

        let reason_code = match subscription.qos {
          0 => ReasonCode::Success,
          _ => ReasonCode::GrantedQoS1,
        };
        reason_codes.push(reason_code);

        // get the retain messages
        let retain_messages = self.storage.get_retain_message(&subscription.topic).await?;
        for (topic, retain_message) in &retain_messages {
          self.send_to_client(&client_id, DecodedPacket::Publish(PublishPacket {
            topic: topic.to_owned(),
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

    if !(self.authenticator.publish)(&client_credential, &packet.topic) {
      return Err(HerdMQError::NotAuthorized);
    }

    // store retain packet
    if packet.config.retain {
      if packet.payload == "" {
        self.storage.remove_retain_message(&packet.topic).await?;
      } else {
        self.storage.store_retain_message(&packet.topic, &packet.payload).await?;
      }
    }

    // handle qos = 1
    if packet.config.qos > 0 {
      let packet_id = packet.packet_id.ok_or_else(|| Error::FormatError)?;

      // store offline packets
      let subscriptions = self.storage.find_subscriptions(&packet.topic).await?;

      for (cid, subscription_config) in subscriptions.iter() {
        if subscription_config.qos > 0 {
          self.storage.store_packet_for_client(&cid, &packet.topic, &packet.payload, packet_id).await?;
        }
      }

      // send puback
      self.send_to_client(&client_id, DecodedPacket::Puback(PubackPacket {
        packet_id: packet_id,
        reason_code: ReasonCode::Success,
        properties: vec![]
      })).await?;
    }

    let subscriptions = self.cluster_client.trie.lock().await.get(&packet.topic);

    // Don't publish to the same broker twice
    let mut published_broker = HashSet::new();
    for (topic, broker_ids) in subscriptions {
      for broker_id in broker_ids {
        if broker_id == *self.broker_id {
          self.forward_packet_to_clients(&packet, &topic).await?;
        } else {
          if !published_broker.contains(&broker_id) {
            published_broker.insert(broker_id.to_owned());
            self.cluster_client.forward_packet(&packet, &broker_id).await?;
          }
        }
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

        let result = self.handle_new_connection(&client_id, &packet, &client_credential).await;

        if let Err(_) = result {
          self.send_to_client(&client_id, DecodedPacket::Connack(ConnackPacket {
            session_present: false,
            reason_code: ReasonCode::NotAuthorized,
            properties: vec![]
          })).await?;
          self.sessions.lock().await.remove(&client_id);
        }
      }
      BrokerMessage::Publish(packet) => {
        let result = self.handle_publish(&client_id, &packet).await;

        if let Err(_) = result {
          if packet.config.qos > 0 {
            let packet_id = packet.packet_id.ok_or_else(|| Error::FormatError)?;
            self.send_to_client(&client_id, DecodedPacket::Puback(PubackPacket {
              packet_id: packet_id,
              reason_code: ReasonCode::NotAuthorized,
              properties: vec![]
            })).await?;
          }
        }
      }
      BrokerMessage::Puback(packet) => {
        self.storage.remove_packet_for_client(&client_id, packet.packet_id).await?;
      }    
      BrokerMessage::Subscribe(packet) => {
        self.handle_subscribe(&client_id, &packet).await?;
      }
      BrokerMessage::Unsubscribe(packet) => {
        self.storage.remove_subscriptions(&client_id, &packet.topics).await?;

        for topic in &packet.topics {
          if self.remove_client_subscription(topic, &client_id).await {
            self.cluster_client.remove_broker_subscription(topic, &self.broker_id).await?;
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
        for (topic, _) in &subscriptions {
          if self.remove_client_subscription(topic, &client_id).await {
            self.cluster_client.remove_broker_subscription(topic, &self.broker_id).await?;
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
            topic: will_config.topic,
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
      BrokerMessage::ClusterForward(packet) => {
        let subscriptions = self.cluster_client.trie.lock().await.get(&packet.topic);

        for (topic, broker_ids) in subscriptions {
          for broker_id in broker_ids {
            if broker_id == *self.broker_id {
              self.forward_packet_to_clients(&packet, &topic).await?;
            }
          }
        }
      }
    }

    Ok(())
  }
}

#[derive(Debug)]
pub enum BrokerMessage {
  // connection messages
  NewConnection(Sender<DecodedPacket>, ConnectPacket),
  Publish(PublishPacket),
  Puback(PubackPacket),
  Subscribe(SubscribePacket),
  Unsubscribe(UnsubscribePacket),
  CloseConnection,

  // cluster messages
  ClusterForward(PublishPacket)
}