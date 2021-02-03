use tokio_util::codec::Framed;
use std::{
  collections::{HashMap, HashSet},
  sync::Arc
};

use tokio::{
  net::{TcpListener, ToSocketAddrs, TcpStream},
  sync::{
    Mutex,
    mpsc::{channel, Sender, Receiver}
  },
  time::{self, Duration}
};

use futures_util::{sink::SinkExt, stream::{StreamExt, SplitStream, SplitSink}};

use serde_json::{Value, json};

use mqtt_codec::{
  types::*,
  codec::MQTTCodec
};

use crate::{
  trie::TopicTrie,
  types::{Result, Error},
  broker::BrokerMessage
};

const CHANNEL_CAPACITY: usize = 100;
const PING_INTERVAL: u16 = 15000;
const CONNECTION_TIMEOUT: u64 = 2000;

type ClusterMessage = (String, DecodedPacket);

pub struct ClusterClient {
  id: Arc<String>,
  rx: Arc<Mutex<Receiver<ClusterMessage>>>,
  tx: Arc<Sender<ClusterMessage>>,
  connections: Arc<Mutex<HashMap<String, Arc<ClusterConnection>>>>,
  pub routing_table: Arc<Mutex<HashMap<String, HashSet<String>>>>,
  pub trie: Arc<Mutex<TopicTrie>>
}

impl ClusterClient {
  pub fn new(id: &str) -> Self {
    let (tx, rx) = channel(CHANNEL_CAPACITY);
    
    ClusterClient {
      id: Arc::new(id.to_owned()),
      rx: Arc::new(Mutex::new(rx)),
      tx: Arc::new(tx),
      connections: Arc::new(Mutex::new(HashMap::new())),
      routing_table: Arc::new(Mutex::new(HashMap::new())),
      trie: Arc::new(Mutex::new(TopicTrie::new()))
    }
  }

  pub async fn insert_broker_subscription(&self, topic: &str, broker_id: &str) -> bool {
    let mut table = self.routing_table.lock().await;
    let set = table.entry(topic.to_owned()).or_insert(HashSet::new());
    if set.insert(broker_id.to_owned()) {
      self.trie.lock().await.insert(topic, broker_id);
      true
    } else {
      false
    }
  }

  pub async fn remove_broker_subscription(&self, topic: &str, broker_id: &str) -> Result<()> {
    let mut table = self.routing_table.lock().await;
    if let Some(set) = table.get_mut(topic) {
      if set.remove(broker_id) {
        self.trie.lock().await.remove(topic, broker_id);
        if broker_id == &*self.id {
          self.forward_unsubscribe(topic).await?;
        }
      }
    }
    Ok(())
  }

  pub async fn forward_packet(&self, packet: &PublishPacket, broker_id: &str) -> Result<()> {
    let connection = match self.connections.lock().await.get(broker_id) {
      Some(connection) => Some(connection.clone()),
      None => None
    };

    if let Some(connection) = connection {
      connection.send(DecodedPacket::Publish(packet.clone())).await?;
    }
    Ok(())
  }

  pub async fn forward_subscription(&self, topic: &str) -> Result<()> {
    let connections: Vec<Arc<ClusterConnection>> = self.connections.lock().await.iter().map(|(_, connection)| connection.clone()).collect();

    for connection in connections {
      connection.send(DecodedPacket::Subscribe(SubscribePacket {
        packet_id: 27,
        properties: vec![],
        subscriptions: vec![SubscriptionConfig {
          topic: topic.to_owned(),
          rap: false,
          retain_handling: 0,
          qos: 0,
          nl: false
        }]
      })).await?;
    }
    Ok(())
  }

  pub async fn forward_unsubscribe(&self, topic: &str) -> Result<()> {
    let connections: Vec<Arc<ClusterConnection>> = self.connections.lock().await.iter().map(|(_, connection)| connection.clone()).collect();

    for connection in connections {
      connection.send(DecodedPacket::Unsubscribe(UnsubscribePacket {
        packet_id: 32,
        properties: vec![],
        topics: vec![topic.to_owned()]
      })).await?;
    }
    Ok(())
  }

  pub async fn run<T: ToSocketAddrs>(&self, url: T) -> Result<()> {
    let listener = TcpListener::bind(url).await?;
    while let Ok((socket, _addr)) = listener.accept().await {
      let tx = self.tx.clone();
      let client_id = self.id.to_string();
      let connections = self.connections.clone();
      let routing_table = self.routing_table.clone();
      tokio::spawn(async move {
        let connection = Arc::new(ClusterConnection::handle_handshake(socket, &client_id, routing_table).await.unwrap());
        connections.lock().await.insert(
          connection.id.to_string(),
          connection.clone()
        );
        connection.listen(tx).await.unwrap();
      });
    }
    Ok(())
  }
  
  pub async fn connect<T: ToSocketAddrs>(&self, url: T) -> Result<()> {
    let socket = TcpStream::connect(url).await?;

    let tx = self.tx.clone();
    let client_id = self.id.to_string();
    let connections = self.connections.clone();
    let routing_table = self.routing_table.clone();
    let trie = self.trie.clone();
    tokio::spawn(async move {
      let connection = Arc::new(ClusterConnection::initiate_handshake(socket, &client_id, routing_table, trie).await.unwrap());
      connections.lock().await.insert(
        connection.id.to_string(),
        connection.clone()
      );
      connection.listen(tx).await.unwrap();
    });
    Ok(())
  }

  pub async fn handle_message(&self, broker_tx: Sender<(String, BrokerMessage)>) -> Result<()> {
    while let Some((client_id, packet)) = self.rx.lock().await.recv().await {
      match packet {
        DecodedPacket::Subscribe(packet) => {
          let topic = &packet.subscriptions[0].topic;
          self.insert_broker_subscription(topic, &client_id).await;
        }
        DecodedPacket::Publish(packet) => {
          broker_tx.send((client_id.to_owned(), BrokerMessage::ClusterForward(packet))).await.map_err(|_| Error::MPSCError)?;
        }
        DecodedPacket::Unsubscribe(packet) => {
          let topic = &packet.topics[0];
          self.remove_broker_subscription(topic, &client_id).await?;
        }
        DecodedPacket::Disconnect(_) => {
          let mut table = self.routing_table.lock().await;
          for (topic, set) in table.iter_mut() {
            if set.remove(&client_id) {
              self.trie.lock().await.remove(topic, &client_id);
            }
          }
        }
        _ => ()
      }
      
    }
    Ok(())
  }
}


struct ClusterConnection {
  pub id: Arc<String>,
  pub sink: Arc<Mutex<SplitSink<Framed<TcpStream, MQTTCodec>, DecodedPacket>>>,
  pub stream: Arc<Mutex<SplitStream<Framed<TcpStream, MQTTCodec>>>>,
}

impl ClusterConnection {
  async fn initiate_handshake(socket: TcpStream, client_id: &str, routing_table: Arc<Mutex<HashMap<String, HashSet<String>>>>, trie: Arc<Mutex<TopicTrie>>) -> Result<Self> {

    let mut stream = Framed::new(socket, MQTTCodec {});

    stream.send(DecodedPacket::Connect(ConnectPacket {
      client_id: client_id.to_owned(),
      clean_start: true,

      will_config: None,

      keep_alive: PING_INTERVAL, 

      username: Some("admin".to_owned()),
      password: Some("password".to_owned()),

      properties: vec![]
    })).await?;

    if let Some(Ok(DecodedPacket::Publish(packet))) = stream.next().await {
      let (sink, stream) = stream.split();

      // update routing table
      let value: Value = serde_json::from_str(&packet.payload).unwrap();
      if let Value::Object(map) = value {
        let mut table = routing_table.lock().await;
        let mut trie = trie.lock().await;
        trie.clear();
        table.clear();
        for (key, item) in map {
          if let Value::Array(vector) = item {
            let mut set = HashSet::new();
            for item in vector {
              if let Value::String(item) = item {
                trie.insert(&key, &item);
                set.insert(item);
              }
            }
            table.insert(key, set);
          }
        }
      }

      Ok(ClusterConnection {
        id: Arc::new(packet.topic.to_owned()),
        sink: Arc::new(Mutex::new(sink)),
        stream: Arc::new(Mutex::new(stream))
      })
    } else {
      Err(Error::NotAuthorized)
    }
  }

  async fn handle_handshake(socket: TcpStream, client_id: &str, routing_table: Arc<Mutex<HashMap<String, HashSet<String>>>>) -> Result<Self> {
    let mut stream = Framed::new(socket, MQTTCodec {});

    let first_packet = time::timeout(Duration::from_millis(CONNECTION_TIMEOUT), stream.next())
    .await.map_err(|_| Error::TimeoutError)?.unwrap()?;

    let connect_packet = match first_packet {
      DecodedPacket::Connect(packet) => packet,
      _ => return Err(Error::FormatError)
    };

    let routing_table = json!(routing_table.lock().await.clone());

    stream.send(DecodedPacket::Publish(PublishPacket {
      packet_id: None,
      topic: client_id.to_owned(),
      payload: routing_table.to_string(),
      config: PublishConfig {
        dup: false,
        qos: 0,
        retain: false
      },
      properties: vec![]
    })).await?;

    let (sink, stream) = stream.split();

    Ok(ClusterConnection {
      id: Arc::new(connect_packet.client_id.to_owned()),
      sink: Arc::new(Mutex::new(sink)),
      stream: Arc::new(Mutex::new(stream))
    })
  }

  async fn listen(&self, tx: Arc<Sender<ClusterMessage>>) -> Result<()> {
    while let Some(Ok(packet)) = self.stream.lock().await.next().await {
      tx.send((self.id.to_string(), packet)).await.map_err(|_| Error::MPSCError)?;
    }
    tx.send((self.id.to_string(), DecodedPacket::Disconnect(DisconnectPacket {
      reason_code: ReasonCode::Success,
      properties: vec![],
    }))).await.map_err(|_| Error::MPSCError)?;
    Ok(())
  }

  async fn send(&self, packet: DecodedPacket) -> Result<()> {
    self.sink.lock().await.send(packet).await?;
    Ok(())
  }
}

