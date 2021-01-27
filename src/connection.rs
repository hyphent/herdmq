use tokio_util::codec::Framed;
use tokio::{
  net::TcpStream,
  sync::{
    Mutex, 
    mpsc::{Sender, Receiver, channel}
  },
  time::{self, Duration}
};

use std::{
  sync::Arc,
  time::SystemTime
};

use futures_util::{sink::SinkExt, stream::{StreamExt, SplitStream, SplitSink}};

use mqtt_codec::{
  codec::MQTTCodec,
  types::{DecodedPacket, PingRespPacket, ReasonCode}
};

use crate::{
  error::HerdMQError,
  types::Result,
  broker::BrokerMessage
};

const CONNECTION_TIMEOUT: u64 = 2000;
const CHANNEL_CAPACITY: usize = 10;
const PING_CHECK_INTERVAL: u64 = 10000;
const MAX_PING_INTERVAL: u128 = (60000 as f64 * 1.5) as u128;

pub struct Connection {
  client_id: String,
  sink: SplitSink<Framed<TcpStream, MQTTCodec>, DecodedPacket>,
  stream: SplitStream<Framed<TcpStream, MQTTCodec>>,
  broker_tx: Sender<(String, BrokerMessage)>,
  rx: Receiver<DecodedPacket>,
  tx: Sender<DecodedPacket>
}

impl Connection {
  pub async fn handle_handshake(socket: TcpStream, broker_tx: Sender<(String, BrokerMessage)>) -> Result<Self> {
    let mut frame = Framed::new(socket, MQTTCodec {});
    let first_packet = time::timeout(Duration::from_millis(CONNECTION_TIMEOUT), frame.next())
      .await.map_err(|_| HerdMQError::TimeoutError)?.unwrap()?;
  
    let connect_packet = match first_packet {
      DecodedPacket::Connect(packet) => packet,
      _ => return Err(HerdMQError::FormatError)
    };

    let (tx, mut rx) = channel(CHANNEL_CAPACITY);

    Self::send_to_server(&broker_tx, BrokerMessage::NewConnection(tx.clone(), connect_packet.clone()), &connect_packet.client_id).await?;

    if let Some(DecodedPacket::Connack(packet)) = rx.recv().await {
      let reason_code = packet.reason_code;
      frame.send(DecodedPacket::Connack(packet)).await?;
      if reason_code == ReasonCode::NotAuthorized {
        return Err(HerdMQError::NotAuthorized);
      }
    }

    let (sink, stream) = frame.split();
    Ok(Connection {
      client_id: connect_packet.client_id,
      sink,
      stream,
      broker_tx,
      rx,
      tx
    })
  }

  async fn send_to_server(broker_tx: &Sender<(String, BrokerMessage)>, message: BrokerMessage, client_id: &str) -> Result<()> {
    broker_tx.send((client_id.to_owned(), message)).await.map_err(|_| HerdMQError::MPSCError)?;
    Ok(())
  }

  async fn reset_ping(last_ping: &Arc<Mutex<SystemTime>>) {
    let mut last_ping = last_ping.lock().await;
    *last_ping = SystemTime::now();
  }

  async fn handle_client_message(mut stream: SplitStream<Framed<TcpStream, MQTTCodec>>, broker_tx: Sender<(String, BrokerMessage)>, 
    tx: Sender<DecodedPacket>, client_id: String, last_ping: Arc<Mutex<SystemTime>>) -> Result<()> {
    while let Some(Ok(decoded_packet)) = stream.next().await {
      Self::reset_ping(&last_ping).await;

      match decoded_packet {
        DecodedPacket::Publish(packet) => {
          Self::send_to_server(&broker_tx, BrokerMessage::Publish(packet.clone()), &client_id).await?;
        }
        DecodedPacket::Puback(packet) => {
          Self::send_to_server(&broker_tx, BrokerMessage::Puback(packet.clone()), &client_id).await?;
        }
        DecodedPacket::Subscribe(packet) => {
          Self::send_to_server(&broker_tx, BrokerMessage::Subscribe(packet.clone()), &client_id).await?;
        }
        DecodedPacket::Unsubscribe(packet) => {
          Self::send_to_server(&broker_tx, BrokerMessage::Unsubscribe(packet.clone()), &client_id).await?;
        }
        DecodedPacket::PingReq(_) => {
          tx.send(DecodedPacket::PingResp(PingRespPacket {})).await.map_err(|_| HerdMQError::MPSCError)?;
        }
        _ => {}
      }
    }
    Ok(())
  }

  async fn handle_broker_message(mut sink: SplitSink<Framed<TcpStream, MQTTCodec>, DecodedPacket>, mut rx: Receiver<DecodedPacket>) -> Result<()> {
    while let Some(message) = rx.recv().await {
      sink.send(message).await?;
    }
    Ok(())
  }

  async fn check_idle_connection(last_ping: Arc<Mutex<SystemTime>>) {
    while match last_ping.lock().await.elapsed() {
      Ok(duration) => duration.as_millis() < MAX_PING_INTERVAL,
      Err(_) => false
    }
    {
      time::sleep(Duration::from_millis(PING_CHECK_INTERVAL)).await;
    }
  }

  pub async fn listen(self) -> Result<()> {
    let last_ping = Arc::new(Mutex::new(SystemTime::now()));
    tokio::select! {
      _ = Self::check_idle_connection(last_ping.clone()) => {},
      _ = Self::handle_client_message(self.stream, self.broker_tx.clone(), self.tx, self.client_id.to_owned(), last_ping) => {},
      _ = Self::handle_broker_message(self.sink, self.rx) => {}
    }
    Self::send_to_server(&self.broker_tx, BrokerMessage::CloseConnection, &self.client_id).await?;
    Ok(())
  }
}