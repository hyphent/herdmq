use tokio::{
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

use futures_util::{
  sink::{Sink, SinkExt}, 
  stream::{Stream, StreamExt}
};

use mqtt_codec::{
  types::{DecodedPacket, PingRespPacket, ReasonCode, ConnackPacket},
  error::{EncodeError, DecodeError}
};

use crate::{
  types::{Result, Error},
  broker::BrokerMessage
};

// TODO change connection timeout back to 2 seconds
const CONNECTION_TIMEOUT: u64 = 15000;
const CHANNEL_CAPACITY: usize = 10;
const PING_CHECK_INTERVAL: u64 = 10000;
const MAX_PING_INTERVAL: u128 = (60000 as f64 * 1.5) as u128;

type DecodedPacketResult = std::result::Result<DecodedPacket, DecodeError>;

pub struct Connection<SI: Sink<DecodedPacket, Error=EncodeError>, ST: Stream<Item=DecodedPacketResult>> {
  client_id: String,
  sink: SI,
  stream: ST,
  broker_tx: Sender<(String, BrokerMessage)>,
  rx: Receiver<DecodedPacket>,
  tx: Sender<DecodedPacket>
}

impl <SI: Sink<DecodedPacket, Error=EncodeError> + Unpin, ST: Stream<Item=DecodedPacketResult> + Unpin>
Connection<SI, ST> {
  pub async fn handle_handshake(mut sink: SI, mut stream: ST, broker_tx: Sender<(String, BrokerMessage)>) -> Result<Self> {
    let first_packet = match time::timeout(Duration::from_millis(CONNECTION_TIMEOUT), stream.next()).await.map_err(|_| Error::TimeoutError)?.unwrap() {
      Ok(packet) => packet,
      Err(DecodeError::ProtocolNotSupportedError) => {
        sink.send(DecodedPacket::Connack(ConnackPacket {
          session_present: false,
          reason_code: ReasonCode::UnsupportedProtocolVersion,
          properties: vec![]
        })).await?; 
        return Err(DecodeError::ProtocolNotSupportedError.into());
      },
      Err(error) => return Err(error.into())
    };
  
    let connect_packet = match first_packet {
      DecodedPacket::Connect(packet) => packet,
      _ => return Err(Error::FormatError)
    };

    let (tx, mut rx) = channel(CHANNEL_CAPACITY);

    Self::send_to_server(&broker_tx, BrokerMessage::NewConnection(tx.clone(), connect_packet.clone()), &connect_packet.client_id).await?;

    if let Some(DecodedPacket::Connack(packet)) = rx.recv().await {
      let reason_code = packet.reason_code;
      sink.send(DecodedPacket::Connack(packet)).await?;
      if reason_code == ReasonCode::NotAuthorized {
        return Err(Error::NotAuthorized);
      }
    }

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
    broker_tx.send((client_id.to_owned(), message)).await.map_err(|_| Error::MPSCError)?;
    Ok(())
  }

  async fn reset_ping(last_ping: &Arc<Mutex<SystemTime>>) {
    let mut last_ping = last_ping.lock().await;
    *last_ping = SystemTime::now();
  }

  async fn handle_client_message(mut stream: ST, broker_tx: Sender<(String, BrokerMessage)>, tx: Sender<DecodedPacket>, client_id: String, last_ping: Arc<Mutex<SystemTime>>) -> Result<()> {
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
          tx.send(DecodedPacket::PingResp(PingRespPacket {})).await.map_err(|_| Error::MPSCError)?;
        }
        _ => ()
      }
    }
    Ok(())
  }

  async fn handle_broker_message(mut sink: SI, mut rx: Receiver<DecodedPacket>) -> Result<()> {
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