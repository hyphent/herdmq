use std::collections::HashMap;
use cdrs_tokio::{
  types::{ IntoRustByName },
  query::*,
  query_values,
  error::Error
};

use crate::types::{Result, StorageSession};

static CREATE_PACKET_TABLE_QUERY: &'static str = r#"
  CREATE TABLE IF NOT EXISTS herdmq.packet (
    client_id text,
    packet_id int,
    topic_name text,
    message text,
    PRIMARY KEY(client_id, packet_id)
  );
"#;

pub async fn initialize(session: &StorageSession) -> Result<()> {
  session.query(CREATE_PACKET_TABLE_QUERY).await.map(|_| (()))?;
  Ok(())
}

pub static SELECT_PACKET_QUERY: &'static str = r#"
  SELECT * FROM herdmq.packet
  WHERE client_id = ?
"#;

pub async fn get_packets_for_client(client_id: &str, session: &StorageSession) -> Result<HashMap<u16, (String, String)>> {
  let values = query_values!(client_id);
  let packets = session
    .query_with_values(SELECT_PACKET_QUERY, values).await
    .and_then(|res| res.get_body())
    .and_then(|body| {
      body
        .into_rows()
        .ok_or(Error::from("cannot get rows from a response body"))
    })
    .and_then(|rows| {
      let mut packets = HashMap::new();
      for row in rows {
        let packet_id: i32 = row.get_by_name("packet_id").unwrap().unwrap();
        let topic_name: String = row.get_by_name("topic_name").unwrap().unwrap();
        let message: String = row.get_by_name("message").unwrap().unwrap();
        packets.insert(packet_id as u16, (topic_name, message));
      }
      Ok(packets)
    })?;
  Ok(packets)
}

pub static UPDATE_PACKET_QUERY: &'static str = r#"
  UPDATE herdmq.packet  
  SET topic_name = ?,
    message = ?
  WHERE client_id = ? AND packet_id = ?
"#;

pub async fn store_packet_for_client(client_id: &str, topic_name: &str, message: &str, packet_id: u16, session: &StorageSession) -> Result<()> {
  let values = query_values!(topic_name, message, client_id, packet_id as i32);
  session.query_with_values(UPDATE_PACKET_QUERY, values).await?;
  Ok(())
}

pub static REMOVE_PACKET_QUERY: &'static str = r#"
  DELETE FROM herdmq.packet
  WHERE client_id = ? and packet_id = ?
"#;

pub async fn remove_packet_for_client(client_id: &str, packet_id: u16, session: &StorageSession) -> Result<()> {
  let values = query_values!(client_id, packet_id as i32);
  session.query_with_values(REMOVE_PACKET_QUERY, values).await?;
  Ok(())
}

pub static REMOVE_ALL_PACKETS_QUERY: &'static str = r#"
  DELETE FROM herdmq.packet
  WHERE client_id = ?
"#;

pub async fn remove_all_packets_for_client(client_id: &str, session: &StorageSession) -> Result<()> {
  let values = query_values!(client_id);
  session.query_with_values(REMOVE_ALL_PACKETS_QUERY, values).await?;
  Ok(())
}