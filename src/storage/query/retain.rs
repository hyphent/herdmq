use std::collections::HashMap;
use cdrs_tokio::{
  types::{ IntoRustByName },
  query::*,
  query_values,
  error::Error
};

use crate::types::{Result, StorageSession};
use super::utils;

static CREATE_RETAIN_TABLE_QUERY: &'static str = r#"
  CREATE TABLE IF NOT EXISTS herdmq.retain (
    prefix text,
    topic_name text,
    message text,
    PRIMARY KEY(prefix, topic_name)
  );
"#;

pub async fn initialize(session: &StorageSession) -> Result<()> {
  session.query(CREATE_RETAIN_TABLE_QUERY).await.map(|_| (()))?;
  Ok(())
}

pub static SELECT_RETAIN_QUERY: &'static str = r#"
  SELECT * FROM herdmq.retain
  WHERE prefix = ? AND topic_name = ?
"#;

pub static SELECT_RETAIN_WILD_CARD_QUERY: &'static str = r#"
  SELECT * FROM herdmq.retain
  WHERE prefix = ? AND topic_name > ? AND topic_name < ?;
"#;

fn get_query_and_values(prefix: &str, topic_name: &str) -> (&'static str, QueryValues) {
  let default = (SELECT_RETAIN_QUERY, query_values!(prefix, topic_name));

  match topic_name.find("/#") {
    Some(x) if x == topic_name.len() - 2 => {
      let mut topic = topic_name.to_owned();
      topic.pop();
      let start = format!("{}", topic);
      topic.pop();
      let end = format!("{}0", topic);
      (SELECT_RETAIN_WILD_CARD_QUERY, query_values!(prefix, start, end))
    }
    _ => default
  }
}

pub async fn get_retain_message(topic_name: &str, session: &StorageSession) -> Result<HashMap<String, String>> {
  let prefix = utils::get_prefix(topic_name);
  let (query, values) = get_query_and_values(prefix, topic_name);
  let messages = session.query_with_values(query, values).await
    .and_then(|res| res.get_body())
    .and_then(|body| {
      body
        .into_rows()
        .ok_or(Error::from("cannot get rows from a response body"))
    })
    .and_then(|rows| {
      let mut messages = HashMap::new();
      for row in rows {
        let topic_name: String = row.get_by_name("topic_name").unwrap().unwrap();
        let message: String = row.get_by_name("message").unwrap().unwrap();
        messages.insert(topic_name, message);
      }
      Ok(messages)
    })?;
  
  Ok(messages)
}

pub static UPDATE_RETAIN_QUERY: &'static str = r#"
  UPDATE herdmq.retain
  SET message = ?
  WHERE prefix = ? AND topic_name = ?
"#;

pub async fn store_retain_message(topic_name: &str, message: &str, session: &StorageSession) -> Result<()> {
  let values = query_values!(message, utils::get_prefix(topic_name), topic_name);
  session.query_with_values(UPDATE_RETAIN_QUERY, values).await?;
  Ok(())
}

pub static REMOVE_RETAIN_QUERY: &'static str = r#"
  DELETE FROM herdmq.retain
  WHERE prefix = ? AND topic_name = ?
"#;

pub async fn remove_retain_message(topic_name: &str, session: &StorageSession) -> Result<()> {
  let values = query_values!(utils::get_prefix(topic_name), topic_name);
  session.query_with_values(REMOVE_RETAIN_QUERY, values).await?;
  Ok(())
}