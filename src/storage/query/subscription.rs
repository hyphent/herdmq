use cdrs_tokio::{
  types::{
    rows::Row,
    IntoRustByName
  },
  query::*,
  query_values,
  error::Error
};
use mqtt_codec::types::SubscriptionConfig;
use crate::types::{Result, StorageSession};

static CREATE_SUBSCRIPTION_BY_TOPIC_TABLE_QUERY: &'static str = r#"
  CREATE TABLE IF NOT EXISTS herdmq.subscription_topic (
    topic text,
    client_id text,
    rap boolean,
    nl boolean,
    qos int,
    PRIMARY KEY(topic, client_id)
  );
"#;

static CREATE_SUBSCRIPTION_BY_CLIENT_ID_TABLE_QUERY: &'static str = r#"
  CREATE TABLE IF NOT EXISTS herdmq.subscription_client (
    topic text,
    client_id text,
    qos int,
    PRIMARY KEY(client_id, topic)
  );
"#;

pub async fn initialize(session: &StorageSession) -> Result<()> {
  session.query(CREATE_SUBSCRIPTION_BY_TOPIC_TABLE_QUERY).await.map(|_| (()))?;
  session.query(CREATE_SUBSCRIPTION_BY_CLIENT_ID_TABLE_QUERY).await.map(|_| (()))?;
  Ok(())
}

fn get_topic_alias(topic: &str) -> Vec<String> {
  let mut topic_alias = Vec::new();
  let mut topic_buffer = "".to_owned();
  for subtopic in topic.split('/') {
    topic_buffer = format!("{}{}/", topic_buffer, subtopic);
    topic_alias.push(match topic_buffer == format!("{}/", topic) {
      true => topic.to_owned(),
      false => format!("{}#", topic_buffer)
    });
  }
  topic_alias
}

fn row_to_subscription(row: Row, topic: &str) -> SubscriptionConfig {
  let rap = row.get_by_name("rap").unwrap().unwrap();
  let nl = row.get_by_name("nl").unwrap().unwrap();
  let qos: i32 = row.get_by_name("qos").unwrap().unwrap();

  SubscriptionConfig {
    topic: topic.to_owned(),
    retain_handling: 0,
    rap: rap,
    nl: nl,
    qos: qos as u8
  }
}

static SELECT_SUBSCRIPTIONS_QUERY: &'static str = r#"
  SELECT * FROM herdmq.subscription_topic
  WHERE topic IN ?
"#;

pub async fn find_subscriptions(topic: &str, session: &StorageSession) -> Result<Vec<(String, SubscriptionConfig)>> {
  let topic_alias = get_topic_alias(topic);
  let values = query_values!(topic_alias);
  let subscriptions = session.query_with_values(SELECT_SUBSCRIPTIONS_QUERY, values).await
    .and_then(|res| res.get_body())
    .and_then(|body| {
      body
        .into_rows()
        .ok_or(Error::from("cannot get rows from a response body"))
    })
    .and_then(|rows| {
      let mut subscriptions = Vec::with_capacity(rows.len());

      for row in rows {
        let client_id = row.get_by_name("client_id").unwrap().unwrap();
        let subscription_config = row_to_subscription(row, topic);
        subscriptions.push((client_id, subscription_config));
      }

      Ok(subscriptions)
    })?;
  
  Ok(subscriptions)
}

pub static SELECT_CLIENT_SUBSCRIPTION_QUERY: &'static str = r#"
  SELECT * FROM herdmq.subscription_client
  WHERE client_id = ?
"#;

pub async fn find_client_subscriptions(client_id: &str, session: &StorageSession) -> Result<Vec<(String, u8)>> {
  let values = query_values!(client_id);
  let subscriptions = session.query_with_values(SELECT_CLIENT_SUBSCRIPTION_QUERY, values).await
    .and_then(|res| res.get_body())
    .and_then(|body| {
      body
        .into_rows()
        .ok_or(Error::from("cannot get rows from a response body"))
    })
    .and_then(|rows| {
      let mut subscriptions = Vec::with_capacity(rows.len());
      
      for row in rows {
        let topic = row.get_by_name("topic").unwrap().unwrap();
        let qos: i32 = row.get_by_name("qos").unwrap().unwrap();
        subscriptions.push((topic, qos as u8));
      }

      Ok(subscriptions)
    })?;
  Ok(subscriptions)
}

static UPDATE_SUBSCRIPTION_QUERY: &'static str = r#"
  UPDATE herdmq.subscription_topic
  SET rap = ?,
    nl = ?,
    qos = ?
  WHERE client_id = ? AND topic = ?
"#;

static UPDATE_CLIENT_SUBSCRIPTION_QUERY: &'static str = r#"
  INSERT INTO herdmq.subscription_client (client_id, topic, qos) VALUES (?, ?, ?);
"#;

pub async fn store_subscriptions(client_id: &str, subscriptions: &Vec<SubscriptionConfig>, session: &StorageSession) -> Result<()> {
  for subscription in subscriptions {
    let values = query_values!(subscription.rap, subscription.nl, subscription.qos as i32, client_id, subscription.topic.to_owned());
    session.query_with_values(UPDATE_SUBSCRIPTION_QUERY, values).await?;
    
    let values_for_client = query_values!(client_id, subscription.topic.to_owned(), subscription.qos as i32);
    session.query_with_values(UPDATE_CLIENT_SUBSCRIPTION_QUERY, values_for_client).await?;
  }
  Ok(())
}

static REMOVE_SUBSCRIPTION_QUERY: &'static str = r#"
  DELETE FROM herdmq.subscription_topic
  WHERE client_id = ? AND topic = ?
"#;

static REMOVE_CLIENT_SUBSCRIPTION_QUERY: &'static str = r#"
  DELETE FROM herdmq.subscription_client
  where client_id = ? AND topic = ?
"#;

pub async fn remove_subscriptions(client_id: &str, topics: &Vec<String>, session: &StorageSession) -> Result<()> {
  for topic in topics {
    let values = query_values!(client_id, topic.to_owned());
    session.query_with_values(REMOVE_SUBSCRIPTION_QUERY, values.clone()).await?;
    session.query_with_values(REMOVE_CLIENT_SUBSCRIPTION_QUERY, values).await?;
  }
  Ok(())
}

static REMOVE_ALL_SUBSCRIPTIONS_QUERY: &'static str = r#"
  DELETE FROM herdmq.subscription_topic
  WHERE topic IN ?
"#;

static REMOVE_ALL_CLIENT_SUBSCRIPTIONS_QUERY: &'static str = r#"
  DELETE FROM herdmq.subscription_client
  WHERE client_id = ?
"#;

pub async fn remove_all_subscriptions(client_id: &str, session: &StorageSession) -> Result<()> {
  let values_for_client = query_values!(client_id);
  let topics = session
    .query_with_values(SELECT_CLIENT_SUBSCRIPTION_QUERY, values_for_client.clone()).await
    .and_then(|res| res.get_body())
    .and_then(|body| {
      body
        .into_rows()
        .ok_or(Error::from("cannot get rows from a response body"))
    })
    .and_then(|rows| {
      let mut topics = Vec::new();
      for row in rows {
        let topic: String = row.get_by_name("topic").unwrap().unwrap();
        topics.push(topic);
      }
      Ok(topics)
    })?;

  let values = query_values!(topics);
  session.query_with_values(REMOVE_ALL_SUBSCRIPTIONS_QUERY, values).await?;
  session.query_with_values(REMOVE_ALL_CLIENT_SUBSCRIPTIONS_QUERY, values_for_client).await?;
  Ok(())
}