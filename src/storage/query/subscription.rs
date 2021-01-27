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
    topic_name text,
    client_id text,
    retain boolean,
    rap boolean,
    nl boolean,
    qos int,
    PRIMARY KEY(topic_name, client_id)
  );
"#;

static CREATE_SUBSCRIPTION_BY_CLIENT_ID_TABLE_QUERY: &'static str = r#"
  CREATE TABLE IF NOT EXISTS herdmq.subscription_client (
    topic_name text,
    client_id text,
    qos int,
    PRIMARY KEY(client_id, topic_name)
  );
"#;

pub async fn initialize(session: &StorageSession) -> Result<()> {
  session.query(CREATE_SUBSCRIPTION_BY_TOPIC_TABLE_QUERY).await.map(|_| (()))?;
  session.query(CREATE_SUBSCRIPTION_BY_CLIENT_ID_TABLE_QUERY).await.map(|_| (()))?;
  Ok(())
}

fn get_topic_alias(topic_name: &str) -> Vec<String> {
  let mut topic_alias = Vec::new();
  let mut topic_buffer = "".to_owned();
  for subtopic in topic_name.split('/') {
    topic_buffer = format!("{}{}/", topic_buffer, subtopic);
    topic_alias.push(match topic_buffer == format!("{}/", topic_name) {
      true => topic_name.to_owned(),
      false => format!("{}#", topic_buffer)
    });
  }
  topic_alias
}

fn row_to_subscription(row: Row, topic_name: &str) -> SubscriptionConfig {
  let retain = row.get_by_name("retain").unwrap().unwrap();
  let rap = row.get_by_name("rap").unwrap().unwrap();
  let nl = row.get_by_name("nl").unwrap().unwrap();
  let qos: i32 = row.get_by_name("qos").unwrap().unwrap();

  SubscriptionConfig {
    topic_name: topic_name.to_owned(),
    retain: retain,
    rap: rap,
    nl: nl,
    qos: qos as u8
  }
}

static SELECT_SUBSCRIPTIONS_QUERY: &'static str = r#"
  SELECT * FROM herdmq.subscription_topic
  WHERE topic_name IN ?
"#;

pub async fn find_subscriptions(topic_name: &str, session: &StorageSession) -> Result<Vec<(String, SubscriptionConfig)>> {
  let topic_alias = get_topic_alias(topic_name);
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
        let subscription_config = row_to_subscription(row, topic_name);
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
        let topic_name = row.get_by_name("topic_name").unwrap().unwrap();
        let qos: i32 = row.get_by_name("qos").unwrap().unwrap();
        subscriptions.push((topic_name, qos as u8));
      }

      Ok(subscriptions)
    })?;
  Ok(subscriptions)
}

static UPDATE_SUBSCRIPTION_QUERY: &'static str = r#"
  UPDATE herdmq.subscription_topic
  SET retain = ?,
    rap = ?,
    nl = ?,
    qos = ?
  WHERE client_id = ? AND topic_name = ?
"#;

static UPDATE_CLIENT_SUBSCRIPTION_QUERY: &'static str = r#"
  INSERT INTO herdmq.subscription_client (client_id, topic_name, qos) VALUES (?, ?, ?);
"#;

pub async fn store_subscriptions(client_id: &str, subscriptions: &Vec<SubscriptionConfig>, session: &StorageSession) -> Result<()> {
  for subscription in subscriptions {
    let values = query_values!(subscription.retain, subscription.rap, subscription.nl, subscription.qos as i32, client_id, subscription.topic_name.to_owned());
    session.query_with_values(UPDATE_SUBSCRIPTION_QUERY, values).await?;
    
    let values_for_client = query_values!(client_id, subscription.topic_name.to_owned(), subscription.qos as i32);
    session.query_with_values(UPDATE_CLIENT_SUBSCRIPTION_QUERY, values_for_client).await?;
  }
  Ok(())
}

static REMOVE_SUBSCRIPTION_QUERY: &'static str = r#"
  DELETE FROM herdmq.subscription_topic
  WHERE client_id = ? AND topic_name = ?
"#;

static REMOVE_CLIENT_SUBSCRIPTION_QUERY: &'static str = r#"
  DELETE FROM herdmq.subscription_client
  where client_id = ? AND topic_name = ?
"#;

pub async fn remove_subscriptions(client_id: &str, topic_names: &Vec<String>, session: &StorageSession) -> Result<()> {
  for topic_name in topic_names {
    let values = query_values!(client_id, topic_name.to_owned());
    session.query_with_values(REMOVE_SUBSCRIPTION_QUERY, values.clone()).await?;
    session.query_with_values(REMOVE_CLIENT_SUBSCRIPTION_QUERY, values).await?;
  }
  Ok(())
}

static REMOVE_ALL_SUBSCRIPTIONS_QUERY: &'static str = r#"
  DELETE FROM herdmq.subscription_topic
  WHERE topic_name IN ?
"#;

static REMOVE_ALL_CLIENT_SUBSCRIPTIONS_QUERY: &'static str = r#"
  DELETE FROM herdmq.subscription_client
  WHERE client_id = ?
"#;

pub async fn remove_all_subscriptions(client_id: &str, session: &StorageSession) -> Result<()> {
  let values_for_client = query_values!(client_id);
  let topic_names = session
    .query_with_values(SELECT_CLIENT_SUBSCRIPTION_QUERY, values_for_client.clone()).await
    .and_then(|res| res.get_body())
    .and_then(|body| {
      body
        .into_rows()
        .ok_or(Error::from("cannot get rows from a response body"))
    })
    .and_then(|rows| {
      let mut topic_names = Vec::new();
      for row in rows {
        let topic_name: String = row.get_by_name("topic_name").unwrap().unwrap();
        topic_names.push(topic_name);
      }
      Ok(topic_names)
    })?;

  let values = query_values!(topic_names);
  session.query_with_values(REMOVE_ALL_SUBSCRIPTIONS_QUERY, values).await?;
  session.query_with_values(REMOVE_ALL_CLIENT_SUBSCRIPTIONS_QUERY, values_for_client).await?;
  Ok(())
}