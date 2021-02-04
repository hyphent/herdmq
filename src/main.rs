mod broker;
mod error;
mod types;
mod connection;
mod storage;
mod trie;
mod authenticator;
mod cluster;

use std::env;
use tokio::runtime;
use getopts::Options;
use uuid::Uuid;

use crate::{
  broker::Broker,
  storage::Storage,
  authenticator::Authenticator,
  types::ClientCredential
};


const BROKER_PREFIX: &'static str = "$herdmq";

fn authenticate_connect(_client_credential: &ClientCredential) -> bool {
  true
}

fn authenticate_subscribe(_client_credential: &ClientCredential, _topic: &str) -> bool {
  true
}

fn authenticate_publish(_client_credential: &ClientCredential, _topic: &str) -> bool {
  true
}

fn print_usage(program: &str, opts: Options) {
  let brief = format!("Usage: {} [options]", program);
  print!("{}", opts.usage(&brief));
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
  let args: Vec<String> = std::env::args().collect();
  let program = args[0].clone();

  let mut opts = Options::new();

  opts.optopt("p", "port", "port for the herdmq instance", "port");
  opts.optopt("c", "cluster", "port for cluster client", "port");
  opts.optopt("s", "seeds", "seeds for discovering herdmq cluster", "url");
  opts.optflag("h", "help", "print this help menu");

  let matches = match opts.parse(&args[1..]) {
    Ok(m) => m,
    Err(f) => panic!(f.to_string())
  };

  if matches.opt_present("h") {
    print_usage(&program, opts);
    return Ok(());
  }

  let seeds = match matches.opt_str("s") {
    Some(seeds) => seeds.split(',').map(|seed| seed.to_owned()).collect(),
    None => vec![]
  };
  let port = match matches.opt_str("p") {
    Some(port) => port.parse::<u16>().unwrap(),
    None => 1883
  };
  let cluster_port = match matches.opt_str("c") {
    Some(port) => port.parse::<u16>().unwrap(),
    None => 3883
  };

  let storage_urls = env::var("STORAGE_URLS").unwrap_or("127.0.0.1:9042".to_owned());

  let rt = runtime::Runtime::new()?;

  rt.block_on(async {
    let broker_id = format!("{}_{}", BROKER_PREFIX, Uuid::new_v4().to_string());
    let authenticator = Authenticator::new(authenticate_connect, authenticate_subscribe, authenticate_publish);
    let storage = Storage::new(storage_urls.split(',').collect()).await.unwrap();

    let broker = Broker::new(broker_id, authenticator, storage);

    broker.run(("0.0.0.0", port), ("0.0.0.0", cluster_port), seeds).await.unwrap();
  });

  Ok(())
}
