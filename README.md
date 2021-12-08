# HerdMQ
A MQTTv5 broker written in Rust with Cassandra/ScyllaDB for message persistence.

# Getting Started

## Building
```
cargo build --release
```
## Prerequisite
Make sure you have a ScyllaDB or a Cassandra running (preferably on port 9042). To do this, you may use the provided docker-compose file:
```
docker-compose up -d
```
## Running
To start the MQTT Broker simply run,
```
./target/release/herdmq
```
## Help
To get more information on the available, such as specifying the port, options for the broker,
```
./target/release/herdmq -h
```


# Features
- MQTTv5 Protocol
- Message Persistence
- Last-Will Messages
- Retain Messages
- Clustered MQTT Brokers
- QOS 0 and 1
- Multi-level wildcards
- Websocket Connections