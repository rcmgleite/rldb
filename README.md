# rldb

## Intro
This repo contains an implementation of a dynamo-like key/value database.
It builds on top of the [dynamo db paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) + many specific implementation details from [riak](https://docs.riak.com/riak/kv/2.2.3/setup/planning/backend/bitcask/index.html#bitcask-implementation-details) and [redis](https://github.com/redis/redis).

Each module in this project contains it own README that will include as much context as possible about the implementation and tradeoffs made.

## Goals
The goal of this project is to have simple implementations for all major features of a dynamo-like database as a way to deep dive on distributed system challenges.

## Running the server

### Standalone
TODO

### Cluster Mode


1. Start nodes using config files in different terminals
```
RUST_BACKTRACE=1 RUST_LOG=debug cargo run --bin rldb-server -- --config-path conf/cluster_node_1.json
```
```
RUST_BACKTRACE=1 RUST_LOG=debug cargo run --bin rldb-server -- --config-path conf/cluster_node_2.json
```
```
RUST_BACKTRACE=1 RUST_LOG=debug cargo run --bin rldb-server -- --config-path conf/cluster_node_3.json
```

2. Include the new nodes to the cluster

In this example, we assume node on port 4001 to be the initial cluster node and we add the other nodes to it.

```
RUST_LOG=DEBUG cargo run --bin rldb-client join-cluster -p 4002 --known-cluster-node 127.0.0.1:4001
RUST_LOG=DEBUG cargo run --bin rldb-client join-cluster -p 4003 --known-cluster-node 127.0.0.1:4001
```