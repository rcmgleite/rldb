# rldb
[![Codecov](https://codecov.io/github/rcmgleite/rldb/coverage.svg?branch=master)](https://codecov.io/gh/rcmgleite/rldb)

## Intro
This repo contains an implementation of a dynamo-like key/value database.
It builds on top of the [dynamo db paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) + many specific implementation details from [riak](https://docs.riak.com/riak/kv/2.2.3/setup/planning/backend/bitcask/index.html#bitcask-implementation-details) and [redis](https://github.com/redis/redis).

Each module in this project contains it own README that will include as much context as possible about the implementation and tradeoffs made.

## Goals
The goal of this project is to have functional implementations for all major features of a dynamo-like database.
There are several interesting books about distributed systems that explain most of the concetps we are trying to implement here.
 - Designing Data-Intensive Applications - Martin Kleppmann
 - Database internals - Alex Petrov
 - DynamoDB paper

What these books/papers lack are concrete implementations of the algorithms explained. This repo tries to close this gap.

The set of features implemented are:

- [WIP] Key/value storage engine 
   - [Done] InMemory
   - [TODO] LSMTree
   - [TODO] Log structured hash table
- [Done] A server to receive incomding requests (currently TCP)
- [Done] PUT, GET and DELETE commands
- [Done] Partitioning scheme (currently only supports consistent hashing)
- [Done] Cluster and standalone modes
- [Done] For cluster mode:
   - [Done] node discovery
   - [Done] failure detection
   - [Done] partitioning ownership propagation
   - [TODO] reshuffling after cluster changes
   - [TODO] versioning and conflict detection (using vector clocks/version vectors)
- [TODO] leaderless replication for partitions
- [TODO] Read repair
- [TODO] Active anti-entropy

Finally, this repo will also touch on topics like
- Rust best practices
- Rust asynchronous programming
- Property based testing/randomized tests
- Linux tools that can help debug correctness and performance issue
  - Based on the book `Systems Performance` by Brendan Gregg
- maybe more??

Alongside this repo, a series of blog posts will be made and linked here for further information and discussion.

## Running the server

### Standalone
```
cargo run --bin rldb-server -- --config-path conf/standalone.json
```

### Cluster Mode


1. Start nodes using config files in different terminals
```
cargo run --bin rldb-server -- --config-path conf/cluster_node_1.json
```
```
cargo run --bin rldb-server -- --config-path conf/cluster_node_2.json
```
```
cargo run --bin rldb-server -- --config-path conf/cluster_node_3.json
```

2. Include the new nodes to the cluster

In this example, we assume node on port 4001 to be the initial cluster node and we add the other nodes to it.

```
cargo run --bin rldb-client join-cluster -p 4002 --known-cluster-node 127.0.0.1:4001
cargo run --bin rldb-client join-cluster -p 4003 --known-cluster-node 127.0.0.1:4001
```

### PUT
```
cargo run --bin rldb-client put -p 3001 -k foo -v bar
```

### GET
```
cargo run --bin rldb-client get -p 3001 -k foo
```

