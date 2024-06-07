# RLDB A Rust implementation of the Amazon Dynamo Paper
[![Codecov](https://codecov.io/github/rcmgleite/rldb/coverage.svg?branch=master)](https://codecov.io/gh/rcmgleite/rldb)

## Introduction
RLDB (Rusty Learning Dynamo Database) is an educational project that provides a Rust implementation of the [Amazon dynamo paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf). This project aims to help developers and students understand the principles behind distributed key value data stores.

## Features
| Feature | Description | Status | Resources |
| --- | --- | --- | --- |
| InMemory Storage Engine | A simple in-memory storage engine | <code style="color : green"> Implemented </code> | [Designing Data-Intensive applications - chapter 3](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) | 
| LSMTree | An LSMTree backed storage engine | <code style="color : yellow"> TODO </code> | [Designing Data-Intensive applications - chapter 3](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) |
| Log Structured HashTable | Similar to the bitcask storage engine | <code style="color : yellow"> TODO </code> | [Bitcask intro paper](https://riak.com/assets/bitcask-intro.pdf) |
| TCP server | A tokio backed TCP server for incoming requests | <code style="color : green"> Implemented </code> | [tokio](https://github.com/tokio-rs/tokio) |
| PUT/GET/DEL client APIs | TCP APIs for PUT GET and DELET | <code style="color : greenyellow"> WIP </code> | N/A |
| PartitioningScheme via Consistent Hashing | A functional consistent-hashing implementation | <code style="color : green"> Implemented </code> | [Designing Data-Intensive applications - chapter 6](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/), [Consistent Hashing by David Karger](https://cs.brown.edu/courses/csci2950-u/papers/chash99www.pdf) |
| Leaderless replication of partitions | Replicating partition data using the leaderless replication approach | <code style="color : green"> Implemented </code> | [Designing Data-Intensive applications - chapter 5](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) |
| Quorum | Quorum based reads and writes for tunnable consistenty guarantees | <code style="color : green"> Implemented </code> | [Designing Data-Intensive applications - chapter 5](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) |
| Node discovery and failure detection| A gossip based mechanism to discover cluster nodes and detect failures | <code style="color : green"> Implemented </code> | [Dynamo Paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
| re-sharding/rebalancing | Moving data between nodes after cluster state changes | <code style="color : yellow"> TODO </code> | [Designing Data-Intensive applications - chapter 6](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) |
| Data versioning | Versioning and conflict detection / resolution (via VersionVectors) | <code style="color : greenyellow"> WIP </code> | [Vector clock wiki](https://en.wikipedia.org/wiki/Vector_clock), [Lamport clock paper](https://lamport.azurewebsites.net/pubs/time-clocks.pdf) (not that easy to parse)
| Reconciliation via Read repair | GETs can trigger repair in case of missing replicas | <code style="color : yellow"> TODO </code> | [Dynamo Paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
| Active anti-entropy | Use merkle trees to detect missing replicas and trigger reconciliation | <code style="color : yellow"> TODO </code> | [Dynamo Paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)

## Running the server

### Standalone Mode
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

In this example, we assume node on port 3001 to be the initial cluster node and we add the other nodes to it.

```
cargo run --bin rldb-client join-cluster -p 3002 --known-cluster-node 127.0.0.1:3001
cargo run --bin rldb-client join-cluster -p 3003 --known-cluster-node 127.0.0.1:3001
```

### PUT
```
cargo run --bin rldb-client put -p 3001 -k foo -v bar
```

### GET
```
cargo run --bin rldb-client get -p 3001 -k foo
```

## Documentation
See [rldb docs](https://docs.rs/rldb/latest/rldb/)

## License
This project is licensed under the MIT license.
See [License](https://github.com/rcmgleite/rldb/blob/master/LICENSE) for details

## Acknowledgments

This project was inspired by the original [Dynamo paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) but also by many other authors and resources like:

- [Designing Data-Intensive Applications](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/)
- [Database Internals](https://www.databass.dev/)

and many others. When modules in this project are based on specific resources, they will be included as part of the module documentation