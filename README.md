# RLDB A Rust implementation of the Amazon Dynamo Paper
[![Codecov](https://codecov.io/github/rcmgleite/rldb/coverage.svg?branch=master)](https://codecov.io/gh/rcmgleite/rldb)

## Introduction
RLDB (Rusty Learning Dynamo Database) is an educational project that provides a Rust implementation of the [Amazon dynamo paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf). This project aims to help developers and students understand the principles behind distributed key value data stores.

## Features
| Feature | Description | Status | Resources |
| --- | --- | --- | --- |
| InMemory Storage Engine | A simple in-memory storage engine | $${\textsf{\color{green}Implemented}}$$ | [Designing Data-Intensive applications - chapter 3](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) | 
| LSMTree | An LSMTree backed storage engine | $${\textsf{\color{yellow}TODO}}$$ | [Designing Data-Intensive applications - chapter 3](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) |
| Log Structured HashTable | Similar to the bitcask storage engine | $${\textsf{\color{yellow}TODO}}$$ | [Bitcask intro paper](https://riak.com/assets/bitcask-intro.pdf) |
| TCP server | A tokio backed TCP server for incoming requests | $${\textsf{\color{green}Implemented}}$$ | [tokio](https://github.com/tokio-rs/tokio) |
| PUT/GET/DEL client APIs | TCP APIs for PUT GET and DELET | $${\textsf{\color{greenyellow}WIP}}$$ | N/A |
| PartitioningScheme via Consistent Hashing | A functional consistent-hashing implementation | $${\textsf{\color{green}Implemented}}$$ | [Designing Data-Intensive applications - chapter 6](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/), [Consistent Hashing by David Karger](https://cs.brown.edu/courses/csci2950-u/papers/chash99www.pdf) |
| Leaderless replication of partitions | Replicating partition data using the leaderless replication approach | $${\textsf{\color{green}Implemented}}$$ | [Designing Data-Intensive applications - chapter 5](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) |
| Quorum | Quorum based reads and writes for tunnable consistenty guarantees | $${\textsf{\color{green}Implemented}}$$ | [Designing Data-Intensive applications - chapter 5](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) |
| Node discovery and failure detection| A gossip based mechanism to discover cluster nodes and detect failures | $${\textsf{\color{green}Implemented}}$$ | [Dynamo Paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
| re-sharding/rebalancing | Moving data between nodes after cluster state changes | $${\textsf{\color{yellow}TODO}}$$ | [Designing Data-Intensive applications - chapter 6](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) |
| Data versioning | Versioning and conflict detection / resolution (via VersionVectors) | $${\textsf{\color{green}Implemented}}$$ | [Vector clock wiki](https://en.wikipedia.org/wiki/Vector_clock), [Lamport clock paper](https://lamport.azurewebsites.net/pubs/time-clocks.pdf) (not that easy to parse)
| Reconciliation via Read repair | GETs can trigger repair in case of missing replicas | $${\textsf{\color{yellow}TODO}}$$ | [Dynamo Paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
| Active anti-entropy | Use merkle trees to detect missing replicas and trigger reconciliation | $${\textsf{\color{yellow}TODO}}$$ | [Dynamo Paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)

## Running the server


1. Start nodes using config files in different terminals
```
cargo run --bin rldb-server -- --config-path conf/node_1.json
```
```
cargo run --bin rldb-server -- --config-path conf/node_2.json
```
```
cargo run --bin rldb-server -- --config-path conf/node_3.json
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

{"message":"Ok"}% 
```

### GET
```
cargo run --bin rldb-client get -p 3001 -k foo

{"values":[{"value":"bar","crc32c":179770161}],"context":"00000001527bd0d79bdb065196e93b951879b64300000000000000000000000000000001"}
```

## Handling conflicts

### Example of a GET request encountering conflicts

```
cargo run --bin rldb-client get -p 3001 -k foo2

{"values":[{"value":"bar2","crc32c":1093081014},{"value":"bar1","crc32c":1383588930},{"value":"bar3","crc32c":3008140469}],"context":"00000003296f248aff807cf05f4bcd0d05a45cc500000000000000000000000000000001527bd0d79bdb065196e93b951879b64300000000000000000000000000000001e975274170197c04e92166baff4d20c900000000000000000000000000000001"}
```

The `context` key of the response is what allows subsequent PUTs to resolve the given conflicts. For example:

```
cargo run --bin rldb-client put -p 3002 -k foo2 -v conflicts_resolved -c 00000003296f248aff807cf05f4bcd0d05a45cc500000000000000000000000000000001527bd0d79bdb065196e93b951879b64300000000000000000000000000000001e975274170197c04e92166baff4d20c900000000000000000000000000000001

{"message":"Ok"}% 

cargo run --bin rldb-client get -p 3001 -k foo2 
{"values":[{"value":"conflicts_resolved","crc32c":3289643150}],"context":"00000003296f248aff807cf05f4bcd0d05a45cc500000000000000000000000000000001527bd0d79bdb065196e93b951879b64300000000000000000000000000000002e975274170197c04e92166baff4d20c900000000000000000000000000000001"}%
```

## Extracting traces with jaeger

The all-in-one jeager docker image can be used to export rldb traces locally. The steps to get there are:

1. Start the jaeger container 

```
$ ./local_jaeger.sh
```

2. Start nodes with the jaeger flag

```
cargo run --bin rldb-server -- --config-path conf/node_1.json --tracing-jaeger
```

3. Use the jaeger UI at: http://localhost:16686

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
