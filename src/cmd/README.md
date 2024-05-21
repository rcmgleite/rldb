## Description

Every command availabe for rldb-server is included in this module.

As a first draft, commands know how to serialize and serialize themselves (so they are tightly coupled with the serialization format used on top of TCP). This decision will likely be changed in the future.

## Commands (client)

### Ping (implemented)
Sends a TCP PING request and receives a PONG response

```
cargo run --bin rldb-client ping -p 3001
```

### Get(key: Bytes) (implemented)
Retrives the value associated with the given key (if it exists)

```
cargo run --bin rldb-client get -p 3001 -k foo
```

### Put(key: Bytes, value: Bytes) (implemented)
Stores the provided key/value pair

```
cargo run --bin rldb-client put -p 301 -k foo -v bar
```

### Delete(key: Bytes) (TODO)
Deletes the value associated with the given key (if it exists)


## Commands (cluster)

### JoinCluster (implemented)
Adds a node to a running cluster.

```
cargo run --bin rldb-client join-cluster -p 4002 --known-cluster-node 127.0.0.1:4001
```

### Heartbeat (implemented)
There's no cli command implemented for this command as of now. This is used purely by cluster nodes
to propagate their view of the cluster.

### RemoveNode (TODO)