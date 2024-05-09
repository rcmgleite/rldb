## Description

Every command availabe for rldb-server is included in this module.

As a first draft, commands know how to serialize and serialize themselves (so they are tightly coupled with the serialization format used on top of TCP). This decision will likely be changed in the future.

## Commands

### Ping (implemented)
Sends a TCP PING request and receives a PONG response

### Get(key: Bytes) (implemented)
Retrives the value associated with the given key (if it exists)

### Put(key: Bytes, value: Bytes) (implemented)
Stores the provided key/value pair

### Delete(key: Bytes) (TODO)
Deletes the value associated with the given key (if it exists)

### ReplicaOf(master_ip) (TODO)
Transforms the current node in a replica of the target node

### Sync (replication_offset) (TODO)
Used by ReplicaOf -> the replica node sends a Sync request to the target master
and from that point onwards, all mutations applied to the master are automatically propagated to the
replica node
