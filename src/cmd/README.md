## Description

Every command availabe for rldb-server is included in this module.

As a first draft, commands know how to serialize and serialize themselves (so they are tightly coupled with the serialization format used on top of TCP). This decision will likely be changed in the future.

## Commands

### Ping

### Get(key: Bytes)

### Put(key: Bytes, value: Bytes)

### Delete(key: Bytes)