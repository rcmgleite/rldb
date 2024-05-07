# rldb

## Intro
This repo contains an implementation of a dynamo-like key/value database.
It builds on top of the [dynamo db paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) + many specific implementation details from [riak](https://docs.riak.com/riak/kv/2.2.3/setup/planning/backend/bitcask/index.html#bitcask-implementation-details) and [redis](https://github.com/redis/redis).

Each module in this project contains it own README that will include as much context as possible about the implementation and tradeoffs made.

## Goals
The goal of this project is to have simple implementations for all major features of a dynamo-like database as a way to deep dive on distributed system challenges.
