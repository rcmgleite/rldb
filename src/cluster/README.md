# Cluster

The cluster module contains 2 interconnected implementations of rldb (when configured in cluster mode)

1. Partitioning - implemented via consistent hashing
  - we will call the partitioning space "ring state"
2. Ring state(node discovery and ring assignment and failure detection) implemented via gossip protocol

See each specific file for detailed information.