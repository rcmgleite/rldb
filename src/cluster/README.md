# Cluster

The cluster module contains 2 interconnected implementations of rldb (when configured in cluster mode)

1. Partitioning - currently only supports consistent hashing
2. Cluster state(node discovery, ring assignment and failure detection) implemented via TCP gossip protocol

More details can be found in each specific file/mod.
