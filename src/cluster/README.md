# Cluster

The cluster module contains 2 interconnected implementations of rldb (when configured in cluster mode)

1. Partitioning - implemented via consistent hashing
2. Ring state(node discovery, ring assignment and failure detection) implemented via TCP gossip protocol

See each specific file for detailed information.