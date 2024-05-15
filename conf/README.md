## Server modes

rldb currently supports 2 types of server setup: Standalone and Cluster.

Currently every configuration is applied at runtime (as opposed to compile time). This might change in the future.

### Standalone
This is the simplest setup possible with rldb. There's a single rldb-server node running and no replication/partitioning is enabled.

### Cluster
In cluster mode, rldb can have up to 1000 nodes running and communicating to each other using the gossip bus.
In this mode, the config file allows the user to decide:
 - which partition scheme to use (currently only supports consistent hashing)
 - R, W and N where R = Read quorum, W = Write quorum and N = number of replicas
   - this is how a user can tweak consistency guarantees of the database