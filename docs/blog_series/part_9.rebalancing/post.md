# Part 9 - Rebalancing partitions after cluster changes

## Background

When new nodes are added to the cluster or existing nodes are removed from the cluster,
the consistent-hash ring changes. This change requires that data is rebalenced between nodes in the new cluster configuration.

This post will go over how this is implemented in *rldb*

## Requirements

- Clients need to be able to still access their data while the cluster is rebalancing partitions

## Possible solutions

- Gossip protocol is used to propagate state changes
- When a node is added or removed, this change is eventually going to get propagated to every node in the cluster
- When a node receives a new cluster state that adds/removes a node, it must be able to check if any keys that it owns should now be moved to some other node.
  - If that's the case, we will rely on a push model, where the node that owns data it shouldn't coodinates the synchronization with the newly established key owner.

## Questions
1. Why not make the new owner pull from the old owner? Is there really a difference?
  - it's hard for the new node to know if it needs to ask for keys from multiple nodes (ie: if multiple nodes were added and we had partial synchronizations already inflight)

## Design

- Node A receives a heartbeat including a new Node C
- Node A then merges the new cluster state with it's own and checks if there are any local keys that should be moved
- Node A kicks off a synchronization task with as many nodes as needed sending every key that is not part of its hash space
- Whenever a key is successfully moved, it's deleted from Node A to claim space back (tombstone, of course)
- When synchronization is done, stop the process and continue serving requests as usual

- For node C:
  - every requests for Node C hash space will fail at the beginning because all keys are actually on Node A
  - To avoid real customer failures, whenver a node is synchronizing from another Node, it has to keep state about which node it's synchronizing from. When a GET requests is issued, you try locally first. If NotFound, try the Node you are synchronizing from. 
    - Note: There's a race condition here in which you tryh locally, fail, receive the data from the other node and when you ask that data to the node it's already deleted. Think about how to do that (One possible solution is to always go for the remote node first.. but that can be expensive)

What about PUTs -> There could be more conflicts when rebalancing happens if clients send stale contexts or even no context at all. For now we will not care as the proper usage of the API is always to do a GET before a PUT.