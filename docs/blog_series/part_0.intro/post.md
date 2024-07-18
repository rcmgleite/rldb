# 1. Background 

Most developers have, at some point in time, interacted with storage systems. Databases like [redis](https://github.com/redis/redis) are almost guaranteed to be part of every tech stack active nowadays.

When using storage systems like this, understanding their nitty-gritty is really key to properly integrate them with your application and, most importantly, operate them correctly. One of the greatest resources on how storage systems work is the book [Designing Data-Intensive Applications](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) by Martin Kleppmann. This book is a comprehensive compilation of most of the algorithms and data structures that power modern storage system.

Now you may ask: why write a deep dive series if the book covers all the relevant topics already? Well, books like this are great but they lack concrete implementations. It's hard to tell if one actually understood all the concepts and how they are applied just by reading about them.

To close this gap between reading and building, I decided to write my own little storage system - [rldb](https://github.com/rcmgleite/rldb) - a dynamo-like key/value database - ie: A Key/value database that implements the [amazon's dynamo paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) from 2007.

This is the first part of a blog post series that will go through every component described in the dynamo paper, discuss the rational behind their design, analyze trade-offs, list possible solutions and then walk through concrete implementations in [rldb](https://github.com/rcmgleite/rldb).

## 1.1. Reader requirement
**The code will be written in Rust**, so a reader is expected to understand the basics of the rust language and have familiarity with asynchronous programming. Other topics like networking and any other specific algorithm/data structure will be at least briefly introduced when required (and relevant links will be included for further reading).

## 1.2. What is *rldb*?

*rldb* is a dynamo-like distributed key/value database that provides PUT, GET and DELETE APIs over TCP. Let's break that apart:
- **dynamo-like** - Our database will be based on the [dynamo paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf). So almost every requirement listed in the paper is a requirement of our implementation as well (aside from efficiency/slas that will be ignored for now)
- **distributed** - Our database will be comprised of multiple processes/nodes connected to each other via network. This means that the data we store will be spread across multiple nodes instead of a single one. This is what creates most of the complexity around our implementation. In later posts we will have to understand trade-offs related to strong vs eventual consistency, conflicts and versioning, partitioning strategies, quorum vs sloppy quorum, etc.. All of these things will be explained in detail in due time.
- **key/value** - Our database will only know how to store and retrieve data based on its associated key. There won't be any secondary indexes, schemas etc...
- **APIs over TCP** - The way our clients will be able to interact with our database is through [TCP](https://en.wikipedia.org/wiki/Transmission_Control_Protocol) messages. We will have a very thin framing/codec layer on top of TCP to help us interpret the requests and responses.

# 2. Dissecting the dynamo paper

Let's go through dynamo's requirements and architecture and make sure we can answer the following question: `what is a dynamo-like database`?

I have to start by saying: The aws dynamoDB offer IS **NOT** based on the amazon dynamo paper. This can make things extra confusing once we start looking at the APIs we are going to create so I want to state this clearly here at the beginning to avoid issues in the future.

## 2.1. The dynamo use-case

One of the most relevant use-cases that led to the development of the dynamo db was the amazon shopping cart.
The most important aspect of the shopping cart is: whenever a customer tries to add an item to the cart (mutate it), the operation **HAS** to succeed. This means maximizing write Availability is a key aspect of a dynamo database.

## 2.2. Dynamo requirements and design

### 2.2.1. Write availability
As explained in the previous section, one of the key aspects of a dynamo database is how important write availability is.
According to the [CAP theorem](https://en.wikipedia.org/wiki/CAP_theorem) when a system is in the presence of partition, it has to choose between Consistency(C) and Availability(A).

Availability is the ability of a system to respond to requests successfully (`availability = (1 - (n_requests - n_error) / n_requests) * 100`)

Consistency is related to the following question: `Is a client guaranteed to see the most recent value for a given key when it issues a GET?` (also known as read-after-write consistency).

In a dynamo-like database, Availability is always prioritized over consistency, making it an eventually-consistent database.

The way dynamo increases write (and get) availability is by using a technique called `leaderless replication` in conjunction with `sloppy quorums` and `hinted hand-offs` (`sloppy quorum` and `hinted hand-off` will be explained in future posts).

In leaderless replicated systems, multiple nodes in the cluster can accept writes (as opposed to leader-based replication) and consistency guarantees can be configured (to some extent) by leveraging `Quorum`. To describe `Quorum`, let's go through an example in which the `Quorum` Configuration is: `replicas: 3, reads: 2, writes: 2`

![leaderless_replication_quorum](https://raw.githubusercontent.com/rcmgleite/rldb/rafael-blobposts/docs/blog_series/part_0.intro/leaderless_replication_quorum.png)

In this example, a client sends a PUT to `Node A`. In order for the Put to be considered successful using the quorum configuration stated, 2 out of 3 replicas need to acknowledge the PUT **synchronously**. So `Node A` stores the data locally (first aknowledged put) and then forwards the request to another 2 nodes (for a total of 3 replicas as per Quorum configuration). If any of the 2 nodes acknowledge the replication Put, `Node A` can responde with success to the client.

A similar algorithm is used for reads. In the configuration from our example, a read is only successful if 2 nodes respond to the read request successfully.

When deciding what your quorum configuration should look like, the trade off being evaluated is around consistency guarantees vs performance(request time).

if you want stronger consistency guarantees, the formula you have to follow is: 

```
reads + writes > replicas
```

in our example, `reads = 2, writes = 2, replicas = 3` -> we are following this equation and therefore are opting for strong consistency guarantees while sacrificing performance - every read and write require at least 2 nodes to respond. When we discuss `sloppy quorums` and `hinted hand-offs` we will be much more nuanced about our analysis on consistency but I'll table this discussion for now for brevity sake.

The problem with leaderless replication is: We are now open to version conflicts due to concurrent writes.
The following image depicts a possible scenario where this would happen. Assume Quorum configuration to be `replicas 2, reads: 1, writes: 1`.

![leaderless_replication_conflict](https://raw.githubusercontent.com/rcmgleite/rldb/rafael-blobposts/docs/blog_series/part_0.intro/leaderless_replication_conflict.png)

To detect conflicts, dynamo databases use techniques like vector clocks (or version vectors). Vector clocks will be explained in great detail in future posts.
If a conflict is detected, both conflicting values are stored and a conflict resolution process needs to happen at a later stage. In the dynamo case, conflicts are handled by clients during `read`. When a client issues a GET for a key which has conflicting values, both values are sent as part of the response and the client has to issue a subsequent PUT to resolve to conflict with whatever value it wants to store. Again, more details on conflict resolution will be part of future posts on Get and Put API implementations.

### 2.2.2. System Scalability

Quoting *Designing Data-Intensive applications - chapter 1*
"Scalability is the term used to describe a system's ability to cope with increased load".
For the dynamo paper, this means that when the number of read/write operations increase, the database should
be able to still operate on the same availability and performance levels.

To achieve scalability, dynamo-like databases rely on several different techniques:
- Replication
    - scales reads
    - as mentioned in the previous section, replication is implemented via leaderless-replication with version vectors for conflict detection and resolution
- Partitioning - spreading the dataset amongst multiple nodes
    - scales writes
    - Dynamo relies on a technique called consistent-hashing to decide which nodes should own which keys. Consistent hashing and its pros and cons will be explained in future posts.

### 2.2.3. Data Durability
"Durability is the ability of stored data to remain intact, complete, and uncorrupted over time, ensuring long-term accessibility." ([ref](https://www.purestorage.com/knowledge/what-is-data-durability.html)).
Storage systems like GCP object storage describe durability in terms of [how many nines of durability they guarantee over a year](https://cloud.google.com/storage/docs/availability-durability).
For GCP, durability is guaranteed at 11 nines - ie: GCP won't lose any more than 0.000000001 percent of your data in a year. We won't be calculating how many nines of durability our database will be able to provide, but we will apply many different techniques that increase data durability in multiple different components.

Most relevant durability techniques that we will go over are:

1. Replication -> Adds redundancy to the data stored
2. Checksum and checksum bracketing -> guarantees that no corruptions (either network or memory) can lead to data loss
3. Anti entry / read repair -> whenever a node doesn't have data that it should have (eg: maybe it was offline for deployment while writes were happening), our system has to be able to back-fill it.

### 2.2.4. Node discovery and failure detection

Dynamo databases rely on [Gossip protocols](https://en.wikipedia.org/wiki/Gossip_protocol) to discover cluster nodes, detect node failures and share partitioning assignments. This is on contrast with databases that rely on external services (like [zookeeper](https://zookeeper.apache.org/doc/r3.8.4/zookeeperUseCases.html)) for this.

## 2.3. Dynamo-like database summary

The dynamo-like database key characteristics are:
- Eventually consistent storage system (but with tunnable consistency guarantees via `Quorum` configuration)
- Relies on leaderless-replication + sloppy quorums and hinted handoffs to maximize PUT availability
- Relies on Vector clocks for conflict detection
- Confliction resolution is handled by the client during reads
- Data is partitioned using Consistent-Hashing
- Durability is guaranteed by multiple techniques with anti-entropy and read-repair being the most relevant ones
- Node discovery and failure detection are implemented via Gossip Protocol

# Next steps

Based on the concepts introduced in this post and the use case of the dynamo paper, the next posts on this series will walk through each component of the dynamo architecture, explain how it fits into the overall design, discuss alternate solutions and tradeoffs and then dive into specific implementations of the chosen solutions.

Below I include a (non-comprehensive) list of topics/components that will be covered in the next posts:

- **Part 1 - Handling requests - a minimal TCP server**
- **Part 2 - Introducing PUT and GET for single node**
- **Part 3 - Bootstrapping our cluster: Node discovery and failure detection**
- **Part 4 - Partitioning with consistent-hashing**
- **Part 5 - Replication - the leaderless approach**
- **Part 6 - Versioning - How can we detect conflicts in a distributed system?**
- **Part 7 - Quorum based PUTs and GETs**
- **Part 8 - Sloppy quorums and hinted handoffs**
- **Part 9 - Re-balancing/re-sharding after cluster changes**
- **Part 10 - Guaranteeing integrity - the usage of checksums**
- **Part 11 - Read repair**
- **Part 12 - Active anti-entropy** (will likely have to be broken down into multiple posts since we will have to discuss merkle trees)

In order for me to focus on what you actually care about, please leave comments, complains and whatever else you might think while going through these posts. It's definitely going to be more useful the more people engage.

Cheers,