## Description

This module implements the TCP server that wraps the database logic for rldb.

### Design

#### Message framing and Json
The server implementation tries to be serialization-agnostic.
This means that every [`Message`] (the unit of framing of the protocol) contains currently only 3 fields

- id: An identifier of the message that can be used to derive how to parse the payload of the request
- length: The length of the payload included
- payload: an opaque byte array

In rldb, the [`cmd`] mod contains the logic related to parsing and serializing payloads according to the implemented commands. It currently uses json as serialization format. See [`cmd`] docs for more info.

#### Cluster listener
Every node in a rldb cluster is connected to every other node via a long lived TCP connection.
Nodes use this TCP connection primarely to propagete their view of the cluster (using a gossip protocol).
This means that for a node in cluster mode, 2 TCP listeners are created and are running at all times.