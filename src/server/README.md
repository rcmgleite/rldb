## Description

This module implements the TCP server that wraps the database logic for rldb.

### Design
The server implementation tries to be serialization-agnostic.
This means that every [`Request`] (the unit of framing of the protocol) contains currently only 3 fields

- id: An identifier of the message that can be used to derive how to parse the payload of the request
- length: The length of the payload included
- payload: an opaque byte array

In rldb, the [`cmd`] mod contains the logic related to parsing and serializing payloads according to the implemented commands. It currently uses json as serialization format. See [`cmd`] docs for more info.