## Part 1 - Exposing APIs to clients

### 1. Background

Every database needs to somehow vend its APIs for clients to consume.
This is not strictly important for our goal of studying the internals of a database system but nevertheless we need to include an entry-point for clients to interact with, even if only to allows us to write proper end-to-end tests.

### 2. Requirements

#### 2.1. Non-functional

1. **readability** - Our concern number one
  - This is going to be a requirement for every component but since this is our first one, better to state it clearly. If, at any point, going through the code base yields too many "wtf"s, we messed up..
2. **Minimal amount of dependencies** 
  - This another cornerstone of every component - our goal is to learn how all of these data structure and algorithms are implemented. if we just include dependencies for all of them, what's the point of even building this database!?
3. **Simplicity and extensibility**
  - similar to requirement 1. - making components as simple as possible definitely helps with readability.
  - let's make sure adding new commands doesn't require lots of changes/boilerplate.

#### 2.2. Functional

The only functional requirement I'll add is the ability to trace requests based on request_ids - ie: For every request our database receives, we have to be able to trace it for debugging purposes.

### 3. Design

Different databases provide different interfaces for clients to interact with. A few examples are:
- **SQLite** - This is a sql database that runs embedded in the process that consumes it. For this reason, there's a C client library that exposes all necessary APIs for database management and query execution.
- **Redis** - Redis is an in memory database that provides a wide variaty of APIs to access different data structures though TCP. It uses a custom protocol on top of TCP called [RESP](https://redis.io/docs/latest/develop/reference/protocol-spec/). So clients that want to integrate with Redis have to implement RESP on top of TCP to be able to issue commands against it.
- **CouchDB** - exposes a RESTful HTTP API for clients to consume.

As previously state, in our case the actual interface is less relevant - our goal is to study the internals of storage systems, not to write a production ready, performant database for broad consumption. For that reason, I'll use non-functional requirements 2.(as few dependencies as possible) and 3. (simplicity) to choose to expose our APIs via TCP using a thin serialization protocol based on JSON.

Our serialization protocol will be based on what we call a `Message`. This is a small binary header followed by an optional JSON payload that represents the `Command` we want to execute.
After parsing a `Message` from the socket, we will then build a `Command` (in this post we will go over `Ping`) and a `Command` can be executed.

### 4. Implementation

Ok, let's start by defining what a `Command` is. 
Think of a `Command` as a `Controller` for a regular [MVC](https://en.wikipedia.org/wiki/Model%E2%80%93view%E2%80%93controller) application. A command basically interprets the request a client issued and generates the appropriate response by calling internal methods.
Let's get a bit more concrete by implementing `Ping`.
From a client's perspective, `Ping` is as simple as:

1. Client sends a Ping request over TCP,
2. Cluster node parses the received bytes into a `Message`, 
3. Cluster node constructs a `Ping` command from the `Message` parsed in step 2
4. Cluster node replies to the client with a *"PONG"* message

The ping command can be found [here](https://github.com/rcmgleite/rldb/blob/b96e5a9be7ef72bce98e1f3359a0c8792ae9a59c/src/cmd/ping.rs) in `rldb`. The core of it's implementation is shown below:
```
...
#[derive(Debug)]
pub struct Ping;

#[derive(Serialize, Deserialize)]
pub struct PingResponse {
    pub message: String,
}

impl Ping {
    pub async fn execute(self) -> Result<PingResponse> {
        Ok(PingResponse {
            message: "PONG".to_string(),
        })
    }
}
...
```

Ok, pretty simple so far. How do we construct a `Ping` `Command` from a Tcp connection?
That's where the `Message` definition comes into play. `Message` is our serialization protocol on top of TCP.
It defines the format/frame of the bytes a client has to send in order for our server to be able to interpret it.
Think of it as an intermediate abstraction that enables our database server to properly route requests to the specific `Command` based on the arguments provided.

A `Message` is defined as follows:
```
pub struct Message {
    /// Used as a way of identifying the format of the payload for deserialization
    pub cmd_id: CommandId,
    /// A unique request identifier - used for request tracing and debugging
    /// Note that this has to be encoded as utf8 otherwise parsing the message will fail
    pub request_id: String,
    /// the Request payload
    pub payload: Option<Bytes>,
}
```

Every field prior to the payload can be thought of as the header of the message while payload is what actually encodes the `Command` we are trying to execute. Many other fields could be included as part of the header. Two fields will most likely be added in the future
1. a checksum of the payload (maybe one including the overall message as well?)
2. the timestamp of the message (just for debugging purposes)

Now let's construct a Message from a Tcp connection:

```
impl Message {
    pub async fn try_from_async_read(tcp_connection: &mut TcpStream) -> Result<Self> {
        let cmd_id = CommandId::try_from(tcp_connection.read_u8().await?)?;
        let request_id_length = tcp_connection.read_u32().await?;
        let request_id = {
            let mut buf = vec![0u8; request_id_length as usize];
            tcp_connection.read_exact(&mut buf).await?;
            String::from_utf8(buf).map_err(|_| {
                Error::InvalidRequest(InvalidRequest::MessageRequestIdMustBeUtf8Encoded)
            })?
        };

        let payload_length = tcp_connection.read_u32().await?;

        let payload = if payload_length > 0 {
            let mut buf = vec![0u8; payload_length as usize];
            tcp_connection.read_exact(&mut buf).await?;
            Some(buf.into())
        } else {
            None
        };

        Ok(Self {
            cmd_id,
            request_id,
            payload,
        })
    }
}
```

Three notes:
1. I removed most of the error handling from this snippet to make it more readable. Read the actual implementation [here](https://github.com/rcmgleite/rldb/blob/b96e5a9be7ef72bce98e1f3359a0c8792ae9a59c/src/server/message.rs#L68) if curious.
2. If you check the real implementation, you will see that the signature of the function is slightly different. Instead of

`pub async fn try_from_async_read(tcp_connection: &mut TcpStream) -> Result<Message>`

we have

`pub async fn try_from_async_read<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self>`

This is an important distinction that is worth discussing. If we had the `TcpStream` as argument, every test would require that we setup a real Tcp server, create Tcp connections and send / receive bytes via localhost.
Not only this would make tests slow, they would also make writing tests much more complicated than they had to be. When writing tests, the way I would recommend anyone to think about them is:

   - Write as little code as possible for setting up the test
     - Think of it this way: The more complicated the test setup, the more likely your test is not doing what you think it's doing. It is very common to believe a test is asserting on a specific condition when in fact, due to the setup, the code path you are supposed to be testing is not even being exercised. This is also true when we abuse the usage of mocks. We will talk more about this when we get into testing our more complicated components.
   - Think about the invariants of your function: ie: what can you assert about your inputs, outputs and internal state that should always hold? Again, this will be much clearer once we start building our internal components. For now, you can take a look at [this test](https://github.com/rcmgleite/rldb/blob/b96e5a9be7ef72bce98e1f3359a0c8792ae9a59c/src/cluster/heartbeat.rs#L212-L216) for an example.
   - Remember that [dependency injection](https://en.wikipedia.org/wiki/Dependency_injection) is key for unit testing but DON'T ABUSE mocks.
   - Specific error types are a game changer for Rust code. In the test example I'll link below, you will see that we can assert precisely on the type of the error returned. This is much better than relying on specific stringified messages or any other form of error handling. A good rule of thumb for rust code is: Use the type system to help you. This is true for errors but also for every other abstraction you build.

Here is an example of a test that we can write without setting up any tcp connection:

```
#[tokio::test]
    async fn test_max_message_size_exceeded() {
        let mut reader = MaxMessageSizeExceededAsyncRead::default();
        let err = Message::try_from_async_read(&mut reader)
            .await
            .err()
            .unwrap();

        match err {
            Error::InvalidRequest(InvalidRequest::MaxMessageSizeExceeded { max, got }) => {
                assert_eq!(max, MAX_MESSAGE_SIZE);
                assert_eq!(got, MAX_MESSAGE_SIZE + 1);
            }
            _ => {
                panic!("Unexpected error: {}", err);
            }
        }
    }
```
In this example, I chose to impl [AsyncRead](https://docs.rs/tokio/latest/tokio/io/trait.AsyncRead.html) for my custom struct `MaxMessageSizeExceededAsyncRead`. But we could've used `UnixStream`s or many other options of types that impl `AsyncRead` to inject the bytes we are interested in.

3. You will likely notice that no timeouts are set to any of the tcp connection interactions. This is something that has to be included if we are building a production ready service but that I chose to skip at this point as it is not the focus of our work. But it has to be stated that if you plan on deploying anything that interacts with the network to a production environment, error handling of timeouts/slow requests is mandatory. (please let me know if you would find it useful to go over how to introduce timeouts in this project.. I would gladly do so and go over the how and why)

Now that we can build a `Message` out of a `TcpStream`, we must be able to serialize a `Message` into bytes so that it can be sent via network. The snippet below depicts how this can be done (without error handling again)
    
```
impl Message {
    pub fn serialize(self) -> Bytes {
        let payload = self.payload.clone();
        let payload_len = payload.clone().map_or(0, |payload| payload.len());
        let mut buf = BytesMut::with_capacity(
            self.request_id.len() + payload_len + 2 * size_of::<u32>() + size_of::<u8>(),
        );

        buf.put_u8(self.cmd_id as u8);
        buf.put_u32(self.request_id.len() as u32);
        buf.put(self.request_id.as_bytes());
        buf.put_u32(payload_len as u32);
        if let Some(payload) = payload {
            buf.put(payload);
        }

        assert_eq!(buf.capacity(), buf.len());
        buf.freeze()
    }
}
```
Let's go over some notes here as well:
1. You will quickly see that I rely on the [bytes](https://crates.io/crates/bytes) crate heavily throught this project. Bytes is a very useful tool to write network related applications that allows you to work with contiguous byte buffers while avoiding memcopies almost entirely. It also provides some neat traits like [BufMut](https://docs.rs/bytes/1.6.1/bytes/buf/trait.BufMut.html) which gives use methods like `put_u32` etc.. Please refer to its documentation for more information.
2. I included an assertion about `buf` len and capacity that is there to make sure this important invariant is held: If we allocate a buffer of size X, we must completely fill it without ever resizing it. This guarantees that we are properly computing the buffer size prior to filling it (which is important if we care about performance, for example)

Finally, given that we have a `cmd_id` field in `Message`, we can easily choose how to serialize the payload field for each specific Command and vice versa.

For Ping, a Request `Message` would look like

```
Message {
    cmd_id: 1,
    request_id: <some string>,
    payload: None 
}
```

and a response message would look like

```
Message {
    cmd_id: 1,
    request_id: <some string>,
    payload Some(Bytes::from(serde_json::to_string(PingResponse {message: "PONG".to_string()})))
}
```

And that's it: A Client needs to know only about `Message` in order to be able to interact with our database.

If you want to walk through the code yourself, here are the pointers to the 4 larger components

1. [The TcpListener](https://github.com/rcmgleite/rldb/blob/b96e5a9be7ef72bce98e1f3359a0c8792ae9a59c/src/server/mod.rs#L38)
2. [Message](https://github.com/rcmgleite/rldb/blob/b96e5a9be7ef72bce98e1f3359a0c8792ae9a59c/src/server/message.rs)
3. [Command](https://github.com/rcmgleite/rldb/blob/b96e5a9be7ef72bce98e1f3359a0c8792ae9a59c/src/cmd/mod.rs)
4. [Ping](https://github.com/rcmgleite/rldb/blob/b96e5a9be7ef72bce98e1f3359a0c8792ae9a59c/src/cmd/ping.rs)

#### The IntoMessage trait and request_id (covers the functional requirement around tracing requests)

For any type that we want to be able to be converted into a `Message`, we can implement the `IntoMessage` trait for it.

```
pub trait IntoMessage {
    /// Same as [`Message::cmd_id`]
    fn cmd_id(&self) -> CommandId;
    /// Same as [`Message::payload`]
    fn payload(&self) -> Option<Bytes> {
        None
    }
    fn request_id(&self) -> String {
        REQUEST_ID
            .try_with(|rid| rid.clone())
            .unwrap_or("NOT_SET".to_string())
    }
}
```

You will see that this trait has 2 default implementations:

1. payload -> Commands like Ping don't have a payload. So Ping doesn't have to implement this specific function and just rely on the default behavior
2. request_id -> This is the more interesting one: Every message has t have a request_id associated with it. This is going to be extremely important once we have to analyze logs/traces for requests received by the database.

The way request_id is handled by *rldb* (at the time of this writing) is: request_id is injected either by the client or by the server into [tokio::task_local](https://docs.rs/tokio/latest/tokio/macro.task_local.html) as soon as the `Message` is parsed from the network.
If you really think about it, it's a bit strange that we let the client set the request_id that is internal to the database. We allow this to happen (at least for now) because rldb nodes **talk to each other**. In requests like `Get`, a rldb cluster node will have to talk to at least another 2 nodes to issue internal GET requests. In order for us to be able to connect all of the logs generated for a client request, we have to provide a mechanism for a rldb node to inject the request_id being executed when issuing requests other nodes in the cluster.

## Closing remarks

That's all for part 1 of the series. The next chapter will introduce the first version of the PUT and GET APIs.
As usual, please leave comments, questions, complains, etc.. That's how I'll know if this content is valuable or not.

Cheers,