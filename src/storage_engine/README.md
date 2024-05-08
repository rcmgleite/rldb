## Description

This module contains storage engine implementations that can back rldb.

## Implementations

### InMemory
TODO

### Log structured hash table
TODO

### Log structured merge tree
TODO

## Design notes
- Currently all APIs are async and will likely rely on tokio for its functionality
  - This can be a bit odd since async-io for files (which will be used for disk based implementations)
    is not handled by the kernel (eg: epoll).
  - Since performance is not the point of this project, choosing this path for now for simplicity