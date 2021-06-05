# How to read DB directly - not by Json-RPC/Graphql:

There are 2 options exist:

1. call --private.api.addr there is grpc interface with low-level data access methods - can read any data in any order,
   etc... Interface is here: https://github.com/ledgerwatch/interfaces/blob/master/remote/kv.proto
   Go/C++/Rust libs already exist. Names of buckets and their format you can find in `bucket.go` You can do such calls
   by network.
2. Read Erigon's db while Erigon is running - it's also ok - just need be careful - do not run too long read
   transactions (long read transactions do block free space in DB). Then your app will share with Erigon same OS-level
   PageCache where hot part of db stored. It may be great - if you read hot data (for example do incremental update of
   graph node) - then your reads will be super fast and almost never touch disk. But if you wanna read cold data - then
   your app will load cold data to PageCache and maybe evict some Erigon's hot data. Probably it will not be very
   dangerous - because your reads will happen once while Erigon will touch hot data often and OS's built-in LRU will
   understand which data is more Hot and keep it in RAM.

this 2 options ^ are exactly how RPCDaemon works with flags `--private.api.addr` and `--datadir`. One by using grpc
interface, another by opening Erigon's db in read-only mode while Erigon running. But both this options are
using `RoKV` (stands for read-only) `kv_abstract.go` interface. Option 1 using `kv_remote.go` to implement `RoKV`,
option 2 using - `kv_mdbx.go`

Erigon using MDBX database. But any articles in internet about LMDB are also valid for MDBX.

We have Go, Rust and C++ implementations of `RoKV` interface. 

Rationale and Architecture of DB interface: [./../../ethdb/Readme.md](./../../ethdb/Readme.md)

MDBX docs: [erthink.github.io/libmdbx/](https://erthink.github.io/libmdbx/) and [https://github.com/erthink/libmdbx/blob/master/mdbx.h](https://github.com/erthink/libmdbx/blob/master/mdbx.h)
