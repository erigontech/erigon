# TxnBench
TxnBench is a tool to benchmark nodes latency on different sets of blocks 
and requests

## How does it work?
For me, benchmark of anything is always about 3 steps:
1. Choosing a dataset to bench
2. Benching all the machines/nodes/versions_of_code (run benchmark on them)
3. Comparing them (maybe just by glance or by utilities)

So that's why this tool provides a pretty straightforward way to bench:

## Example
Let's use txnbench on the only example we can: compare latency of different 
node versions on `eth_getTransactionByHash` request
### Prepare data
We want to test our request on a stable dataset. Moreover, we want to make it on
all of evm blockchains, also we want it to be always reliable to the current situation.

For our bench we need three types of txs: from the beginning of the blockchain,
the middle of it, and some almost latest blocks. Of course after some time
this dataset would be outdated, so there is a way to build it (and rebuild later).

So, command `BENCH_RPC_URL=put_here_rpc_address go run ./cmd/txnbench gen` will generate you
`benchdata.toml`, which would look like that
```toml
[meta]
chain_id = 1
latest_block = 23641147

[[blocks]]
number = 5910286
txs = ['0x087ac153f3e7efb8b85fecda8c32e134bf728420d277fab1f44cd6af3ee80104', '0xba8daf63e0b5b21abadf3fdc63dd63e03a9df15d8e8ba5feadd003116ae8307e']

[[blocks]]
number = 11820573
txs = ['0x98e050234d58a45af349986e43db5c65ef780386ca12beefa1856478742e877d', '0xeef69816ba0878d0c0a3b914ffe0aac71df4c5f09f667a81bae9852a084d403a']

[[blocks]]
number = 17730860
txs = ['0x532e1129b4b526532d156f9bb78316e8fe2290d01798484c2c25ff85998f3900', '0xde48c40de6f5c99fe58d8fc2709840bd020e6d03e1563ae4c33eb54b1bc8a01c']
```
As you can see, here is some meta-info of the blockchain on top of benchdata and three groups of txs bellow.

### Bench your node
Command bellow would prepare bench_name.json file to you for given node
`BENCH_RPC_URL=your_rpc_address go run ./cmd/txnbench run name_of_your_bench`
Here you can see that I added the possibility to name your benchmark just for
quality of life (mine personally). For example, this command above will create
name_of_your_bench.json
```json
{
  "meta": {
    "name": "name_of_your_bench",
    "timestamp": "2025-10-23T15:21:17.737343962Z",
    "rpc_url": "http://localhost:8545",
    "chain_id": 100,
    "bench_file": "benchdata.toml",
    "latest_block_hint": 42774261
  },
  "results": [
    {
      "block": 10693566,
      "tx_hash": "0x41882c9314d389ae1d0a9024e0b48ac0b4e0b56e8f969e30aa1010ce3e27e783",
      "first_latency_ms": 82.163,
      "avg_latency_ms": 0.2687,
      "stddev_latency_ms": 0.05410699277049749,
      "repeats": 10
    },
    {
      "block": 21387130,
      "tx_hash": "0x40c31807fc2d9036bef76967ff70249cedf14d87a504c8b287436ee12cb564e7",
      "first_latency_ms": 49.424,
      "avg_latency_ms": 0.23220000000000002,
      "stddev_latency_ms": 0.044145718302508616,
      "repeats": 10
    },
    {
      "block": 21387130,
      "tx_hash": "0x87d915be0d9599e0182904587697ea80c65a431a3069ed2c8c8f8aa661ab61d1",
      "first_latency_ms": 44.842,
      "avg_latency_ms": 0.23409999999999997,
      "stddev_latency_ms": 0.036525333674040546,
      "repeats": 10
    },
    {
      "block": 32080695,
      "tx_hash": "0x779bb6dd00b47934784fea79bf9f55b4a9f5491847032d4394248ed7705956ae",
      "first_latency_ms": 31.31,
      "avg_latency_ms": 1.0277999999999998,
      "stddev_latency_ms": 0.2060446553541246,
      "repeats": 10
    },
    {
      "block": 32080695,
      "tx_hash": "0x1834741b96f2fc0d6376d552eb6a73bb0ed0a66f244d96904444748ae31c5a3a",
      "first_latency_ms": 29.926,
      "avg_latency_ms": 0.2666,
      "stddev_latency_ms": 0.047933286972624785,
      "repeats": 10
    }
  ]
}
```
Here we benched average latency and "first latency" for every transaction
from `benchdata.toml`. I have an idea that most of the nodes have something like "warm cache", so
there are two different types of latency: cold (from files/db) and warm (from cache).
So, `first_latency_ms` represents cold latency, and `avg_latency_ms` and `stddev_latency_ms` represents
average latency of warm cache and standard derivation of it.

### Compare two benches
Here I wanted some tooling like benchstat, so I've made this command:
`go run ./cmd/txnbench compare old.json new.json`

Example of the output below:
```
FIRST-LATENCY (cold)
name	old avg ms	new avg ms	delta
block_10693566	82.163	0.681	-99.2%
block_21387130	47.133	0.340	-99.3%
block_32080695	30.618	0.680	-97.8%

WARM-LATENCY (avg of repeats)
name	old avg ms	new avg ms	delta
block_10693566	0.269	0.260	-3.2%
block_21387130	0.233	0.221	-5.0%
block_32080695	0.647	0.659	+1.8%
```

