
### Create files with sample requests
`go run ./cmd/rpctest/main.go bench1` will print tmpDir. 
And create in tmpDir `results_*.csv` and `vegeta_*.txt` files. 

Command takes long time. Kill it when `vegeta_erigon_debug_storageRangeAt.txt` is few MB. 

File `vegeta_erigon_*.txt` will produce load to `erigon` node, `vegeta_geth_*.txt` to `geth`.
Change host/port in `--gethUrl`, `--erigonUrl` variable. 

By default `go run ./cmd/rpctest/main.go bench1` calling only Erigon node 
because `cmd/rpctest/rpctest/bench1.go` calling it with first parameter `needCompare=false`.
Set `--needCompare` to call Geth and Erigon nodes and compare results.   

### Install Vegeta
```
go get -u github.com/tsenart/vegeta
```

### Run vegeta
``` 
tmpDir = "/var/folders/x_/1mnbt25s3291zr5_fxhjfnq9n86kng/T/"
cat $(tmpDir)/erigon_stress_test/vegeta_geth_debug_storageRangeAt.csv | vegeta attack -rate=600 -format=json -duration=20s -timeout=300s | vegeta plot > plot.html
open plot.html
```

### Mac environment changes
[Change Open Files Limit](https://gist.github.com/tombigel/d503800a282fcadbee14b537735d202c)


### Results from my Macbook:
start rpcdaemon with erigon:
```
GODEBUG=remotedb.debug=1 go run ./cmd/erigon --private.api.addr localhost:9997   --rpcport 8545  --rpc --rpcapi eth,debug,net --nodiscover
GODEBUG=remotedb.debug=1 go run ./cmd/rpcdaemon --rpcapi eth,debug,net --rpcport 9545 --private.api.addr 127.0.0.1:9997
```

On simple requests `eth_getBlockByNumber` RPC Daemon looks well:  
```
cat /tmp/erigon_stress_test/vegeta_erigon_eth_getBlockByNumber.txt | vegeta attack -rate=1000 -format=json -duration=20s -timeout=300s | vegeta report

300rps: 
- Geth Alone: 80% of CPU, 95-Latency 2ms

- Geth Behind RPC Daemon: 25% of CPU
- RPC Daemon: 45% of CPU, 95-Latency 3ms

1000rps: 
- Geth Alone: 200% of CPU, 95-Latency 3ms

- Geth Behind RPC Daemon: 50% of CPU
- RPC Daemon: 120% of CPU, 95-Latency 6ms

2000rps: 
- Geth Alone: 400% of CPU, 95-Latency 15ms

- Geth Behind RPC Daemon: 100% of CPU
- RPC Daemon: 250% of CPU, 95-Latency 10ms

```

On complex request - `debug_storageRangeAt` producing >600 db.View calls with twice more .Bucket/.Cursor/.Seek calls:
```
echo "POST http://localhost:8545 \n Content-Type: application/json \n @$(pwd)/cmd/rpctest/heavyStorageRangeAt.json" | vegeta attack -rate=20 -duration=20s -timeout=300s | vegeta report

10rps, batchSize 10K:
- Geth Alone: 100% of CPU, 95-Latency 15ms 

- Geth Behind RPC Daemon: 200% of CPU
- RPC Daemon: 230% of CPU, 95-Latency 7s

10rps, batchSize 10:
- Geth Alone: 100% of CPU, 95-Latency 15ms 

- Geth Behind RPC Daemon: 110% of CPU
- RPC Daemon: 100% of CPU, 95-Latency 230ms
```

Reason is: often usage of `.GetAsOf()` - this method does much `.Next()` and `.Seek()` calls. 
Each `.Seek()` call invalidate internal batch cache of `.Next()` method and remote_db does read `CursorBatchSize` amount of keys again.

```
PoolSize=128, CursorBatchSize=10K -> 95-Latency 30s (eat all conns in pool)
PoolSize=128, CursorBatchSize=1K -> 95-Latency 6s (eat 50 conns in pool)
PoolSize=128, CursorBatchSize=100 -> 95-Latency 600ms (eat 5 conns in pool)
```

Idea to discuss: implement `CmdGetAsOf`  

BenchmarkSerialize: 
```
BenchmarkSerialize/encodeKeyValue()-12         	 4249026	       268 ns/op	      12 B/op	       0 allocs/op
BenchmarkSerialize/encoder.Encode(&k)-12       	 4702418	       258 ns/op	      14 B/op	       0 allocs/op
BenchmarkSerialize/encoder.Encode(k)-12        	 3382797	       350 ns/op	     104 B/op	       2 allocs/op
BenchmarkSerialize/encoder.MustEncode(&k)-12   	 8431810	       140 ns/op	       0 B/op	       0 allocs/op
BenchmarkSerialize/encoder.MustEncode(k)-12    	 5446293	       262 ns/op	     114 B/op	       2 allocs/op
BenchmarkSerialize/Encode(struct)-12           	 4160940	       266 ns/op	       0 B/op	       0 allocs/op
BenchmarkSerialize/10K_Encode(&k,_&v)-12       	    1368	   1089648 ns/op	  402976 B/op	       0 allocs/op
BenchmarkSerialize/Encode([10K]k,_[10K]v)-12   	    1825	    548953 ns/op	  491584 B/op	       4 allocs/op
```


