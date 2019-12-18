
### Run
`go run ./cmd/rpctest --action=bench1` will print tmpDir. 
And create in tmpDir `results_*.csv` and `vegeta_*.txt` files. 

Command takes long time. Kill it when `vegeta_turbo_geth_debug_storageRangeAt.txt` is few MB. 

File `vegeta_turbo_geth_*.txt` will produce load to `turbo_geth` node, `vegeta_geth_*.txt` to `geth`.
Change host/port in `cmd/rpctest/main.go:routes` variable. 

By default `go run ./cmd/rpctest --action=bench1` calling only Geth node 
because `cmd/rpctest/main.go:main` calling it with first parameter `needCompare=false`.
Set `needCompare=true` to call Geth and TurboGeth nodes and compare results.   

### Install Vegeta
```
go get -u github.com/rs/jplot
go get -u github.com/rs/jaggr
go get -u github.com/tsenart/vegeta
```

### Run vegeta
``` 
tmpDir = "/var/folders/x_/1mnbt25s3291zr5_fxhjfnq9n86kng/T/"
cat $(tmpDir)/turbo_geth_stress_test/vegeta_geth_debug_storageRangeAt.csv | vegeta attack -rate=600 -format=json -duration=20s | vegeta plot > plot.html
open plot.html
```


### Mac environment changes
[Change Open Files Limit](https://gist.github.com/tombigel/d503800a282fcadbee14b537735d202c)


### Results from my Macbook:
start rpcdaemon with turbo_geth: 
```
GODEBUG=remotedb.debug=1 go run ./cmd/geth --remote-db-listen-addr localhost:9997   --rpcport 8545  --rpc --rpcapi eth,debug --nodiscover
GODEBUG=remotedb.debug=1 go run ./cmd/rpcdaemon --rpcapi eth,debug --rpcport 9545 --remote-db-addr 127.0.0.1:9997
```

On simple requests `eth_getBlockByNumber` RPC Daemon looks well:  
```
cat /tmp/turbo_geth_stress_test/vegeta_turbo_geth_eth_getBlockByNumber.txt | vegeta attack -rate=1000 -format=json -duration=20s | vegeta report

Rate 300: 
- Geth Alone: 80% of CPU, 95-Latency 2ms

- Geth Behind RPC Daemon: 25% of CPU
- RPC Daemon: 45% of CPU, 95-Latency 3ms

Rate 1000: 
- Geth Alone: 200% of CPU, 95-Latency 3ms

- Geth Behind RPC Daemon: 50% of CPU
- RPC Daemon: 120% of CPU, 95-Latency 6ms

Rate 2000: 
- Geth Alone: 400% of CPU, 95-Latency 15ms

- Geth Behind RPC Daemon: 100% of CPU
- RPC Daemon: 250% of CPU, 95-Latency 10ms

```

On complex request - `debug_storageRangeAt` producing >600 db.View calls with twice more .Bucket/.Cursor/.Seek calls:
```
echo "POST http://localhost:9545 \n Content-Type: application/json \n @$(pwd)/cmd/rpctest/heavyStorageRangeAt.json" | vegeta attack -rate=20 -duration=20s | vegeta report

Rate 10:
- Geth Alone: 100% of CPU, 95-Latency 15ms 

- Geth Behind RPC Daemon: 200% of CPU
- RPC Daemon: 230% of CPU, 95-Latency 7s
```
Reason is: often usage of `.GetAsOf()` - this method does much `.Next()` and `.Seek()` calls. 
Each `.Seek()` call invalidate internal batch cache of `.Next()` method and remote_db does read `CursorBatchSize` amount of keys again.

```
PoolSize=128, CursorBatchSize=10K -> 95-Latency 30s (eat all conns in pool)
PoolSize=128, CursorBatchSize=1K -> 95-Latency 6s (eat 50 conns in pool)
PoolSize=128, CursorBatchSize=100 -> 95-Latency 600ms (eat 5 conns in pool)
```

RPC daemon known problems: 
- we call .Encode for each cmd/key/value - "maybe" it does syscall to send data. 
Maybe need buffered io.Writer or encode struct{cmd,key,value} at once. Need to write benchmark.   


