# Datastream Correctness Check

This Go program connects to a datastream server, processes entries from the datastream, and checks for correctness by validating the sequence and types of entries. 

this tool uses a config file called: `debugToolsConfig.yaml`

## debug tools config

an example of [debugToolsConfig.yaml](../../../debugToolsConfig.yaml.example)

```.yaml
url: "http://localhost:8545"
localUrl: "http://localhost:8546"
datastream: "localhost:12345"
dumpFileName: "dumpfile.dat"
block: 123456
addressRollup: "0x1234567890abcdef1234567890abcdef12345678"
l1Url: "http://localhost:8547"
l1ChainId: 1
l1SyncStartBlock: 1000000
rollupId: 2
elderberryBachNo: 42
```

## How to run

```sh
go run zk/debug_tools/datastream-correctness-check/main.go
```