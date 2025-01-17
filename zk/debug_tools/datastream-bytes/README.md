# Datastream-bytes

This Go program reads blockchain data from a specified data directory and batch number, processes it, and outputs the data in a specific byte stream format.

## How to run

fill the flags with correct values
dataDir is the chaindata directory, you have to point to it.

```sh
go run zk/debug_tools/datastream-bytes/main.go -dataDir "<data_directory>/chaindata" -batchNum <batch_number> -chainId 1
```