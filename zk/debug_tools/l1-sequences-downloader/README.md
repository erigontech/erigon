# l1 sequences downloaders

we have some downloaders in this part of the repo.

## Acc Input Hash

This Go program reads sequences, calldata, and accumulated input hashes from JSON files, verifies the integrity of the accumulated input hashes for a series of batches, and prints progress every 1000 sequences.

To run the program, use the following command:

```sh
go run zk/debug_tools/l1-sequences-downloader/acc-input-hash/main.go
```

The program requires three JSON files as input:

- sequencesMainnet.json: Contains an array of L1BatchInfo objects representing the sequences.
- calldataMainnet.json: Contains a map of transaction hashes to their corresponding calldata.
- accInputHashesMainnet.json: Contains a map of batch numbers to their accumulated input hashes.

## Sequence AccInputHash

Go program that reads a list of L1 batch sequences from a JSON file, fetches the corresponding accumulated input hashes from an Ethereum rollup contract, and writes the results to another JSON file.

this downloader also uses debugToolsConfig an example of it: [debugToolsConfig.yaml](../../../debugToolsConfig.yaml.example)


To run the program, use the following command:

```sh
go run zk/debug_tools/l1-sequences-downloader/sequence-accinputhash/main.go
```

The program requires this JSON files as input:

- sequencesMainnet.json: Contains an array of L1BatchInfo objects representing the sequences.

## Sequence Calldata

Go program that reads L1 batch sequences from a JSON file, fetches the corresponding call data for each transaction from an Ethereum rollup contract, and writes the results to another JSON file.


this downloader also uses debugToolsConfig an example of it: [debugToolsConfig.yaml](../../../debugToolsConfig.yaml.example)

To run the program, use the following command:

```sh
go run zk/debug_tools/l1-sequences-downloader/sequence-calldata/main.go
```

The program requires this JSON files as input:

- sequencesMainnet.json: Contains an array of L1BatchInfo objects representing the sequences.

## Sequence Logs

Go program that fetches logs from an Ethereum rollup contract, parses them to extract L1 batch information, and writes the results to a JSON file.

this downloader also uses debugToolsConfig an example of it: [debugToolsConfig.yaml](../../../debugToolsConfig.yaml.example)

To run the program, use the following command:

```sh
go run zk/debug_tools/l1-sequences-downloader/sequence-logs/main.go
```

The program requires this JSON file as input:

- sequencesMainnet.json: Contains an array of L1BatchInfo objects representing the sequences.