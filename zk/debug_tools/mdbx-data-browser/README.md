# MDBX data browser

MDBX data browser represents a CLI tool, that is able to query the MDBX database, used by the CDK Erigon.
It offers two CLI commands:
- `output-blocks` - it receives block numbers as the parameter and outputs the blocks information
- `output-batches` - it receives batch number as the parameter and outputs the retrieved batches information

## CLI Commands Documentation
This paragraph documents the CLI commands that are incorporated into the MDBX DB browser.

### Global Flags

#### `verbose`
- **Name**: `verbose`
- **Usage**: If verbose output is enabled, it prints all the details about blocks and transactions in the batches, otherwise just its hashes.
- **Destination**: `&verboseOutput`
- **Default Value**: `false`

#### `file-output`
- **Name**: `file-output`
- **Usage**: If file output is enabled, all the results are persisted within a file.
- **Destination**: `&fileOutput`
- **Default Value**: `false`

### Commands

#### `output-batches`
It is used to output the batches by numbers from the Erigon database. 
In case `verbose` flag is provided, it collects all the data for the blocks and transactions in the batch (otherwise only hashes).
In case `file-output` flag is provided, results are printed to a JSON file (otherwise on a standard output).

- **Name**: `output-batches`
- **Usage**: Outputs batches by numbers.
- **Action**: `dumpBatchesByNumbers`
- **Flags**:
  - `data-dir`: Specifies the data directory to use.
  - `bn`: Batch numbers.
    - **Name**: `bn`
    - **Usage**: Batch numbers.
    - **Destination**: `batchOrBlockNumbers`
  - `verbose`: See [verbose](#verbose) flag.
  - `file-output`: See [file-output](#file-output) flag.

#### `output-blocks`
It is used to output the blocks by numbers from the Erigon database. 
In case `verbose` flag is provided, it collects all the data for the blocks and transactions in the batch (otherwise only hashes).
In case `file-output` flag is provided, results are printed to a JSON file (otherwise on a standard output).

- **Name**: `output-blocks`
- **Usage**: Outputs blocks by numbers.
- **Action**: `dumpBlocksByNumbers`
- **Flags**:
  - `data-dir`: Specifies the data directory to use.
  - `bn`: Block numbers.
    - **Name**: `bn`
    - **Usage**: Block numbers.
    - **Destination**: `batchOrBlockNumbers`
  - `verbose`: See [verbose](#verbose) flag.
  - `file-output`: See [file-output](#file-output) flag.

**Note:** In case, `output-blocks` is ran with `verbose` flag provided, it is necessary to provide the proper chain id to the `params/chainspecs/mainnet.json`. This is the case, because CDK Erigon (for now) uses hardcoded data to recover transaction senders, and chain id information is read from the mentioned file.

### Example Usage

**Pre-requisite:** Navigate to the `zk/debug_tools/mdbx-data-browser` folder and run `go build -o mdbx-data-browser`

#### `output-batches` Command

```sh
./mdbx-data-browser output-batches --datadir chaindata/ --bn 1,2,3 [--verbose] [--file-output]
```

#### `output-blocks` Command

```sh
./mdbx-data-browser output-blocks --datadir chaindata/ --bn 100,101,102 [--verbose] [--file-output]
```
