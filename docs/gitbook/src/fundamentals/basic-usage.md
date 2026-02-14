---
description: Command Line Usage and Basic Erigon Configuration
metaLinks:
  alternates:
    - https://app.gitbook.com/s/3DGBf2RdbfoitX1XMgq0/fundamentals/basic-usage
---

# Basic Usage

Erigon is mainly operated through the command line. The commands may vary based on your installation method.

{% tabs %}
{% tab title="Pre-Built Binaries" %}
Open your terminal and simply start Erigon with the command:

```
erigon [options]
```
{% endtab %}

{% tab title="Docker" %}
* You can use Docker Compose like in this [example](../get-started/easy-nodes/how-to-run-an-ethereum-node/#id-2.-configure-and-launch-erigon)
*   Alternatively you can use the Docker syntax, for example:

    `docker run -it erigontech/erigon:v3.2.2 [options]`
{% endtab %}

{% tab title="Built from Source" %}
Open your terminal and move to the directory where you installed Erigon

```
cd erigon
```

You can then start Erigon with the below command:

```shell
./build/bin/erigon [options]
```
{% endtab %}

{% tab title="Windows" %}
To run Erigon after a native compilation, enter the following command in your Command Prompt or PowerShell:

```powershell
erigon.exe [options]
```

{% hint style="warning" %}
**Note**: When you first start Erigon, Windows Firewall may prompt you to allow internet access. Select "YES" to proceed.
{% endhint %}
{% endtab %}
{% endtabs %}

To gracefully stop Erigon simply press `CTRL` + `C`.

## All-in-One Client

The all-in-one client is the preferred option for most users:

```bash
./build/bin/erigon [options]
```

This CLI command allows you to run an Ethereum **full node** where every process is integrated and no special configuration is needed.

The default Consensus Layer utilized is [Caplin](caplin.md), the Erigon flagship embedded CL.

## Example of configuration

To run Erigon with RPCDaemon, TxPool, and other components in a single process is the simplest way to get started. For better performance and load management, you might consider distributing these components across multiple machines.

```sh
./build/bin/erigon --datadir=/desired/path/to/datadir \
 --chain=mainnet \
 --port=30304 \
 --http.port=8546 \
 --torrent.port=42068 \
 --private.api.addr=127.0.0.1:9091 \
 --http \
 --ws \
 --http.api=eth,debug,net,trace,web3,erigon \
 --log.dir.path=/desired/path/to/logs
 --torrent.download.rate=512mb
```

### Flags of Interest

*   Default data directory is `/home/usr/.local/share/erigon`. If you want to store Erigon files in a non-default location, use the flag:

    ```bash
    --datadir=/desired/path/to/datadir
    ```
* The `--chain=mainnet` flag is set by default for Erigon to sync with the Ethereum mainnet. To explore other network options, check the [Supported Networks](supported-networks.md) section. For quick testing, consider selecting a testnet.
* `--log.dir.path` dictates where [logs](logs.md) will be output - useful for sending reports to the Erigon team when issues occur.
* Based on the [sync mode](sync-modes.md) you want to run you can add `--prune.mode=archive` to run a archive node, `--prune.mode=full` for a full node (default value) or `--prune.mode=minimal` for a minimal node.
* `--http.addr="0.0.0.0" --http.api=eth,web3,net,debug,trace,txpool` to use [RPC Service](../interacting-with-erigon/) and e.g. be able to connect your [wallet](web3-wallet.md).
* `--torrent.download.rate=512mb` to increase download speed. While the default downloading speed is 128mb, with this flag Erigon will use as much download speed as it can, up to a maximum of 512 megabytes per second. This means it will try to download data as quickly as possible, but it won't exceed the 512 MB/s limit you've set.

To stop the Erigon node you can use the `CTRL+C` command.

Additional flags can be added to configure the node with several options.

{% content-ref url="configuring-erigon/" %}
[configuring-erigon](configuring-erigon/)
{% endcontent-ref %}

## Help

To learn about the available commands, open your terminal in your Erigon 3 installation directory and run:

```bash
make help
```

This command will display a list of convenience commands available in the Makefile, along with their descriptions.

```
 go-version:                        print and verify go version
 validate_docker_build_args:        ensure docker build args are valid
 docker:                            validate, update submodules and build with docker
 setup_xdg_data_home:               TODO
 docker-compose:                    validate build args, setup xdg data home, and run docker-compose up
 dbg                                debug build allows see C stack traces, run it with GOTRACEBACK=crash. You don't need debug build for C pit for profiling. To profile C code use SETCGOTRCKEBACK=1
 erigon:                            build erigon
 all:                               run erigon with all commands
 db-tools:                          build db tools
 test:                              run unit tests with a 100s timeout
 test-integration:                  run integration tests with a 30m timeout
 lint-deps:                         install lint dependencies
 lintci:                            run golangci-lint linters
 lint:                              run all linters
 clean:                             cleans the go cache, build dir, libmdbx db dir
 devtools:                          installs dev tools (and checks for npm installation etc.)
 mocks:                             generate test mocks
 mocks-clean:                       cleans all generated test mocks
 solc:                              generate all solidity contracts
 abigen:                            generate abis using abigen
 gencodec:                          generate marshalling code using gencodec
 graphql:                           generate graphql code
 gen:                               generate all auto-generated code in the codebase
 bindings:                          generate test contracts and core contracts
 prometheus:                        run prometheus and grafana with docker-compose
 escape:                            run escape path={path} to check for memory leaks e.g. run escape path=cmd/erigon
 git-submodules:                    update git submodules
 install:                           copies binaries and libraries to DIST
 user_linux:                        create "erigon" user (Linux)
 user_macos:                        create "erigon" user (MacOS)
 hive:                              run hive test suite locally using docker e.g. OUTPUT_DIR=~/results/hive SIM=ethereum/engine make hive
 automated-tests                    run automated tests (BUILD_ERIGON=0 to prevent erigon build with local image tag)
 help:                              print commands help

```

For example, from your Erigon 3 installation directory, run:

```bash
make clean
```

This will execute the clean target in the Makefile, which cleans the `go cache`, `build` directory, and `libmdbx` db directory.
