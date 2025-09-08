# How to run a Gnosis Chain node

## Prerequisites

- Check the [hardware](../getting-started/hw-requirements.md) prerequisites;
- Check which [type of node](../basic/node.md) you want to run and the [disk space](../getting-started/hw-requirements.md#minimal-node-requirements) required.

## Install Erigon​

To set up Erigon quickly, we recommend the following:
- For Linux and MacOS users, use our [pre-built binaries](../installation/prebuilt.md);
- For Windows users, [build executable binaries natively](../installation/build_exec_win.md).

# Start Erigon​

To execute a Gnosis Chain full node using pre-compiled binaries, use the following basic command:

```bash
erigon --chain=gnosis
```

## Example of basic configuration​

The command above allows you to run your local Erigon node on the Gnosis Chain. Additionally, you can include several options, as shown in the following example:

```bash
erigon \
--chain=gnosis \
--datadir=<your_data_dir> \
--prune.mode=minimal \
--http.addr="0.0.0.0" \
--http.api=eth,web3,net,debug,trace,txpool \
--torrent.download.rate=512mb
```

### Flags explanation

- `--chain=gnosis` specifies the Gnosis Chain network, use `--chain=chiado` for Chiado testnet.
- `--datadir=<your_data_dir>` to store Erigon files in a non-default location. Default data directory is `./home/user/.local/share/erigon`.
- Erigon is full node by default, use `--prune.mode=archive` to run a archive node or `--prune.mode=minimal` (EIP-4444). If you want to change [type of node](../basic/node.md) delete the `--datadir` folder content and restart Erigon with the appropriate flags.
- `--http.addr="0.0.0.0" --http.api=eth,web3,net,debug,trace,txpool` to use RPC and e.g. be able to connect your [wallet](../basic/wallet.md).
- `--torrent.download.rate=512mb` to increase download speed. While the default downloading speed is 128mb, with this flag Erigon will use as much download speed as it can, up to a maximum of 512 megabytes per second. This means it will try to download data as quickly as possible, but it won't exceed the 512 MB/s limit you've set.

To stop your Erigon node you can use the `CTRL+C` command.

When you get familiar with running Erigon from CLI you may also consider [staking](../staking.md) and/or run a [Gnosis chain node with an external Consensus Layer](gno_extcl.md).

Additional flags can be added to [configure](../advanced/configuring.md) Erigon with several [options](../advanced/options.md).