---
description: How to configure your web3 wallet to use your Erigon node RPC
---

# Web3 Wallet

Whatever network you are running, it's easy to connect your Erigon node to your local web3 wallet.

For Erigon to provide access to wallet functionalities it is necessary to enable RPC by adding the flags

```bash
--http.addr="0.0.0.0" --http.api=eth,web3,net,debug,trace,txpool
```

For example:

```bash
/build/bin/erigon --http.addr="0.0.0.0" --http.api=eth,web3,net,debug,trace,txpool
```

## Metamask

To configure your local Metamask wallet (browser extension):

* Click on the **network selector button**. This will display a list of networks to which you're already connected
* Click **Add network**
* A new browser tab will open, displaying various fields to fill out. Complete the fields with the proper information, in this example for Ethereum network:
  * **Network Name**: `Ethereum on E3` (or any name of your choice)
  * **Chain ID**: `1` for chain ID parameter see [Supported Networks](supported-networks.md)
  * **New RPC URL**: `http://127.0.0.1:8545`
  * **Currency Symbol**: `ETH`
  * **Block Explorer URL**: `https://www.etherscan.io` (or any explorer of your choice)

After performing the above steps, you will be able to see the custom network the next time you access the network selector.
