---
description: Erigon Supported Networks and --chain Flag Reference
---

# Supported Networks

The default flag is `--chain=mainnet`, which enables Erigon to operate on the Ethereum mainnet. Utilize the flag `--chain=<tag>` to synchronize with one of the supported networks. For example, to synchronize Holesky, one of the Ethereum testnets, use:

```bash
./build/bin/erigon --chain=holesky
```

## Mainnets

| Chain     | Tag         | ChainId |
| --------- | ----------- | ------- |
| Ethereum  | mainnet     | 1       |
| Polygon\* | bor-mainnet | 137     |
| Gnosis    | gnosis      | 100     |

## Testnets

### Ethereum testnets

| Chain   | Tag     | ChainId  |
| ------- | ------- | -------- |
| Sepolia | sepolia | 11155111 |
| Hoodi   | hoodi   | 560048   |

### Polygon testnets

| Chain  | Tag  | ChainId |
| ------ | ---- | ------- |
| Amoy\* | amoy | 80002   |

### Gnosis Chain Testnets

| Chain  | Tag    | ChainId |
| ------ | ------ | ------- |
| Chiado | chiado | 10200   |

{% hint style="warning" %}
\* The final release series of Erigon that officially supports Polygon is 3.1.\*. For the software supported by Polygon, please refer to the link: [https://github.com/0xPolygon/erigon/releases](https://github.com/0xPolygon/erigon/releases).
{% endhint %}
