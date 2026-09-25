---
title: "Supported Networks"
description: "Mainnet, testnets, Gnosis, and all other chains Erigon can sync."
sidebar_position: 8
---


# Supported Networks

The default flag is `--chain=mainnet`, which enables Erigon to operate on the Ethereum mainnet. Utilize the flag `--chain=<tag>` to synchronize with one of the supported networks. For example, to synchronize Holesky, one of the Ethereum testnets, use:

```bash
./build/bin/erigon --chain=hoodi
```

## Mainnets

| Chain    | Tag     | ChainId |
| -------- | ------- | ------- |
| Ethereum | mainnet | 1       |
| Gnosis   | gnosis  | 100     |

## Testnets

### Ethereum testnets

| Chain   | Tag     | ChainId  |
| ------- | ------- | -------- |
| Sepolia | sepolia | 11155111 |
| Hoodi   | hoodi   | 560048   |

### Gnosis Chain Testnets

| Chain  | Tag    | ChainId |
| ------ | ------ | ------- |
| Chiado | chiado | 10200   |

## Polygon (not supported)

:::warning
Erigon does not support Polygon. The final release series that officially supported it is 3.1.\* — for Polygon-supported software see [https://github.com/0xPolygon/erigon/releases](https://github.com/0xPolygon/erigon/releases).

The `bor-mainnet` (137) and `amoy` (80002) chain tags remain selectable in this release and the `bor` namespace is still wired, but neither is maintained or tested. Do not plan a new Polygon deployment on Erigon 3.6.
:::
