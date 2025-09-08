# Supported Networks

The default flag is `--chain=mainnet`, which enables Erigon 3 to operate on the Ethereum mainnet.
Utilize the flag `--chain=<tag>` to synchronize with one of the supported networks. For example, to synchronize Holesky, one of the Ethereum testnets, use:

```bash
./build/bin/erigon --chain=holesky
```

# Mainnets

|Chain   |	Tag         | ChainId |
|----    | ----         | ----    |
|Ethereum|	mainnet     |   1     |
|Polygon |	bor-mainnet |	137   |
|Gnosis  |	gnosis      |   100   |


# Testnets

## Ethereum testnets

|Chain     |    Tag| ChainId  |
|----------|-------|----------|
|Holesky   |holesky|    17000 |
|Sepolia   |sepolia|  11155111|
|Hoodi     |hoodi  |   560048 |

## Polygon testnets

| Chain       |	Tag  | ChainId |
|----------   |------|---------|
|  Amoy       |	amoy | 80002   |


## Gnosis Chain Testnets

|Chain  | Tag    | ChainId|
|-------|------- |--------|
|Chiado | chiado |	10200 |

