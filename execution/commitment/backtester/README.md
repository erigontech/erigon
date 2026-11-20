# Commitment Backtester

## Background

The commitment backtester aims to provide ways to:

* easily re-execute commitment computations only (in isolation, without block re-execution) for past blocks
* do correctness tests between different commitment computation algorithms (i.e. sequential vs parallel)
* do performance comparisons between different commitment computation algorithms (i.e. sequential vs parallel)

## Usage

To use the commitment backtester we have to sync an Erigon node for any chain with commitment history enabled.
Once that node has synced it can be stopped and can be used for running backtests.

There are two main ways a backtest can be started:

* using `--from 1000000 --to 2000000` to specify a block number range

```
erigon backtest-commitment --from 1000000 --to 2000000 --datadir <datadir> --output-dir <output-dir>
```

* using `--tMinusN 1000000` to specify the number of blocks prior to the current tip:

```
erigon backtest-commitment --tMinusN 1000000 --datadir <datadir> --output-dir <output-dir>
```

To explore other available flags run:

```
erigon backtest-commitment --help
```

## Artefacts

Every backtest run produces result artefacts which are stored in the `--output-dir`. The convention is to create a new
directory for each run which follows the `{fromBlockNum}_{toBlockNum}_{runStartTimestamp}` naming.

The results directory can be found at the end of the backtest logs:

```
[INFO] [12-17|18:55:52.808] finished commitment backtest             blocks=33 in=29.974303041s results=/output-dir/1752715_1752747_1765958122
```

The following can be found inside the results' directory:

```
...
block_1752746
block_1752747
```

With the per-block result directories each containing:

- a pprof cpu profile
- a fgprof profile

```
cpu.fgprof
cpu.prof
```
