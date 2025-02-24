# Sentry - component to

In the root of `Erigon` project, use this command to build the sentry:

```
make sentry
```

There are two modes in which the program can be run - with external p2p sentry, or with internal p2p sentry (also called
combined). Ethereum mainnet configuration is currently hard-coded.

## Running with an external p2p sentry

```
./build/bin/sentry
```

```
./build/bin/sentry --datadir=<sentry_datadir>
```

The command above specifies `--datadir` option - directory where the database files will be written (it doesn't need access to Erigon's datadir). These two options
will need to be specified regardless of the mode the program is run. This specific command above assumes and external
p2p sentry running on the same computer listening to the port `9091`. In order to use a p2p sentry on a different
computer, or a different port (or both), the option `--sentry.api.addr` can be used. For example:

```
./build/bin/sentry --datadir=<sentry1_datadir> --sentry.api.addr=localhost:9091
./build/bin/sentry --datadir=<sentry2_datadir> --sentry.api.addr=localhost:9191
./build/bin/erigon --sentry.api.addr="localhost:9091,localhost:9191"
```

The command above will expect the p2p sentry running on the same computer, but on the port `9091`

Options `--nat`, `--port`, `--staticpeers`, `--netrestrict`, `--discovery` are also available.
