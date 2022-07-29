# Sentry - component to

In the root of `Erigon` project, use this command to build the sentry:

```
make sentry
```

There are two modes in which the program can be run - with external p2p sentry, or with internal p2p sentry (also called
combined). Ethereum mainnet configuration is currently hard-coded.

## Running with an external p2p sentry

```
./buid/bin/sentry
```

```
./buid/bin/sentry --datadir=<sentry_datadir>
```

The command above specifies `--datadir` option - directory where the database files will be written (it doesn't need access to Erion's datadir). These two options
will need to be specified regardless of the mode the program is run. This specific command above assumes and external
p2p sentry running on the same computer listening to the port `9091`. In order to use a p2p sentry on a different
computer, or a different port (or both), the option `--sentry.api.addr` can be used. For example:

```
./buid/bin/sentry --datadir=<sentry_datadir> --sentry.api.addr=localhost:9999
```

The command above will expect the p2p sentry running on the same computer, but on the port `9999`

Options `--nat`, `--port`, `--staticpeers`, `--netrestrict`, `--discovery` are also available.

We are currently testing against two implementations of the p2p sentry - one internal to `Erigon`, and another - written
in Rust as a part of `rust-ethereum`: https://github.com/rust-ethereum/sentry
In order to run the internal sentry, use the following command:


