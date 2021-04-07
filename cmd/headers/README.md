# How to run the new header/body downloader

In the root of `turbo-geth` project, use this command to build the program:

```
make headers
```

There are two modes in which the program can be run - with external p2p sentry, or with internal p2p sentry (also called
combined). Ethereum mainnet configuration is currently hard-coded.

## Running with an external p2p sentry

```
./buid/bin/headers download --chaindata <path_to_database>
```

The command above specifies `--datadir` option - directory where the database files will be written. These two options
will need to be specified regardless of the mode the program is run. This specific command above assumes and external
p2p sentry running on the same computer listening to the port `9091`. In order to use a p2p sentry on a different
computer, or a different port (or both), the option `--sentry.api.addr` can be used. For example:

```
./buid/bin/headers download  --chaindata <path_to_database> --sentry.api.addr localhost:9999
```

The command above will expect the p2p sentry running on the same computer, but on the port `9999`

## Running with an internal p2p sentry

```
./buid/bin/headers download --chaindata <path_to_database> --combined
```

The command above will run p2p sentry and the header downloader in the same proccess. In this mode, p2p sentry can be
configured using options `--nat`, `--port`, `--staticpeers`, `--netrestrict`, `--discovery`.

## Running p2p sentry

We are currently testing against two implementations of the p2p sentry - one internal to `turbo-geth`, and another -
written in Rust as a part of `rust-ethereum`: https://github.com/rust-ethereum/sentry
In order to run the internal sentry, use the following command:

```
./buid/bin/headers sentry
```

By default, sentry is listening on the `localhost:9091`. In order to change that (to listen on a different network
interface, different port, or both), the option `--sentry.api.addr` can be used. As with the internal p2p sentry,
options `--nat`, `--port`, `--staticpeers`, `--netrestrict`, `--discovery` are also available.

