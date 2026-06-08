# How to prepare Erigon release (things not to forget)

## Update DB Schema version if required

In the file `db/kv/tables.go` there is variable `DBSchemaVersion` that needs to be updated if there are any changes in the database schema, leading to data migrations.
In most cases, it is enough to bump minor version.

## Update remote KV version if required

In the file `db/kv/remotedbserver/remotedbserver.go` there is variable `KvServiceAPIVersion` that needs to be updated if there are any changes in the remote KV interface, or
database schema, leading to data migrations.
In most cases, it is enough to bump minor version. It is best to change both DB schema version and remote KV version together.

## Compact the state domains if a regeneration is done

If a regeneration is done, the state domains need to be compacted. This can be done by running the following command:
````
make integration
./build/bin/integration compact_domains --datadir=<path to datadir> --replace-in-datadir
````

## Update app.go

After a release branch has been created, update `db/version/app.go`.
Let's say you're releasing Erigon v3.6.0.
Then in branch `release/3.6` of [erigon](https://github.com/erigontech/erigon) set `Major = 3`, `Minor = 6`, `Micro = 0`, `Modifier = ""`, and `DefaultSnapshotGitBranch = "release/3.6"`. (Don't forget to create branch `release/3.6` of [erigon-snapshot](https://github.com/erigontech/erigon-snapshot).)
In branch `main` of [erigon](https://github.com/erigontech/erigon) set `Major = 3`, `Minor = 7`, `Micro = 0`, and `Modifier = "dev"`.
