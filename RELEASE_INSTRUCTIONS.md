# How to prepare Erigon release (things not to forget)

## Update DB Schema version if required

In the file `common/dbutils/bucket.go` there is variable `DBSchemaVersion` that needs to be updated if there are any changes in the database schema, leading to data migrations.
In most cases, it is enough to bump minor version.

## Update remote KV version if required

In the file `ethdb/remote/remotedbserver/server.go` there is variable `KvServiceAPIVersion` that needs to be updated if there are any changes in the remote KV interface, or
database schema, leading to data migrations.
In most cases, it is enough to bump minor version. It is best to change both DB schema version and remove KV version together.

## Compact the state domains if a regeneration is done

If a regeneration is done, the state domains need to be compacted. This can be done by running the following command:
````
make integration
./build/bin/integration compact_domains --datadir=<path to datadir> --replace-in-datadir
````

## Update version.go

After a release branch has been created, update `erigon/db/version/version.go`.
Let's say you're releasing Erigon v3.1.0.
Then in branch `release/3.1` of [erigon](https://github.com/erigontech/erigon) set `Major = 3`, `Minor = 1`, `Patch = 0`, `Modifier = ""`, and `DefaultSnapshotGitBranch = "release/3.1"`. (Don't forget to create branch `release/3.1` of [erigon-snapshot](https://github.com/erigontech/erigon-snapshot).)
In branch `main` of [erigon](https://github.com/erigontech/erigon) set `Major = 3`, `Minor = 2`, `Patch = 0`, `Modifier = "dev"`, and `DefaultSnapshotGitBranch = "main"`.
