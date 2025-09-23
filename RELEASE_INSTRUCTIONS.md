# How to prepare Erigon release (things not to forget)

## Update Jump dest optimisation code
This step does not have to be completed during emergency updates, because failure to complete it has only a minor impact on the
performance of the initial chain sync.

In the source code `core/skip_analysis.go`, there is a constant `MainnetNotCheckedFrom` which should be equal to the block number,
until which we have manually checked the usefulness of the Jump dest code bitmap. In order to update this, one needs to run these
commands:
````
make state
./build/bin/state checkChangeSets --datadir=<path to datadir> --block=<value of MainnetNotCheckedFrom>
````
If there are any transactions where code bitmap was useful, warning messages like this will be displayed:
````
WARN [08-01|14:54:27.778] Code Bitmap used for detecting invalid jump tx=0x86e55d1818b5355424975de9633a57c40789ca08552297b726333a9433949c92 block number=6426298
````
In such cases (unless there are too many instances), all block numbers need to be excluded in the `SkipAnalysis` function, and comment to it. The constant `MainnetNotCheckedFrom` needs to be updated to the first block number we have not checked. The value can be taken from the output of the `checkChangeSets`
utility before it exits, like this:
````
INFO [08-01|15:36:04.282] Checked                                  blocks=10573804 next time specify --block=10573804 duration=36m54.789025062s
````

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
