# How to prepare Erigon release (things not to forget)

## Update DB Schema version if required

In the file `db/kv/tables.go` there is variable `DBSchemaVersion` that needs to be updated if there are any changes in the database schema, leading to data migrations.
In most cases, it is enough to bump minor version.

## Update remote KV version if required

In the file `db/kv/remotedbserver/remotedbserver.go` there is variable `KvServiceAPIVersion` that needs to be updated if there are any changes in the remote KV interface, or
database schema, leading to data migrations.
In most cases, it is enough to bump minor version. It is best to change both DB schema version and remote KV version together.

## Update app.go

After a release branch has been created, update `db/version/app.go`.
Let's say you're releasing Erigon v3.6.0.
Then in branch `release/3.6` of [erigon](https://github.com/erigontech/erigon) set `Major = 3`, `Minor = 6`, `Micro = 0`, `Modifier = ""`, and `DefaultSnapshotGitBranch = "release/3.6"`. (Don't forget to create branch `release/3.6` of [erigon-snapshot](https://github.com/erigontech/erigon-snapshot).)
In branch `main` of [erigon](https://github.com/erigontech/erigon) set `Major = 3`, `Minor = 7`, `Micro = 0`, and `Modifier = "dev"`.

## Update documentation version

After creating a release branch, manually dispatch `.github/workflows/docs-version-bump.yml`
on that branch. For example, after creating `release/3.6`:

```sh
gh workflow run docs-version-bump.yml --ref release/3.6 -f archive_version=v3.5 -f new_label=v3.6
```

The workflow archives the previous documentation label, updates the current label, builds the
site, and opens a documentation pull request when changes are needed.
