# Snapshots - tool for managing remote stanshots

In the root of `Erigon` project, use this command to build the the commands:

```shell
make snapshots
```

It can then be run using the following command

```shell
./buid/bin/snapshots sub-command options...
```

Snapshots supports the following sub commands:

## cmp - compare snapshots

This command takes the following form: 

```shell
    snapshots cmp <location> <location>
```

This will cause the .seg files from each location to be copied to the local machine, indexed and then have their rlp contents compared.

Optionally a `<start block>` and optionally an `<end block>` may be specified to limit the scope of the operation

It is also possible to set the `--types` flag to limit the type of segment file being downloaded and compared.  The currently supported types are `header` and `body` 

## copy - copy snapshots

This command can be used to copy segment files from one location to another.

This command takes the following form: 

```shell
    snapshots copy <source> <destination>
```

Optionally a `<start block>` and optionally an `<end block>` may be specified to limit the scope of the operation

## verify - verify snapshots

-- TBD

## manifest - manage the manifest file in the root of remote snapshot locations

The `manifest` command supports the following actions

| Action | Description |
|--------|-------------|
| list | list manifest from storage location|
| update | update the manifest to match the files available at its storage location | 
| verify |verify that manifest matches the files available at its storage location|

All actions take a `<location>` argument which specified the remote location which contains the manifest

Optionally a `<start block>` and optionally an `<end block>` may be specified to limit the scope of the operation

## torrent - manage snapshot torrent files

The `torrent` command supports the following actions

| Action | Description |
|--------|-------------|
| list | list torrents available at the specified storage location |
| hashes | list the hashes (in toml format) at the specified storage location |
| update | update re-create the torrents for the contents available at its storage location |
| verify |verify that manifest contents are available at its storage location|

All actions take a `<location>` argument which specified the remote location which contains the torrents.

Optionally a `<start block>`` and optionally an `<end block>` may be specified to limit the scope of the operation





