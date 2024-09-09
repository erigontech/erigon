# Erigon Sub Commands

## Backup

## Import

## Init

## Support

This command connects erigon to diagnostics tools by establishing websocket connection. 

In order to conect diagnostics run 
```
./build/bin/erigon support --debug.addrs <value> --diagnostics.addr <value> --diagnostics.sessions <PINs>
```

|   |   |
|---|---|
|diagnostics.addr|Address of the diagnostics system provided by the support team, include unique session PIN. [Instructions how to get proper adderess](https://github.com/erigontech/diagnostics?tab=readme-ov-file#step-4)|
|debug.addrs|Comma separated list of URLs to the debug endpoints thats are being diagnosed. This endpoints must mutch values of `diagnostics.endpoint.addr:diagnostics.endpoint.port` by default its `localhost:6060`|
|diagnostics.sessions|Comma separated list of session PINs to connect to [Instructions how to obtain PIN](https://github.com/erigontech/diagnostics?tab=readme-ov-file#step-2)|
|||


## Snapshots

This sub command can be used for manipulating snapshot files

### Uploader

The `snapshots uploader` command starts a version of erigon customized for uploading snapshot files to
a remote location.  

It breaks the stage execution process after the senders stage and then uses the snapshot stage to send
uploaded headers, bodies and (in the case of polygon) bor spans and events to snapshot files.  Because 
this process avoids execution in run signifigantly faster than a standard erigon configuration.

The uploader uses rclone to send seedable (100K or 500K blocks) to a remote storage location specified
in the rclone config file.

The **uploader** is configured to minimize disk usage by doing the following:

* It removes snapshots once they are loaded
* It agressively prunes the database once entites are transferred to snapshots

in addition to this it has the following performance related features:

* maximises the workers allocated to snapshot processing to improve thoughtput
* Can be started from scratch by downloading the latest snapshots from the remote location to seed processing

The following configuration can be used to upload blocks from genesis where:

|   |   |
|---|---|
| sync.loop.prune.limit=500000  | Sets the records to be pruned to the database to 500,000 per iteration (as opposed to 100)  |
| upload.location=r2:erigon-v2-snapshots-bor-mainnet | Specified the rclone loaction to upload snapshot to |
| upload.from=earliest | Sets the upload start location to be the earliest available block, which will be 0 in the case of a fresh installation, or specified by the last block in the chaindata db |
| upload.snapshot.limit=1500000 | Tells the uploader to keep a maximum 1,500,000 blocks in the `snapshots` before deleting the aged snapshot |
| snapshot.version=2 | Indivates the version to be appended to snapshot file names when they are creatated|


```shell
erigon/build/bin/erigon seg uploader --datadir=~/snapshots/bor-mainnet --chain=bor-mainnet \
  --bor.heimdall=https://heimdall-api.polygon.technology --bor.milestone=false --sync.loop.prune.limit=500000 \
  --upload.location=r2:erigon-v2-snapshots-bor-mainnet --upload.from=earliest --snapshot.version=2 \
  --upload.snapshot.limit=1500000 
```

In order to start with the lates uploaded block when starting with an empty drive set the `upload.from` flag to `latest`.  e.g. 

```shell
--upload.from=latest
```

The configuration of the uploader implicitly sets the following flag values on start-up:

```shell
    --sync.loop.break.after=Senders
	--sync.loop.block.limit=100000
	--sync.loop.prune.limit=100000
	--upload.snapshot.limit=1500000 
	--nodownloader=true
	--http.enables=false
	--txpool.disable=true
```
