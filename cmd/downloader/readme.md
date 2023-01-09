# Downloader

Service to seed/download historical data (snapshots, immutable .seg files) by Bittorrent protocol

## Start Erigon with snapshots support

As many other Erigon components (txpool, sentry, rpc daemon) it may be built-into Erigon or run as separated process.

```shell
# 1. Downloader by default run inside Erigon, by `--snapshots` flag:
erigon --snapshots --datadir=<your_datadir> 
```

```shell
# 2. It's possible to start Downloader as independent process, by `--snapshots --downloader.api.addr=127.0.0.1:9093` flags:
make erigon downloader 

# Start downloader (can limit network usage by 512mb/sec: --torrent.download.rate=512mb --torrent.upload.rate=512mb)
downloader --downloader.api.addr=127.0.0.1:9093 --torrent.port=42068 --datadir=<your_datadir>
# --downloader.api.addr - is for internal communication with Erigon
# --torrent.port=42068  - is for public BitTorrent protocol listen 

# Erigon on startup does send list of .torrent files to Downloader and wait for 100% download accomplishment
erigon --snapshots --downloader.api.addr=127.0.0.1:9093 --datadir=<your_datadir> 
```

Use `--snap.keepblocks=true` to don't delete retired blocks from DB

Any network/chain can start with snapshot sync:

- node will download only snapshots registered in next repo https://github.com/ledgerwatch/erigon-snapshot
- node will move old blocks from DB to snapshots of 1K blocks size, then merge snapshots to bigger range, until
  snapshots of 500K blocks, then automatically start seeding new snapshot

Flag `--snapshots` is compatible with `--prune` flag

## How to create new network or bootnode

```shell
# Need create new snapshots and start seeding them
 
# Create new snapshots (can change snapshot size by: --from=0 --to=1_000_000 --segment.size=500_000)
# It will dump blocks from Database to .seg files:
erigon snapshots retire --datadir=<your_datadir> 

# Create .torrent files (Downloader will seed automatically all .torrent files)
# output format is compatible with https://github.com/ledgerwatch/erigon-snapshot
downloader torrent_hashes --rebuild --datadir=<your_datadir>

# Start downloader (seeds automatically)
downloader --downloader.api.addr=127.0.0.1:9093 --datadir=<your_datadir>

# Erigon is not required for snapshots seeding. But Erigon with --snapshots also does seeding. 
```

Additional info:

```shell
# Snapshots creation does not require fully-synced Erigon - few first stages enough. For example:  
STOP_AFTER_STAGE=Senders ./build/bin/erigon --snapshots=false --datadir=<your_datadir> 
# But for security - better have fully-synced Erigon


# Erigon can use snapshots only after indexing them. Erigon will automatically index them but also can run (this step is not required for seeding):
erigon snapshots index --datadir=<your_datadir> 
```

## Architecture

Downloader works based on <your_datadir>/snapshots/*.torrent files. Such files can be created 4 ways:

- Erigon can do grpc call downloader.Download(list_of_hashes), it will trigger creation of .torrent files
- Erigon can create new .seg file, Downloader will scan .seg file and create .torrent
- operator can manually copy .torrent files (rsync from other server or restore from backup)
- operator can manually copy .seg file, Downloader will scan .seg file and create .torrent

Erigon does:

- connect to Downloader
- share list of hashes (see https://github.com/ledgerwatch/erigon-snapshot )
- wait for download of all snapshots
- when .seg available - automatically create .idx files - secondary indices, for example to find block by hash
- then switch to normal staged sync (which doesn't require connection to Downloader)
- ensure that snapshot dwnloading happening only once: even if new Erigon version does include new pre-verified snapshot
  hashes, Erigon will not download them (to avoid unpredictable downtime) - but Erigon may produce them by self.

Downloader does:

- Read .torrent files, download everything described by .torrent files
- Use https://github.com/ngosang/trackerslist see [./trackers/embed.go](../../../erigon-lib/downloader/trackers/embed.go)
- automatically seeding

Technical details:

- To prevent attack - .idx creation using random Seed - all nodes will have different .idx file (and same .seg files)
- If you add/remove any .seg file manually, also need remove `<your_datadir>/snapshots/db` folder

## How to verify that .seg files have same checksum withch current .torrent files

```
# Use it if you see weird behavior, bugs, bans, hardware issues, etc...
downloader torrent_hashes --verify --datadir=<your_datadir>
```

## Faster rsync

```
rsync -aP --delete -e "ssh -T -o Compression=no -x" <src> <dst>
```

## Release details

Start automatic commit of new hashes to branch `master`

```
crontab -e
@hourly        cd <erigon_source_dir> && ./cmd/downloader/torrent_hashes_update.sh <your_datadir> <network_name> 1>&2 2>> ~/erigon_cron.log
```

It does push to branch `auto`, before release - merge `auto` to `main` manually
