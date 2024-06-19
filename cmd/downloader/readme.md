## Snapshots (synonym of segments/shards) overview

- What are "snapshots"? - It's a way to store "cold" data outside of main database. It's not 'temporary' files -
  it's `frozen db` where stored old blocks/history/etc... Most important: it's "building block" for future "sync Archive
  node without execution all blocks from genesis" (will release this feature in Erigon3).

- When snapshots are created? - Blocks older than 90K (`FullImmutabilityThreshold`) are moved from DB to files
  in-background

- Where snapshots are stored? - `datadir/snapshots` - you can symlink/mount it to cheaper disk.

- When snapshots are pulled? - Erigon download snapshots **only-once** when creating node - all other files are
  self-generated

- How does it benefit the new nodes? - P2P and Becaon networks may have not enough good peers for old data (no
  incentives). StageSenders results are included into blocks snaps - means new node can skip it.

- How network benefit? - Serve immutable snapshots can use cheaper infrastructure: Bittorrent/S3/R2/etc... - because
  there is no incentive. Polygon mainnet is 12Tb now. Also Beacon network is very bad in serving old data.

- How does it benefit current nodes? - Erigon's db is 1-file (doesens of Tb of nvme) - which is not friendly for
  maintenance. Can't mount `hot` data to 1 type of disk and `cold` to another. Erigon2 moving only Blocks to snaps
  but Erigon3 also moving there `cold latest state` and `state history` - means new node doesn't need re-exec all blocks
  from genesis.

# Downloader

Service to seed/download historical data (snapshots, immutable .seg files) by
Bittorrent protocol

## Start Erigon with snapshots support

As many other Erigon components (txpool, sentry, rpc daemon) it may be
built-into Erigon or run as separated process.

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

- node will download only snapshots registered in next
  repo https://github.com/ledgerwatch/erigon-snapshot
- node will move old blocks from DB to snapshots of 1K blocks size, then merge
  snapshots to bigger range, until
  snapshots of 500K blocks, then automatically start seeding new snapshot

Flag `--snapshots` is compatible with `--prune` flag

## How to create new network or bootnode

```shell
# Need create new snapshots and start seeding them
 
# Create new snapshots (can change snapshot size by: --from=0 --to=1_000_000 --segment.size=500_000)
# It will dump blocks from Database to .seg files:
erigon snapshots retire --datadir=<your_datadir> 

# Create .torrent files (you can think about them as "checksum")
downloader torrent_create --datadir=<your_datadir>

# output format is compatible with https://github.com/ledgerwatch/erigon-snapshot
downloader torrent_hashes --datadir=<your_datadir>

# Start downloader (read all .torrent files, and download/seed data)
downloader --downloader.api.addr=127.0.0.1:9093 --datadir=<your_datadir>
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

Downloader works based on <your_datadir>/snapshots/*.torrent files. Such files
can be created 4 ways:

- Erigon can do grpc call downloader.Download(list_of_hashes), it will trigger
  creation of .torrent files
- Erigon can create new .seg file, Downloader will scan .seg file and create
  .torrent
- operator can manually copy .torrent files (rsync from other server or restore
  from backup)
- operator can manually copy .seg file, Downloader will scan .seg file and
  create .torrent

Erigon does:

- connect to Downloader
- share list of hashes (see https://github.com/ledgerwatch/erigon-snapshot )
- wait for download of all snapshots
- when .seg available - automatically create .idx files - secondary indices, for
  example to find block by hash
- then switch to normal staged sync (which doesn't require connection to
  Downloader)
- ensure that snapshot downloading happens only once: even if new Erigon version
  does include new pre-verified snapshot
  hashes, Erigon will not download them (to avoid unpredictable downtime) - but
  Erigon may produce them by self.

Downloader does:

- Read .torrent files, download everything described by .torrent files
- Use https://github.com/ngosang/trackerslist
  see [./downloader/util.go](../../erigon-lib/downloader/util.go)
- automatically seeding

Technical details:

- To prevent attack - .idx creation using random Seed - all nodes will have
  different .idx file (and same .seg files)
- If you add/remove any .seg file manually, also need
  remove `<your_datadir>/downloader` folder

## How to verify that .seg files have the same checksum as current .torrent files

```
# Use it if you see weird behavior, bugs, bans, hardware issues, etc...
downloader --verify --datadir=<your_datadir>
downloader --verify --verify.files=v1-1-2-transaction.seg --datadir=<your_datadir>
```

## Create cheap seedbox

Usually Erigon's network is self-sufficient - peers automatically producing and
seeding snapshots. But new network or new type of snapshots need Bootstrapping
step - no peers yet have this files.

**Seedbox** - machine which only seeds archive files:

- Doesn't need synced erigon
- Can work on very cheap disks, cpu, ram
- It works exactly like Erigon node - downloading archive files and seed them

```
downloader --seedbox --datadir=<your> --chain=mainnet
```

Seedbox can fallback to **Webseed** - HTTP url to centralized infrastructure. For example: private S3 bucket with
signed_urls, or any HTTP server with files. Main idea: erigon decentralized infrastructure has higher prioriity than
centralized (which used as **support/fallback**).

```
# Erigon has default webseed url's - and you can create own
downloader --datadir=<your> --chain=mainnet --webseed=<webseed_url>
# See also: `downloader --help` of `--webseed` flag. There is an option to pass it by `datadir/webseed.toml` file
```

--------- 

## Utilities

```
downloader torrent_cat /path/to.torrent

downloader torrent_magnet /path/to.torrent

downloader torrent_clean --datadir <datadir> # remote all .torrent files in datadir
```

## Remote manifest verify
To check that remote webseeds has available manifest and all manifested files are available, has correct format of ETag, does not have dangling torrents etc.
```
downloader manifest-verify --chain <chain> [--webseeds 'a','b','c']
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

## Create seedbox to support network

```
# Can run on empty datadir
downloader --datadir=<your> --chain=mainnet
```

## Launch new network or new type of snapshots

Usually Erigon's network is self-sufficient - peers automatically producing and
seedingsnapshots. But new network or new type of snapshots need Bootstrapping
step - no peers yet have this files.

**WebSeed** - is centralized file-storage - used to Bootstrap network. For
example S3 with signed_url.

Erigon dev team can share existing **webseed_url**. Or you can create own.

```
downloader --datadir=<your> --chain=mainnet --webseed=<webseed_url>

# See also: `downloader --help` of `--webseed` flag. There is an option to pass it by `datadir/webseed.toml` file.   
```

---------------
