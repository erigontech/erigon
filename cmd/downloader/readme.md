## Snapshots overview

Snapshots store "cold" data outside the main database as immutable files (`.seg`). They cover old blocks, state history, etc. New nodes can sync from snapshots instead of re-executing all blocks from genesis.

- Stored in `datadir/snapshots` — can be symlinked/mounted to cheaper disk
- Downloaded **once** on first startup; subsequent files are self-generated
- Blocks older than 90K (`FullImmutabilityThreshold`) are moved from DB to snapshots in the background

# snapshots directory structure

The downloader maintains data files in `datadir/snapshots`. Files in subdirectories are also data files.

- `preverified.toml` — created when all snapshots from the published hash list are downloaded and verified. Acts as a marker for initial sync completion. Removing it causes the downloader to re-sync to the latest published hashes.
- `.torrent` — created per snapshot file. During initial sync, created only after full download to avoid locking to an incomplete file.
- `.part` — created for files still downloading, to prevent other components from reading incomplete data.

# Downloader

Service to seed/download historical data (snapshots, immutable `.seg` files) via BitTorrent.

## Start Erigon with snapshots support

```shell
# 1. Downloader runs inside Erigon by default:
erigon --snapshots --datadir=<your_datadir>
```

```shell
# 2. Run downloader as a separate process:
make erigon downloader

# Start downloader (optionally limit bandwidth: --torrent.download.rate=512mb --torrent.upload.rate=512mb)
downloader --downloader.api.addr=127.0.0.1:9093 --torrent.port=42068 --datadir=<your_datadir>
# --downloader.api.addr  internal communication with Erigon
# --torrent.port=42068   public BitTorrent protocol port

erigon --snapshots --downloader.api.addr=127.0.0.1:9093 --datadir=<your_datadir>
```

Nodes download only snapshots registered in https://github.com/erigontech/erigon-snapshot.
Snapshots grow from 1K-block files, merge up to 500K-block files, then seed automatically.

## How to create snapshots for a new network

```shell
# Dump blocks from DB to .seg files:
erigon snapshots retire --datadir=<your_datadir>

# Create .torrent files (checksums):
downloader torrent_create --datadir=<your_datadir>

# Print hashes (output format compatible with https://github.com/erigontech/erigon-snapshot):
downloader torrent_hashes --datadir=<your_datadir> --chain=<chain>

# Show diff vs currently released .toml from erigon-snapshot (one line per entry, grep-friendly):
downloader torrent_hashes --datadir=<your_datadir> --chain=<chain> --diff

# Start seeding:
downloader --downloader.api.addr=127.0.0.1:9093 --datadir=<your_datadir>
```

```shell
# Snapshot creation doesn't require a fully-synced node — a few stages are enough:
STOP_AFTER_STAGE=Senders ./build/bin/erigon --snapshots=false --datadir=<your_datadir>
# For security, a fully-synced node is preferred.

# Erigon indexes snapshots automatically; can also run manually (not required for seeding):
erigon seg index --datadir=<your_datadir>
```

## Architecture

Downloader works from `datadir/snapshots/*.torrent` files, which can be created four ways:

- Erigon gRPC call `downloader.Download(list_of_hashes)` — triggers `.torrent` creation
- Erigon creates a new `.seg` file — downloader scans and creates `.torrent`
- Operator manually copies `.torrent` files (rsync from another server or backup)
- Operator manually copies `.seg` file — downloader scans and creates `.torrent`

Erigon:
- Connects to downloader and shares the list of hashes (see https://github.com/erigontech/erigon-snapshot)
- Waits for 100% download, then creates `.idx` secondary index files (e.g. block-by-hash lookup)
- Switches to normal staged sync — no further connection to downloader needed
- Does not re-download snapshots on upgrade to avoid unpredictable downtime; may produce new ones itself

Downloader:
- Reads `.torrent` files, downloads and seeds everything they describe
- Uses https://github.com/ngosang/trackerslist — see [../../db/downloader/util.go](../../db/downloader/util.go)
- Seeds automatically

Note: `.idx` files use a random seed to prevent attacks — all nodes have different `.idx` files but identical `.seg` files. If you add/remove `.seg` files manually, also remove `<your_datadir>/downloader`.

## Verify snapshot checksums

```shell
# Use if you see bugs, bans, or suspect hardware issues:
downloader --verify --datadir=<your_datadir>
downloader --verify --verify.files=v1.0-1-2-transaction.seg --datadir=<your_datadir>
```

## Seedbox

A seedbox seeds archive files without running a full Erigon node:
- No synced Erigon required
- Works on cheap hardware (disk, CPU, RAM)

```shell
downloader --seedbox --datadir=<your> --chain=mainnet
```

Seedbox can fall back to **Webseed** — a centralized HTTP file store (e.g. private S3 bucket with signed URLs). Decentralized BitTorrent has priority; webseed is used as support/fallback.

```shell
downloader --datadir=<your> --chain=mainnet --webseed=<webseed_url>
# See also: --help for --webseed. Can also be passed via datadir/webseed.toml
```

## WebSeed — bootstrap a new network

Upload data to R2 (or any HTTP server) and create a manifest:

```shell
go run ./cmd/downloader manifest --datadir=/erigon/ --chain="$CHAIN" > /erigon/snapshots/manifest.txt

rclone sync /erigon/snapshots/ your_account:your-bucket-name-$CHAIN/ -L --progress \
  --files-from=/erigon/snapshots/manifest.txt \
  --s3-use-multipart-uploads=true --s3-use-multipart-etag=true --s3-upload-cutoff=300Mi
```

Point Erigon at the webseed:

```shell
erigon --datadir=<your> --chain=mainnet --webseed=<webseed_url>
# or
downloader --datadir=<your> --chain=mainnet --webseed=<webseed_url> --seedbox
# default URL list: erigon-snapshot/webseed/mainnet.toml
```

## Utilities

```shell
downloader torrent_cat /path/to.torrent

downloader torrent_magnet /path/to.torrent

downloader torrent_clean --datadir <datadir>  # remove all .torrent files in datadir

downloader manifest-verify --chain <chain> [--webseeds 'a','b','c']  # verify remote webseed manifest
```

## Faster rsync

```shell
rsync -aP --delete -e "ssh -T -o Compression=no -x" <src> <dst>
```

## Release — publish new snapshot hashes

Hashes are auto-committed hourly to branch `auto`; before release, merge `auto` → `main` manually.

```
crontab -e
@hourly  cd <erigon_source_dir> && ./cmd/downloader/torrent_hashes_update.sh <your_datadir> <network_name> 1>&2 2>> ~/erigon_cron.log
```
