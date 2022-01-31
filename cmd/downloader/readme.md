# Downloader

Service to seed/download historical data (immutable .seg files)

## Start Erigon in snapshot sync mode 

Only Mainnet, Goerli and BSC networks are supported now.

```shell
make erigon downloader 

# Start downloader (can limit network usage by 512mb/sec: --download.rate=512mb --upload.rate=512mb)
downloader --downloader.api.addr=127.0.0.1:9093 --torrent.port=42068 --datadir=<your_datadir>
# --downloader.api.addr - is for internal communication with Erigon
# --torrent.port=42068  - is for public BitTorrent protocol listen 

# Erigon on startup does send list of .torrent files to Downloader and wait for 100% download accomplishment
erigon --experimental.snapshot --downloader.api.addr=127.0.0.1:9093 --datadir=<your_datadir> 
```

## To create new network or bootnode, need create new snapshots and start seeding them

```shell
# Create new snapshots (can change snapshot size by: --from=0 --to=1_000_000 --segment.size=500_000)
# It will dump blocks from Database to .seg files:
erigon snapshots create --datadir=<your_datadir> 

# Create .torrent files (Downloader will seed automatically all .torrent files)
# output format is compatible with https://github.com/ledgerwatch/erigon-snapshot
downloader info_hashes --rebuild --datadir=<your_datadir>

# Start downloader
downloader --downloader.api.addr=127.0.0.1:9093 --datadir=<your_datadir>

# Erigon is not required for snapshots seeding 
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

Downloader does:

- Read .torrent files, download everything described by .torrent files
- Use https://github.com/ngosang/trackerslist see [./trackers/embed.go](./trackers/embed.go)
- automatically seeding

Technical details:

- To prevent attack - .idx creation using random Seed - all nodes will have different .idx file (and same .seg files)