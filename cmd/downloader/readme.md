# Downloader

Is a service which does download and seed historical data.

Historical data - is immutable, files have .seg extension.

## Architecture

Downloader works based on <your_datadir>/snapshots/*.torrent files (`etl-tmp` and `snapshots` directories MUST be on
same drive). Such files can be created 4 ways:

- Erigon can do grpc call downloader.Download(list_of_hashes), it will trigger creation of .torrent files
- Erigon can create new .seg file, Downloader will scan .seg file and create .torrent
- operator can manually copy .torrent files (rsync from other server or restore from backup)
- operator can manually copy .seg file, Downloader will scan .seg file and create .torrent

Erigon does:

- connect to Downloader
- share list of hashes (see https://github.com/ledgerwatch/erigon-snapshot )
- wait for download of all snapshots
- then switch to normal staged sync (which doesn't require connection to Downloader)

Downloader does:

- Read .torrent files, download everything described by .torrent files
- Use https://github.com/ngosang/trackerslist see [./trackers/embed.go](./trackers/embed.go)
- automatically seeding

## How to

### Start

```
downloader --datadir=<your_datadir> --downloader.api.addr=127.0.0.1:9093
erigon --downloader.api.addr=127.0.0.1:9093 --experimental.snapshot
```

### Limit download/upload speed

```
downloader --download.limit=10mb --upload.limit=10mb
```

### Print info_hashes in format compatible with https://github.com/ledgerwatch/erigon-snapshot

```
downloader info_hashes --datadir=<your_datadir>
```

### Force re-calculate info_hashes and print in format compatible with https://github.com/ledgerwatch/erigon-snapshot

```
// will re-read all .seg files (high disk IO)
// also does remove and create .torrent files
downloader info_hashes --datadir=<your_datadir> --rebuild
```

### Create new snapshots

```
rm <your_datadir>/snapshots/*.torrent
erigon snapshots create --datadir=<your_datadir> --from=0 --segment.size=500_000
```

### Download snapshots to new server

```
rsync server1:<your_datadir>/snapshots/*.torrent server2:<your_datadir>/snapshots/
// re-start downloader 
```

### Re-create all .idx files (by re-read all .seg files)

```
// Disk-read-intense
erigon snapshots index --datadir=<your_datadir> --rebuild
```

## Known Issues

- RPCDaemon with --datadir option need restart to make new segments available