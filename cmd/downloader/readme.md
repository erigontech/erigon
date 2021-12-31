# Downloader

Is a service which does download and seed historical data.

Historical data - is immutable, files have .seg extension.

## Architecture

Erigon does:

- connect to Downloader
- share list of hashes (see https://github.com/ledgerwatch/erigon-snapshot )
- wait for download of all snapshots
- then switch to normal staged sync (which doesn't require connection to Downloader)

Downloader does:

- create .torrent files in <your_datadir>/snapshot directory (can be used by any torrent client)
- download everything. Currently rely on https://github.com/ngosang/trackerslist
  see [./trackers/embed.go](./trackers/embed.go)
- automatically seeding
- operator can manually copy .seg files to <your_datadir>/snapshot directory, then Downloader will not download files (
  but will verify it's hash).

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

### Add hashes to https://github.com/ledgerwatch/erigon-snapshot

```
downloader print_torrent_files --datadir=<your_datadir>
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


