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


