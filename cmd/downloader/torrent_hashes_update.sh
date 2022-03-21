#!/usr/bin/env bash

network=$1
datadir=$2

#git reset --hard
#git checkout devel
#git pull
# it will return only .seg of 500K (because Erigon send to Downloader only such files)
go run -trimpath ./cmd/downloader torrent_hashes --datadir="$datadir" > ./../erigon-snpshots/"$network".toml
cd ./../erigon-snapshots
git add "$network".toml
git commit -m "ci: $network"
git push
# update Erigon submodule
cd ./../erigon
cd turbo/snapshotsync/snapshothashes/erigon-snapshots
git checkout main
git pull
cd ../../../../
git add turbo/snapshotsync/snapshothashes/erigon-snapshots
git commit -m "ci: $network snapshots"
git push
