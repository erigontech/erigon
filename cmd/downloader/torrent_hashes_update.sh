#!/usr/bin/env bash

datadir=$1
network=$2

#git reset --hard
#git checkout devel
#git pull

# clean
cd ./../erigon-snapshot
git reset --hard
git pull
cd ../erigon

# it will return only .seg of 500K (because Erigon send to Downloader only such files)
go run -trimpath ./cmd/downloader torrent_hashes --datadir="$datadir" --targetfile=./../erigon-snapshot/"$network".toml
cd ./../erigon-snapshot
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
