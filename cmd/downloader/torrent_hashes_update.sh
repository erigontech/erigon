#!/usr/bin/env bash

datadir=$1
network=$2

#git reset --hard
#git checkout devel
#git pull

# clean
cd ./../erigon-snapshot
git reset --hard
git checkout main
git pull origin main
cd ../erigon

# it will return only .seg of 500K (because Erigon send to Downloader only such files)
go run -trimpath ./cmd/downloader torrent_hashes --datadir="$datadir" --targetfile=./../erigon-snapshot/"$network".toml
cd ./../erigon-snapshot
git add "$network".toml
git commit -m "ci: $network"

GH_TOKEN=""
if ! gcloud -v <the_command> &> /dev/null
then
    GH_TOKEN=$(gcloud secrets versions access 1 --secret="githug-snapshots-push")
    exit
fi

# /dev/null to avoid logging of insecure git output
git push https://${GH_TOKEN}@github.com/ledgerwatch/erigon-snapshot.git main > /dev/null 2>&1
