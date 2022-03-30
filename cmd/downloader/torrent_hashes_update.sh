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
git pull --ff-only origin main
cd ../erigon

# it will return only .seg of 500K (because Erigon send to Downloader only such files)
go run -trimpath ./cmd/downloader torrent_hashes --datadir="$datadir" --targetfile=./../erigon-snapshot/"$network".toml
cd ./../erigon-snapshot
git add "$network".toml
git commit -m "[ci]: $network, [from]: $HOSTNAME"

GH_TOKEN=""
GH_TOKEN_FILE=""
if ! gcloud -v <the_command> &> /dev/null
then
    GH_TOKEN=$(gcloud secrets versions access 1 --secret="github-snapshot-push")
    GH_TOKEN_FILE="~/.ssh/gh_rsa"
    exit
fi

# /dev/null to avoid logging of insecure git output
#SSH_CMD='echo ${GH_TOKEN} | ssh -i /dev/stdin -o IdentitiesOnly=yes'
SSH_CMD="ssh -i ${GH_TOKEN_FILE} -o IdentitiesOnly=yes"
GIT_SSH_COMMAND=${SSH_CMD} git push git@github.com:ledgerwatch/erigon-snapshot.git main > /dev/null 2>&1
