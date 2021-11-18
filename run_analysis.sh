#!/bin/sh

BASE=$(pwd) # absolute path to erigon
BIN_DIR=$BASE/build/bin

EXECUTABLE=tx_analysis
ENTRY_FOLDER=$BASE/tx_analysis

RPCDAEMONPORT=8545
IP="127.0.0.1"
WORKERS=4


print_help() {
    # $1 exit code
    echo ""
    echo "Usage: ./run_analysis.sh args [OPTIONS]"
    echo ""
    echo "-h | --help       print usage and available options"
    echo ""
    echo "args: "
    echo "-p | --port       RPC daemon port number"
    echo "-i | --ip         RPC daemon IP address (default '127.0.0.1')"
    echo "-d | --datadir    absolute path to Erigon node folder, e.g: /home/user/my_node"
    echo ""
    echo "Options: "
    echo "-w | --workers    number of parallel workers analizing transactions (default 4)"
    exit $1
}

print_setup() {
    echo "Current setup: "
    echo "RPC daemon port number:   $RPCDAEMONPORT"
    echo "RPC daemon ip:            $IP"
    echo "Data directory:           $DATADIR"
    echo "Number of workers:        $WORKERS"
}

for i in "$@"; do
    case $i in
    -h|--help)
        print_help 0
        ;;
    -p=* | --port=*)
        RPCDAEMONPORT="${i#*=}"
        shift
        ;;
    -i=* | --ip=*)
        IP="${i#*=}"
        shift
        ;;
    -w=* | --workers=*)
        WORKERS="${i#*=}"
        shift
        ;;
    esac
done


if [ -z "$RPCDAEMONPORT" ] || [ -z "$IP" ]; then 
    
    echo "--port, --ip or --datadir args are empty... exiting"

    print_help 1
fi

print_setup

echo "---------------------------------------------------------------"



echo "Building..."
go build -o $BIN_DIR/$EXECUTABLE $ENTRY_FOLDER/...
echo "Starting a programm..."
$BIN_DIR/$EXECUTABLE -port "$RPCDAEMONPORT" -ip "$IP" -workers $WORKERS


# GO = go
# GOBIN = $(CURDIR)/build/bin
# GOTEST = GODEBUG=cgocheck=0 $(GO) test ./... -p 2

# GIT_COMMIT ?= $(shell git rev-list -1 HEAD)
# GIT_BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD)
# GIT_TAG    ?= $(shell git describe --tags `git rev-list --tags="v*" --max-count=1`)
# GOBUILD = env GO111MODULE=on $(GO) build -trimpath -ldflags "-X github.com/ledgerwatch/erigon/params.GitCommit=${GIT_COMMIT} -X github.com/ledgerwatch/erigon/params.GitBranch=${GIT_BRANCH} -X github.com/ledgerwatch/erigon/params.GitTag=${GIT_TAG}"
# GO_DBG_BUILD = $(GO) build -trimpath -tags=debug -ldflags "-X github.com/ledgerwatch/erigon/params.GitCommit=${GIT_COMMIT} -X github.com/ledgerwatch/erigon/params.GitBranch=${GIT_BRANCH} -X github.com/ledgerwatch/erigon/params.GitTag=${GIT_TAG}" -gcflags=all="-N -l"  # see delve docs

# GO_MAJOR_VERSION = $(shell $(GO) version | cut -c 14- | cut -d' ' -f1 | cut -d'.' -f1)
# GO_MINOR_VERSION = $(shell $(GO) version | cut -c 14- | cut -d' ' -f1 | cut -d'.' -f2)