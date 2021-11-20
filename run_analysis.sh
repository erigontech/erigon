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

go build -o $BIN_DIR/$EXECUTABLE $ENTRY_FOLDER/main.go

echo "Starting a programm..."
$BIN_DIR/$EXECUTABLE -port "$RPCDAEMONPORT" -ip "$IP" -workers $WORKERS
