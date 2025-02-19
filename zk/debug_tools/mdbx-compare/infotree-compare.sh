#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status
set -o pipefail  # Capture errors in pipelines

# Variables
datadir1=${1}
datadir2=${2}
tablesToCheck=(
    "l1_info_roots"
    "l1_info_leaves"
    "l1_info_tree_updates"
    "l1_info_tree_updates_by_ger"
)

# Run MDBX-Compare debug tool
echo "[$(date)] Running MDBX Compare tool..."
for table in "${tablesToCheck[@]}"; do
    echo "[$(date)] Checking table: $table"
    go run main.go compare --datadir1=$datadir1 --datadir2=$datadir2 --table="$table"
done

echo "[$(date)] Info tree's match!"