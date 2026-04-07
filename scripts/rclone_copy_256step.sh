#!/bin/bash
# Copy 256-step history/domain/idx files from remote without removing existing local files.
# Usage: ./scripts/rclone_copy_256step.sh [remote] [local_snapshots_dir]
#
# Example:
#   ./scripts/rclone_copy_256step.sh 'snapshotter-bm-v34-mainnet-n3.local:/erigon-data/snapshots/' /erigon-data/snapshots/

set -euo pipefail

REMOTE="${1:?Usage: $0 <remote> <local_snapshots_dir>}"
LOCAL="${2:?Usage: $0 <remote> <local_snapshots_dir>}"
FILELIST=$(mktemp /tmp/rclone_256step_XXXXXX.txt)
trap "rm -f $FILELIST" EXIT

echo "Listing remote files from: $REMOTE"
rclone lsf -R "$REMOTE" | grep -E '\.[0-9]+-[0-9]+\.' | while IFS= read -r f; do
    # Extract step range: something.FROM-TO.ext
    if [[ "$f" =~ \.([0-9]+)-([0-9]+)\.[a-z] ]]; then
        from="${BASH_REMATCH[1]}"
        to="${BASH_REMATCH[2]}"
        span=$((to - from))
        if [ "$span" -eq 256 ]; then
            echo "$f"
        fi
    fi
done > "$FILELIST"

count=$(wc -l < "$FILELIST")
echo "Found $count files with 256-step span"
echo "File list: $FILELIST"

if [ "$count" -eq 0 ]; then
    echo "Nothing to copy."
    exit 0
fi

echo "Preview (first 20):"
head -20 "$FILELIST"
echo ""

#read -p "Proceed with rclone copy? [y/N] " confirm
#if [[ "$confirm" =~ ^[Yy]$ ]]; then
#    rclone copy --progress --dry-run --files-from "$FILELIST" "$REMOTE" "$LOCAL" --transfers 10 --checkers 14
#    echo "Done."
#else
#    echo "Aborted. File list saved at: $FILELIST"
#    trap - EXIT  # keep the file
#fi
