#!/bin/bash

## create backup of a datadir for experiments/debugging etc.
## it creates copy of editable files like mdbx.dat
## and hardlinks to immutable files like snapshots

set -e

# Check if correct number of arguments provided
if [ $# -ne 2 ]; then
    echo "Usage: $0 <source_directory> <destination_directory>"
    echo "Example: $0 /path/to/source /path/to/destination"
    exit 1
fi

# Get source and destination from command line arguments
source="$1"
destination="$2"

# Validate that source directory exists
if [ ! -d "$source" ]; then
    echo "Error: Source directory '$source' does not exist"
    exit 1
fi

# Remove trailing slashes for consistent path handling
source="${source%/}"
destination="${destination%/}"

# Convert to absolute paths (required for --link-dest)
source_abs=$(realpath "$source")

# Create the destination directory first, then get absolute path (portable, no -m needed)
mkdir -p "$destination"
destination_abs=$(realpath "$destination")

# Check if destination is a subdirectory of source
dest_rel=""
if [[ "$destination_abs" == "$source_abs"/* ]]; then
    dest_rel="${destination_abs#$source_abs/}"
    echo "Note: Destination is a subdirectory of source, will be excluded from operations"
fi

echo "Mirroring '$source' to '$destination'"

# Files that must be copied (mutable), not hardlinked
MUTABLE_FILES=(mdbx.dat mdbx.lck jwt.hex LOCK prohibit_new_downloads.lock nodekey '*.log')

# Build rsync options for hardlinking immutable files
RSYNC_LINK_OPTS=(
    -a                          # archive mode
    --link-dest="$source_abs"   # hardlink to source when files match
    --delete                    # remove files in dest that don't exist in source
    --exclude='.DS_Store'
)

# Exclude destination if it's a subdirectory of source
if [ -n "$dest_rel" ]; then
    RSYNC_LINK_OPTS+=(--exclude="$dest_rel")
fi

# Exclude mutable files from hardlink pass
for f in "${MUTABLE_FILES[@]}"; do
    RSYNC_LINK_OPTS+=(--exclude="$f")
done

# First pass: hardlink all immutable files
echo "Hardlinking immutable files..."
rsync "${RSYNC_LINK_OPTS[@]}" "$source_abs/" "$destination_abs/"

# Second pass: copy mutable files (without --link-dest to force real copy)
echo "Copying mutable files..."
RSYNC_COPY_OPTS=(
    -a
)

# Exclude destination if it's a subdirectory of source
if [ -n "$dest_rel" ]; then
    RSYNC_COPY_OPTS+=(--exclude="$dest_rel")
fi

# Build include patterns for mutable files only
INCLUDE_PATTERNS=()
for f in "${MUTABLE_FILES[@]}"; do
    INCLUDE_PATTERNS+=(--include="$f")
done

rsync "${RSYNC_COPY_OPTS[@]}" \
    --include='*/' \
    "${INCLUDE_PATTERNS[@]}" \
    --exclude='*' \
    "$source_abs/" "$destination_abs/"

echo "Mirror complete!"