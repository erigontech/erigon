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

# Second pass: copy mutable files using reflinks (CoW clones) where supported
echo "Copying mutable files..."

# Test if reflinks work from source to destination (requires same filesystem + CoW support)
_test_src="$source_abs/.reflink_test_$$"
_test_dst="$destination_abs/.reflink_test_$$"
_cp_clone=""
touch "$_test_src" 2>/dev/null && {
    case "$(uname -s)" in
        Darwin) cp -c "$_test_src" "$_test_dst" 2>/dev/null && _cp_clone="cp -c" ;;
        *)      cp --reflink=always "$_test_src" "$_test_dst" 2>/dev/null && _cp_clone="cp --reflink=always" ;;
    esac
    rm -f "$_test_src" "$_test_dst"
}

if [ -n "$_cp_clone" ]; then
    echo "  Using reflinks ($_cp_clone)"
    for f in "${MUTABLE_FILES[@]}"; do
        if [ -n "$dest_rel" ]; then
            find "$source_abs" -path "$source_abs/$dest_rel" -prune -o -name "$f" -type f -print0
        else
            find "$source_abs" -name "$f" -type f -print0
        fi | while IFS= read -r -d '' src_file; do
            rel="${src_file#$source_abs/}"
            mkdir -p "$(dirname "$destination_abs/$rel")"
            $_cp_clone "$src_file" "$destination_abs/$rel" 2>/dev/null || \
                [ -f "$destination_abs/$rel" ] || \
                { echo "Error: failed to clone $rel" >&2; exit 1; }
        done
    done
else
    # Fall back to rsync when reflinks are not supported
    RSYNC_COPY_OPTS=(-a)
    if [ -n "$dest_rel" ]; then
        RSYNC_COPY_OPTS+=(--exclude="$dest_rel")
    fi
    INCLUDE_PATTERNS=()
    for f in "${MUTABLE_FILES[@]}"; do
        INCLUDE_PATTERNS+=(--include="$f")
    done
    rsync "${RSYNC_COPY_OPTS[@]}" \
        --include='*/' \
        "${INCLUDE_PATTERNS[@]}" \
        --exclude='*' \
        "$source_abs/" "$destination_abs/"
fi

echo "Mirror complete!"