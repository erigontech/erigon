#!/bin/bash

## create backup of a datadir for experiments/debugging etc.
## it creates copy of editable files like mdbx.dat
## and symlinks to immutable files like snapshots

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

# Convert to absolute paths for comparison
source_abs=$(realpath "$source")
destination_abs=$(realpath -m "$destination")  # -m allows non-existent paths

# Check if destination is a subdirectory of source
if [[ "$destination_abs" == "$source_abs"/* ]]; then
    echo "Note: Destination is a subdirectory of source, will be excluded from operations"
    # Calculate the relative path of destination within source for exclusion
    dest_rel="${destination_abs#$source_abs/}"
fi

# Determine optimal number of parallel jobs (default to number of CPU cores)
num_jobs=$(nproc 2>/dev/null || echo 4)

echo "Syncing files from '$source' to '$destination' using $num_jobs parallel jobs"

# Create the destination directory if it doesn't exist
mkdir -p "$destination"

# First create the directory structure, excluding destination if it's a subfolder
echo "Creating directory structure..."
if [ -n "$dest_rel" ]; then
    find "$source" -type d -path "$source/$dest_rel" -prune -o -type d -print | \
        sed "s|^$source|$destination|" | xargs -I{} mkdir -p {}
else
    find "$source" -type d | sed "s|^$source|$destination|" | xargs -I{} mkdir -p {}
fi

# Function to process individual files
process_file() {
    local file="$1"
    local source="$2"
    local destination="$3"
    
    rel_path="${file#$source}"
    filename=$(basename "$file")
    
    # Skip these files entirely
    if [ "$filename" = "erigon.log" ] || [ "$filename" = ".DS_Store" ]; then
        echo "Skipping: $file"
        return
    fi
    
    # Copy these files instead of symlinking
    if [ "$filename" = "mdbx.dat" ] || \
       [ "$filename" = "mdbx.lck" ] || \
       [ "$filename" = "jwt.hex" ] || \
       [ "$filename" = "LOCK" ] || \
       [ "$filename" = "prohibit_new_downloads.lock" ] || \
       [ "$filename" = "nodekey" ]; then
        if cmp -s "$file" "$destination$rel_path" 2>/dev/null; then
            echo "Already up-to-date: $file"
        else
            echo "Copying: $file"
            cp "$file" "$destination$rel_path" 2>/dev/null || {
                echo "Copying: $file"
                cp "$file" "$destination$rel_path"
            }
        fi
    else
        # Symlink all other files
        if [ "$(readlink "$destination$rel_path" 2>/dev/null)" != "$file" ]; then
            echo "Linking: $file"
            ln -f "$file" "$destination$rel_path"
        fi
    fi
}

# Export function for parallel execution
export -f process_file

# Process files in parallel, excluding destination directory if it's a subfolder
echo "Creating symbolic links and copying files..."
if [ -n "$dest_rel" ]; then
    find "$source" -path "$source/$dest_rel" -prune -o -type f -print | \
        xargs -I{} -P"$num_jobs" bash -c 'process_file "$1" "$2" "$3"' _ {} "$source" "$destination"
else
    find "$source" -type f | \
        xargs -I{} -P"$num_jobs" bash -c 'process_file "$1" "$2" "$3"' _ {} "$source" "$destination"
fi

# Function to clean orphaned files
cleanup_file() {
    local file="$1"
    local source="$2"
    local destination="$3"
    
    rel_path="${file#$destination}"
    filename=$(basename "$file")
    
    # Skip these files in cleanup since we don't sync them
    if [ "$filename" = "erigon.log" ] || [ "$filename" = ".DS_Store" ]; then
        return
    fi
    
    if [ ! -e "$source$rel_path" ]; then
        echo "Removing orphaned: $file"
        rm "$file"
    fi
}

# Export cleanup function for parallel execution
export -f cleanup_file

# Remove files in destination that don't exist in source (except erigon.log which we skip)
echo "Cleaning up orphaned links..."
find "$destination" -type f -o -type l | \
    xargs -I{} -P"$num_jobs" bash -c 'cleanup_file "$1" "$2" "$3"' _ {} "$source" "$destination"

echo "Sync complete!"