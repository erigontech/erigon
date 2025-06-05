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

echo "Syncing symlinks from '$source' to '$destination'"

# Create the destination directory if it doesn't exist
mkdir -p "$destination"

# First create the directory structure
echo "Creating directory structure..."
find "$source" -type d | sed "s|^$source|$destination|" | xargs -I{} mkdir -p {}

# Then create symbolic links for all files (or copy specific ones)
echo "Creating symbolic links and copying special files..."
find "$source" -type f | while read file; do
    rel_path="${file#$source}"
    filename=$(basename "$file")
    
    # Skip erigon.log entirely
    if [ "$filename" = "erigon.log" ]; then
        echo "Skipping: $file"
        continue
    fi
    
    # Copy these files instead of symlinking
    if [ "$filename" = "mdbx.dat" ] || [ "$filename" = "mdbx.lck" ] || [ "$filename" = "jwt.hex" ] || [ "$filename" = "LOCK" ] || [ "$filename" = "prohibit_new_downloads.lock" ] || [ "$filename" = "nodekey" ]; then
        echo "Copying: $file"
        cp "$file" "$destination$rel_path"
    else
        # Symlink all other files
        ln -sf "$file" "$destination$rel_path"
    fi
done

# Remove files in destination that don't exist in source (except erigon.log which we skip)
echo "Cleaning up orphaned links..."
find "$destination" -type f -o -type l | while read file; do
    rel_path="${file#$destination}"
    filename=$(basename "$file")
    
    # Skip erigon.log cleanup since we don't sync it
    if [ "$filename" = "erigon.log" ]; then
        continue
    fi
    
    if [ ! -e "$source$rel_path" ]; then
        echo "Removing orphaned: $file"
        rm "$file"
    fi
done

echo "Sync complete!"