#!/bin/bash

set -e
set -o pipefail

TARGET="silkworm_capi"
script_dir=$(dirname "${BASH_SOURCE[0]}")
project_dir=$(realpath "$script_dir/../..")

src_dir="$1"
build_dir="$2"

if [[ ! -d "$src_dir" ]]
then
    echo "source directory '$src_dir' not found"
    exit 1
fi

if [[ -z "$build_dir" ]]
then
    build_dir="$src_dir/build"
fi

if [[ ! -d "$build_dir" ]]
then
    echo "build directory '$build_dir' not found"
    exit 1
fi

replace_dir=$(mktemp -d -t silkworm-go 2> /dev/null || mktemp -d -t silkworm-go.XXXXXXXX)

git clone --depth 1 "https://github.com/erigontech/silkworm-go" "$replace_dir"

ln -s "$src_dir/silkworm/capi/silkworm.h" "$replace_dir/include/"

product_dir="$build_dir/silkworm/capi"
product_path=$(echo "$product_dir/"*$TARGET*)
product_file_name=$(basename "$product_path")

for platform in macos_arm64 macos_x64 linux_arm64 linux_x64
do
    mkdir "$replace_dir/lib/$platform"
    ln -s "$product_path" "$replace_dir/lib/$platform/$product_file_name"
done

cd "$project_dir/.."
rm -f "go.work"
go work init "$project_dir"
go work use "$replace_dir"
