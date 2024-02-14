#!/bin/bash

set -e
set -o pipefail

TARGET="silkworm_capi"
script_dir=$(dirname "${BASH_SOURCE[0]}")
project_dir=$(realpath "$script_dir/../..")

silkworm_dir="$1"
silkworm_build_dir="$2"
silkworm_go_dir="$3"

if [[ ! -d "$silkworm_dir" ]]
then
    echo "silkworm source directory '$silkworm_dir' not found"
    exit 1
fi

if [[ -z "$silkworm_build_dir" ]]
then
    silkworm_build_dir="$silkworm_dir/build"
fi

if [[ ! -d "$silkworm_build_dir" ]]
then
    echo "silkworm build directory '$silkworm_build_dir' not found"
    exit 1
fi

if [[ -z "$silkworm_go_dir" ]]
then
    silkworm_go_dir=$(mktemp -d -t silkworm-go 2> /dev/null || mktemp -d -t silkworm-go.XXXXXXXX)
    git clone --depth 1 "https://github.com/erigontech/silkworm-go" "$silkworm_go_dir"
fi

if [[ ! -d "$silkworm_go_dir" ]]
then
    echo "silkworm-go directory '$silkworm_go_dir' not found"
    exit 1
fi

ln -s "$silkworm_dir/silkworm/capi/silkworm.h" "$silkworm_go_dir/include/"

product_dir="$silkworm_build_dir/silkworm/capi"
product_path=$(echo "$product_dir/"*$TARGET*)
product_file_name=$(basename "$product_path")

for platform in macos_arm64 macos_x64 linux_arm64 linux_x64
do
    mkdir "$silkworm_go_dir/lib/$platform"
    ln -s "$product_path" "$silkworm_go_dir/lib/$platform/$product_file_name"
done

cd "$project_dir/.."
rm -f "go.work"
go work init "$project_dir"
go work use "$silkworm_go_dir"
