#!/bin/bash
set -e

dir=$1
files=$(ls $dir/snapshots | grep -v old | grep -v tor | grep seg |  sort -n -t$'-' -k1)
for file in $files
do
   from=$dir/snapshots/$file
   to=$dir/snapshots/$file.new
   echo "file: $file"

   ./build/bin/erigon_old snapshots uncompress $from | ./build/bin/erigon snapshots compress $to --datadir=$dir
   a=$(du -h $from | awk '{print $1;}')
   b=$(du -h $to | awk '{print $1;}')
   echo "size: $a -> $b"
   mv $from $from.old
   mv $from.new $from
   
done


