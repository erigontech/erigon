#!/bin/bash 

set -Eeuxoa pipefail

make
make hack

if [ "$#" -eq 1 ]; then
  ./build/bin/hack --action cfg $1
elif [ "$#" -eq 0  ]; then
  ./build/bin/hack --action cfg
else
  echo "invalid args"
fi

