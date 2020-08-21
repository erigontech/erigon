#!/bin/bash

set -Eeuxoa pipefail

make && make hack && ./build/bin/hack --action cfg

rm -f cfg.png
dot -Tpng cfg.dot > cfg.png && open cfg.png
