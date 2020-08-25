#!/bin/bash

set -Eeuxoa pipefail

make
make hack
bin=$(solc --combined-json bin-runtime deposit_contract.sol| sed 's/deposit_contract.sol:DepositContract/depositcontract/g' | sed 's/bin-runtime/bin/g' | jq '.contracts.depositcontract.bin' | sed 's/\"//g')
./build/bin/hack --action cfg $bin
dot -Tpng cfg.dot > cfg.png && open cfg.png
