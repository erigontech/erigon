// SPDX-License-Identifier: LGPL-3.0
pragma solidity >=0.8.0;

// solc --allow-paths ., --abi --bin --overwrite --optimize -o execution/state/contracts/build execution/state/contracts/storeanddie.sol
// ./build/bin/abigen -abi execution/state/contracts/build/StoreAndDie.abi -bin execution/state/contracts/build/StoreAndDie.bin -pkg contracts -type storeAndDie -out execution/state/contracts/gen_storeanddie.go
contract StoreAndDie {

    uint256 public slot;

    constructor() {
        slot = 5;
    }

    /* Writes the slot and self-destructs in the same transaction */
    function storeAndDie(uint256 v) external {
        slot = v;
        selfdestruct(payable(msg.sender));
    }
}
