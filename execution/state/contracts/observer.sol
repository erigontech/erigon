// SPDX-License-Identifier: LGPL-3.0
pragma solidity >=0.8.0;

// solc --allow-paths ., --abi --bin --overwrite --optimize -o execution/state/contracts/build execution/state/contracts/observer.sol
// ./build/bin/abigen -abi execution/state/contracts/build/Observer.abi -bin execution/state/contracts/build/Observer.bin -pkg contracts -type observer -out execution/state/contracts/gen_observer.go
contract Observer {

    uint256 public codeSize;
    bytes32 public codeHash;
    uint256 public targetBalance;

    /* Persists the deploy-time view of the target account into storage */
    constructor(address target) {
        uint256 size;
        bytes32 hash;
        assembly {
            size := extcodesize(target)
            hash := extcodehash(target)
        }
        codeSize = size;
        codeHash = hash;
        targetBalance = target.balance;
    }
}
