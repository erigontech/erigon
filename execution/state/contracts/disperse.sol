// SPDX-License-Identifier: LGPL-3.0
pragma solidity >=0.7.0;

// solc --allow-paths ., --abi --bin --overwrite --optimize -o execution/state/contracts/build execution/state/contracts/disperse.sol
// ./build/bin/abigen -abi execution/state/contracts/build/Disperse.abi -bin execution/state/contracts/build/Disperse.bin -pkg contracts -type disperse -out execution/state/contracts/gen_disperse.go

// Disperse fans ether out to many recipients in a single call, crediting
// addresses that may still be empty (mirrors the on-chain "disperse" pattern).
contract Disperse {
    function disperseEther(address[] calldata recipients, uint256[] calldata values) external payable {
        for (uint256 i = 0; i < recipients.length; i++) {
            payable(recipients[i]).transfer(values[i]);
        }
        uint256 remaining = address(this).balance;
        if (remaining > 0) {
            payable(msg.sender).transfer(remaining);
        }
    }
}
