// SPDX-License-Identifier: MIT
pragma solidity ^0.8.15;

contract MulMod {
    uint256 a = 100000000;

    function modx() public returns (uint256) {
        a = mulmod(2, 3, 4);

        return a;
    }
}