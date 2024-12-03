// SPDX-License-Identifier: MIT
pragma solidity ^0.8.15;

contract GasBurner {
    constructor() {
      //dynamic array
      uint[] memory a = new uint[](12000);
        for (uint i = 0; i < 2000; i++) {
            a[i%10000] = i;
        }
    }
}