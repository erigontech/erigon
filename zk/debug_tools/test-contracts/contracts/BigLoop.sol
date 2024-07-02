// SPDX-License-Identifier: MIT
pragma solidity ^0.8.15;

contract BigLoop {
      //dynamic array
      uint[] a = new uint[](12000);

    function bigLoop(uint count) public  {
        for (uint i = 0; i < count; i++) {
            a[i%10000] = i;
        }
    }
}