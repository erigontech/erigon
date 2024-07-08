// SPDX-License-Identifier: GPL-3.0

pragma solidity >=0.7.0 <0.9.0;

contract Counter {
    uint public count;
    
    function increment() external {
        count += 1;
    }

    function getCount() public view returns (uint) {
        return count;
    }
}