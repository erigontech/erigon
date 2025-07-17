// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract SelfDestruct {
    function kill(address payable recipient) public {
        selfdestruct(recipient);
    }

    receive() external payable {}
}
