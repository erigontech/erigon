// SPDX-License-Identifier: GPL-3.0
pragma solidity >=0.7.0 <0.9.0;

contract ChainCallLevel4 {
    receive() external payable {}

    address sender;
    uint256 value;

    function exec() public payable {
        sender = msg.sender;
        value = msg.value;
    }

    function execRevert() public payable {
        require(false, "ahoy, this tx will always revert");
    }

    function get() public pure returns (string memory t) {
        return "ahoy";
    }
}