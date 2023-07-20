// SPDX-License-Identifier: LGPL-3.0

pragma solidity ^0.8.6;

interface IStateReceiver {
  function onStateReceive(uint256 stateId, bytes calldata data) external;
}

contract ChildReceiver is IStateReceiver {
   mapping(address => uint) public received;
  
  constructor() {
  }

  function onStateReceive(uint, bytes calldata data) external override {
    require(msg.sender == address(0x0000000000000000000000000000000000001001), "Invalid sender");
    (address from, uint amount) = abi.decode(data, (address, uint));
    uint total = received[from];
    received[from] = total + amount;
  }
}
