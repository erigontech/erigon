// SPDX-License-Identifier: GPL-3.0

pragma solidity ^0.8.6;

import { TestStateSender } from "./teststatesender.sol";

contract RootSender {
  TestStateSender stateSender;
  address childStateReceiver;
   mapping(address => uint) public sent;

  constructor(
    address stateSender_,
    address childStateReceiver_
  ) {
    stateSender = TestStateSender(stateSender_);
    childStateReceiver = childStateReceiver_;
  }

  function sendToChild(uint amount) external {
    uint total = sent[msg.sender];
    sent[msg.sender] = total + amount;

    stateSender.syncState(
      childStateReceiver,
      abi.encode(msg.sender, amount)
    );
  }
}