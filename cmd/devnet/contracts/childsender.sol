// SPDX-License-Identifier: LGPL-3.0

pragma solidity ^0.8.6;

contract ChildSender  {
    address rootStateReceiver;
    mapping(address => uint) public sent;

    // MessageTunnel on L1 will get data from this event
    event MessageSent(bytes message);

    constructor(address childStateReceiver_) {
        rootStateReceiver = childStateReceiver_;
    }

    function _sendMessageToRoot(bytes memory message) internal {
        emit MessageSent(message);
    }

    function sendToRoot(uint amount) external {
        uint total = sent[msg.sender];
        sent[msg.sender] = total + amount;

        _sendMessageToRoot(
            abi.encode(rootStateReceiver, msg.sender, amount)
        );
  }
}
