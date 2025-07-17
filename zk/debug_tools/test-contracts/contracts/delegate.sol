// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

contract Delegate {
    event FallbackCalled(address indexed sender, uint256 value, bytes data);

    fallback() external payable {
        emit FallbackCalled(msg.sender, msg.value, msg.data);
    }

    receive() external payable {
        emit FallbackCalled(msg.sender, msg.value, "");
    }

    uint256 private _stored;

    function setStored(uint256 x) external {
        _stored = x;
    }

    function stored() external view returns (uint256) {
        return _stored;
    }
}
