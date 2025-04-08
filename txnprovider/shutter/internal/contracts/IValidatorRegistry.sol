// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

interface IValidatorRegistry {
    struct Update {
        bytes message;
        bytes signature;
    }

    function getNumUpdates() external view returns (uint256);

    function getUpdate(uint256 i) external view returns (Update memory);

    function update(bytes memory message, bytes memory signature) external;

    event Updated(bytes message, bytes signature);
}
