// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "IValidatorRegistry.sol";

contract ValidatorRegistry is IValidatorRegistry {
    Update[] updates;

    function update(bytes memory message, bytes memory signature) external {
        updates.push(Update({message: message, signature: signature}));
        emit Updated(message, signature);
    }

    function getNumUpdates() external view returns (uint256) {
        return updates.length;
    }

    function getUpdate(uint256 i) external view returns (Update memory) {
        return updates[i];
    }
}
