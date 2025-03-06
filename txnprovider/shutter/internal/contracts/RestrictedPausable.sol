// SPDX-License-Identifier: LGPL-3.0
pragma solidity ^0.8.22;

import "openzeppelin/contracts/access/AccessControl.sol";
import "openzeppelin/contracts/utils/Pausable.sol";

error UnauthorizedInitializer();
error AlreadyInitialized();

abstract contract RestrictedPausable is AccessControl, Pausable {
    bytes32 public constant PAUSER_ROLE = keccak256("PAUSER_ROLE");

    address public initializer;

    modifier onlyInitializer() {
        if (initializer == address(0)) {
            revert AlreadyInitialized();
        }

        if (msg.sender != initializer) {
            revert UnauthorizedInitializer();
        }
        _;
    }

    constructor(address _initializer) {
        initializer = _initializer;
    }

    function initialize(
        address admin,
        address pauser
    ) public virtual onlyInitializer {
        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(PAUSER_ROLE, pauser);
        initializer = address(0);
    }

    function pause() external onlyRole(PAUSER_ROLE) {
        _pause();
    }

    function unpause() external onlyRole(DEFAULT_ADMIN_ROLE) {
        _unpause();
    }
}
