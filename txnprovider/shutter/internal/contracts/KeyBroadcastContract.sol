// SPDX-License-Identifier: MIT
pragma solidity ^0.8.22;

import "./KeyperSetManager.sol";
import "./KeyperSet.sol";

error InvalidKey();
error NotAllowed();
error AlreadyHaveKey();

interface EonKeyPublisher {
    function eonKeyConfirmed(bytes memory eonKey) external returns (bool);
}

contract KeyBroadcastContract {
    mapping(uint64 => bytes) private keys;
    KeyperSetManager private keyperSetManager;

    event EonKeyBroadcast(uint64 eon, bytes key);

    constructor(address keyperSetManagerAddress) {
        keyperSetManager = KeyperSetManager(keyperSetManagerAddress);
    }

    function broadcastEonKey(uint64 eon, bytes memory key) external {
        if (key.length == 0) {
            revert InvalidKey();
        }
        if (keys[eon].length > 0) {
            revert AlreadyHaveKey();
        }
        KeyperSet keyperSet = KeyperSet(
            keyperSetManager.getKeyperSetAddress(eon)
        );
        if (!keyperSet.isAllowedToBroadcastEonKey(msg.sender)) {
            revert NotAllowed();
        }

        keys[eon] = key;
        emit EonKeyBroadcast(eon, key);
    }

    function getEonKey(uint64 eon) external view returns (bytes memory) {
        return keys[eon];
    }
}
