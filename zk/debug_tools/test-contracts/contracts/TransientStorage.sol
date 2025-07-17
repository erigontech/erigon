// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24; // 0.8.24+ required for EIP-1153 support

contract TransientStorage {
    /// @notice Stores a 32-byte value in transient storage at key=0x00,
    /// then immediately loads it back and returns it. If EIP-1153 is active,
    /// you'll see the same bytes you passed in. Otherwise the call will revert
    /// with invalid-opcode when hitting TSTORE or TLOAD.
    function writeThenReadTransient(bytes32 input) external returns (bytes32) {
        bytes32 loaded;
        assembly {
            // TSTORE: store `input` at key = 0
            // opcode 0x5d: TSTORE(key, value)
            mstore(0x00, input)        // put input on stack
            tstore(0x00, mload(0x00))  // store into transient storage slot 0x00

            // TLOAD: load from transient storage key = 0
            let temp := tload(0x00)    // retrieve from transient slot 0x00
            loaded := temp
        }
        return loaded;
    }
}
