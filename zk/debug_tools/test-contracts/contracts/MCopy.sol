// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20; // MCOPY introduced around 0.8.20+

contract MinimalMCopy {
    /// @notice Returns a new bytes array that is an exact copy of `input`,
    /// using the MCOPY opcode. If MCOPY is unsupported, this call will revert.
    function copy(bytes memory input) external pure returns (bytes memory output) {
        uint256 len = input.length;
        // Allocate a fresh bytes array of the same length
        output = new bytes(len);

        assembly {
            // In memory, a `bytes` pointer is:
            //   [0x00 .. 0x1f] = length
            //   [0x20 .. 0x20+len-1] = data
            //
            // We want pointers to the data region (skipping the 32-byte length)
            let srcPtr := add(input, 0x20)
            let dstPtr := add(output, 0x20)
            // Invoke MCOPY(dstPtr, srcPtr, len)
            mcopy(dstPtr, srcPtr, len)
        }
        // If MCOPY is not supported, the EVM will hit an invalid‐opcode and revert.
        // Otherwise, `output` now holds exactly the same bytes as `input`.
    }
}
