// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract Basefee {
    /// @notice Directly returns the BASEFEE opcode result
    function getBasefee() external view returns (uint256) {
        assembly {
            // BASEFEE (0x48) pushes the current block’s baseFeePerGas
            let bf := basefee()
            mstore(0x00, bf)
            return(0x00, 0x20)
        }
    }
}
