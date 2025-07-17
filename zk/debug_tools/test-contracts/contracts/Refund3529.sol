// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/// @notice A tiny contract to demonstrate SSTORE refunds.
///  - setVal(uint256) writes `val` from 0→x (no refund).
///  - clearVal() writes `val` from x→0 (refund should be 4 k under EIP-3529,
///    instead of 15 k pre-Berlin).
contract Refund {
    uint256 public val;

    /// @notice Set `val = x`.  (If val was zero before, no refund occurs.)
    function setVal(uint256 x) external {
        val = x;
    }

    /// @notice Clear `val` back to zero.
    /// Under pre-EIP-3529, this would refund 15 000 gas; after EIP-3529, it only refunds 4 000.
    function clearVal() external {
        val = 0;
    }
}
