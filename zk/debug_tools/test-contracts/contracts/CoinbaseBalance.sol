// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract CoinbaseBalance {
    /// @notice Returns the balance of block.coinbase
    function getCoinbaseBalance() external view returns (uint256) {
        return block.coinbase.balance;
    }
}
