// SPDX-License-Identifier: LGPL-3.0
pragma solidity ^0.8.20;

error InsufficientFee();

interface ISequencer {
    function submitEncryptedTransaction(
        uint64 eon,
        bytes32 identityPrefix,
        bytes memory encryptedTransaction,
        uint256 gasLimit
    ) external payable;

    function submitDecryptionProgress(bytes memory message) external;

    event TransactionSubmitted(
        uint64 eon,
        uint64 txIndex,
        bytes32 identityPrefix,
        address sender,
        bytes encryptedTransaction,
        uint256 gasLimit
    );
    event DecryptionProgressSubmitted(bytes message);
}
