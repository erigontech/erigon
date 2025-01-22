// SPDX-License-Identifier: LGPL-3.0
pragma solidity ^0.8.20;

import "./ISequencer.sol";

contract Sequencer is ISequencer {
    mapping(uint64 eon => uint64 txCount) private txCounters;

    function submitEncryptedTransaction(
        uint64 eon,
        bytes32 identityPrefix,
        bytes memory encryptedTransaction,
        uint256 gasLimit
    ) external payable {
        if (msg.value < block.basefee * gasLimit) {
            revert InsufficientFee();
        }

        uint64 index = txCounters[eon];
        txCounters[eon] = index + 1;

        emit TransactionSubmitted(
            eon,
            index,
            identityPrefix,
            msg.sender,
            encryptedTransaction,
            gasLimit
        );
    }

    function submitDecryptionProgress(bytes memory message) external {
        emit DecryptionProgressSubmitted(message);
    }

    function getTxCountForEon(uint64 eon) external view returns (uint64) {
        return txCounters[eon];
    }
}
