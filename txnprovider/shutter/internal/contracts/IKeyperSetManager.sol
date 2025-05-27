// SPDX-License-Identifier: LGPL-3.0
pragma solidity ^0.8.20;

error KeyperSetNotFinalized();
error AlreadyHaveKeyperSet();
error NoActiveKeyperSet();

interface IKeyperSetManager {
    function addKeyperSet(
        uint64 activationSlot,
        address keyperSetContract
    ) external;

    function getNumKeyperSets() external view returns (uint64);

    function getKeyperSetIndexByBlock(
        uint64 slot
    ) external view returns (uint64);

    function getKeyperSetAddress(uint64 index) external view returns (address);

    function getKeyperSetActivationBlock(
        uint64 index
    ) external view returns (uint64);

    event KeyperSetAdded(uint64 activationSlot, address keyperSetContract);
}
