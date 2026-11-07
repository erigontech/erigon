// SPDX-License-Identifier: LGPL-3.0
pragma solidity >=0.8.0;

// solc --allow-paths ., --abi --bin --overwrite --optimize -o execution/state/contracts/build execution/state/contracts/statechurn.sol
// ./build/bin/abigen -abi execution/state/contracts/build/StateChurn.abi -bin execution/state/contracts/build/StateChurn.bin -pkg contracts -type stateChurn -out execution/state/contracts/gen_statechurn.go

// StateChurn maintains a fixed ring of pseudo-random storage keys. Each poke
// overwrites the next key in round-robin with a pseudo-random value in [0, 3),
// so a third of the writes set a slot to 0 (which deletes it) and keys cycle
// through delete then recreate - the storage pattern that exposes buggy unwinds.
// trackedSum is maintained incrementally and must always equal the sum
// recomputed from storage; poke reverts otherwise, turning any state corruption
// left behind by an incorrect unwind/reorg into a failed transaction.
contract StateChurn {
    error CreateReverted();
    uint256 private constant N = 16;

    mapping(uint256 => uint256) private values;
    uint256[N] private keys;
    uint256 public cursor;
    uint256 public trackedSum;

    constructor() {
        for (uint256 i = 0; i < N; i++) {
            keys[i] = uint256(keccak256(abi.encodePacked("statechurn", i)));
        }
    }

    function poke(uint256 seed) external {
        uint256 key = keys[cursor % N];
        uint256 newVal = uint256(keccak256(abi.encodePacked(seed, cursor))) % 3;
        trackedSum = trackedSum - values[key] + newVal;
        values[key] = newVal;
        cursor++;
        require(computedSum() == trackedSum, "StateChurn: sum mismatch");
        checkRevertedCreate();
    }

    function createAndRevert() external {
        require(msg.sender == address(this));
        StateChurnChild child = new StateChurnChild{salt: bytes32(0)}();
        require(address(child).code.length > 0);
        revert CreateReverted();
    }

    function checkRevertedCreate() private {
        address child = address(uint160(uint256(keccak256(abi.encodePacked(
            bytes1(0xff), address(this), bytes32(0), keccak256(type(StateChurnChild).creationCode)
        )))));
        try this.createAndRevert() {
            revert("StateChurn: expected revert");
        } catch (bytes memory reason) {
            require(keccak256(reason) == keccak256(abi.encodeWithSelector(CreateReverted.selector)),
                "StateChurn: creation failed");
        }
        // The reverted frame must also discard cached code size, not just code.
        require(child.code.length == 0, "StateChurn: reverted code remains");
    }

    function computedSum() public view returns (uint256 sum) {
        for (uint256 i = 0; i < N; i++) {
            sum += values[keys[i]];
        }
    }

    function check() external view returns (bool) {
        return computedSum() == trackedSum;
    }
}

contract StateChurnChild {
    function value() external pure returns (uint256) { return 1; }
}
