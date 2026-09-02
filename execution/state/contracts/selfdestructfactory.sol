// SPDX-License-Identifier: LGPL-3.0
pragma solidity >=0.7.0;

// solc --allow-paths ., --abi --bin --overwrite --optimize -o execution/state/contracts/build execution/state/contracts/selfdestructfactory.sol
// ./build/bin/abigen -abi execution/state/contracts/build/SelfDestructInConstructor.abi -bin execution/state/contracts/build/SelfDestructInConstructor.bin -pkg contracts -type selfDestructInConstructor -out execution/state/contracts/gen_selfdestructinconstructor.go
// ./build/bin/abigen -abi execution/state/contracts/build/SelfDestructFactory.abi -bin execution/state/contracts/build/SelfDestructFactory.bin -pkg contracts -type selfDestructFactory -out execution/state/contracts/gen_selfdestructfactory.go

// SelfDestructInConstructor self-destructs to itself while still constructing,
// so post-EIP-8246 it leaves a balance-only account behind. It carries no
// constructor arguments, keeping its CREATE2 init code deterministic so the
// factory can deploy to the same address again (EIP-7610 permits creation over
// an account with no code, nonce or storage).
contract SelfDestructInConstructor {
    constructor() payable {
        selfdestruct(payable(address(this)));
    }
}

// SelfDestructFactory CREATE2-deploys SelfDestructInConstructor under a
// caller-supplied salt, forwarding the call value into the creation.
contract SelfDestructFactory {
    function deploy(bytes32 salt) public payable returns (SelfDestructInConstructor instance) {
        instance = new SelfDestructInConstructor{salt: salt, value: msg.value}();
    }
}
