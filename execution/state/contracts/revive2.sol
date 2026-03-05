pragma solidity >=0.6.0;

// solc --allow-paths ., --abi --bin --overwrite --optimize -o execution/state/contracts/build execution/state/contracts/revive2.sol
// ./build/bin/abigen -abi execution/state/contracts/build/Revive2.abi -bin execution/state/contracts/build/Revive2.bin -pkg contracts -type revive2 -out execution/state/contracts/gen_revive2.go
// ./build/bin/abigen -abi execution/state/contracts/build/Phoenix.abi -bin execution/state/contracts/build/Phoenix.bin -pkg contracts -type phoenix -out execution/state/contracts/gen_phoenix.go
contract Revive2 {

    constructor() {
    }

    event DeployEvent (Phoenix d);

    /* Deploys self-destructing contract with given salt and emits DeployEvent with the address of the created contract */
    function deploy(bytes32 salt) public {
        Phoenix d;
        d = new Phoenix{salt: salt}();
        emit DeployEvent(d);
    }
}

contract Phoenix {
    uint256 d;

    function increment() public {
        d++;
    }

    constructor() {
    }


    receive() external payable {
    }

    function die() public {
        address payable nil = payable(0);
        selfdestruct(nil);
    }
}
