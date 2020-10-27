pragma solidity >=0.6.0;

// solc --allow-paths ., --abi --bin --overwrite --optimize -o cmd/tester/contracts/build cmd/tester/contracts/revive2.sol
// abigen -abi cmd/tester/contracts/build/Revive2.abi -bin cmd/tester/contracts/build/Revive2.bin -pkg contracts -type revive2 -out cmd/tester/contracts/gen_revive2.go
// abigen -abi cmd/tester/contracts/build/Phoenix.abi -bin cmd/tester/contracts/build/Phoenix.bin -pkg contracts -type phoenix -out cmd/tester/contracts/gen_phoenix.go
contract Revive2 {

    constructor() public {
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
    uint256 sstoreIndex;
    uint256 sloadIndex;
    mapping(uint256=>uint256) data;

    function store() public {
        data[sstoreIndex] = 1;
        sstoreIndex++;
    }

    function increment() public {
        require(sloadIndex <= sstoreIndex, "try to increment not created storage");
        data[sloadIndex] = data[sloadIndex] + 1;
        sloadIndex++;
    }

    constructor() public {
    }


    receive() external payable {
    }

    function die() public {
        selfdestruct(address(0));
    }
}
