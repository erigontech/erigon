pragma solidity >=0.6.0;

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
        selfdestruct(payable(0));
    }
}
