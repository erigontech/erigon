pragma solidity >=0.5.0;

// solc --allow-paths ., --abi --bin --overwrite --optimize -o core/state/contracts/build core/state/contracts/revive.sol
// ./build/bin/abigen -abi core/state/contracts/build/Revive.abi -bin core/state/contracts/build/Revive.bin -pkg contracts -type revive -out core/state/contracts/gen_revive.go
contract Revive {

    constructor() public {
    }

    event DeployEvent (address d);

    /* Deploys self-destructing contract with given salt and emits DeployEvent with the address of the created contract */
    function deploy(uint256 salt) public {
        // PUSH1 0x42; NUMBER; SSTORE; PUSH1 0x30; PUSH1 0; MSTORE8; PUSH1 0xff; PUSH1 1; MSTORE8; PUSH1 2; PUSH1 0; RETURN;
        // Stores constant 0x42 in the storage position equal to the current block number, then returns code 30ff, which is
        // ADDRESS; SELFDESTRUCT 
        bytes memory init_code = hex"60424355603060005360ff60015360026000f3";
        address payable d;
        assembly{
            d := create2(0, add(init_code, 32), mload(init_code), salt)
        }
        emit DeployEvent(d);
    }
}
