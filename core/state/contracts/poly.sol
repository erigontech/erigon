// SPDX-License-Identifier: LGPL-3.0
pragma solidity >=0.5.0;

// solc --allow-paths ., --abi --bin --overwrite --optimize -o core/state/contracts/build core/state/contracts/poly.sol
// ./build/bin/abigen -abi core/state/contracts/build/Poly.abi -bin core/state/contracts/build/Poly.bin -pkg contracts -type poly -out core/state/contracts/gen_poly.go
contract Poly {

    constructor() public {
    }

    event DeployEvent (address d);

    /* Deploys self-destructing contract with given salt and emits DeployEvent with the address of the created contract */
    function deploy(uint256 salt) public {
        // PUSH1 0x60; PUSH1 0; MSTORE8; NUMBER; PUSH1 1; MSTORE8; PUSH1 0xff; PUSH1 2; MSTORE8; PUSH1 3; PUSH1 0; RETURN;
        // Returns code 60<N>ff, which is PUSH1 <N>; SELFDESTRUCT. Value <N> is determined by the block number where deploy function is called
        bytes memory init_code = hex"60606000534360015360ff60025360036000f3";
        address payable d;
        assembly{
            d := create2(0, add(init_code, 32), mload(init_code), salt)
        }
        emit DeployEvent(d);
    }
}
