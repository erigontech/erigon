pragma solidity >=0.6.0;

// solc --allow-paths ., --abi --bin --overwrite --optimize -o execution/state/contracts/build execution/state/contracts/selfdestruct.sol
// ./build/bin/abigen -abi execution/state/contracts/build/Selfdestruct.abi -bin execution/state/contracts/build/Selfdestruct.bin -pkg contracts -type selfdestruct -out execution/state/contracts/gen_selfdestruct.go
contract Selfdestruct {

    uint256 x;
    uint256 y;
    uint256 z;

    constructor() public {
        // Fill some storage positions
        x = 1 << 32; // Large number to make sure encoding has multiple bytes
        y = 2;
        z = 3;
    }

    function change() external {
        x += 1;
        y += 1;
        z += 1;
    }

    receive() external payable {
    }


    /* Self-destructs */
    function destruct() public {
        selfdestruct(payable(this));
    }
}
