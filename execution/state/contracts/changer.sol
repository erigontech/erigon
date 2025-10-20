pragma solidity >=0.5.0;

// solc --allow-paths ., --abi --bin --overwrite --optimize -o execution/state/contracts/build execution/state/contracts/changer.sol
// abigen -abi execution/state/contracts/build/Changer.abi -bin execution/state/contracts/build/Changer.bin -pkg contracts -type changer -out execution/state/contracts/gen_changer.go
contract Changer {

    uint256 x;
    uint256 y;
    uint256 z;

    constructor() public {
    }

    function change() external {
        x = 1;
        y = 2;
        z = 3;
    }
}
