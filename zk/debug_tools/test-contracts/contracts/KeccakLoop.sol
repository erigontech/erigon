pragma solidity >=0.8.10;

// 1616 for legacy, 226 for erigon -> 1198 -> 246
contract KeccakLoop {
    constructor () {
        for(uint256 i = 0; i < 200; i++) {
            keccak256(new bytes(i));
        }
    }
}