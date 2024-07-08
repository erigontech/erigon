// SPDX-License-Identifier: MIT
pragma solidity ^0.8.15;

contract EmitLog {
    event Log();
    event LogA(uint256 indexed a);
    event LogAB(uint256 indexed a, uint256 indexed b);
    event LogABC(uint256 indexed a, uint256 indexed b, uint256 indexed c);
    event LogABCD(uint256 indexed a, uint256 indexed b, uint256 indexed c, uint256 d);

    function emitLogs() public {
        emit Log();
        emit LogA(1);
        emit LogAB(1, 2);
        emit LogABC(1, 2, 3);
        emit LogABCD(1, 2, 3, 4);
        emit LogABCD(4, 3, 2, 1);
        emit LogABC(3, 2, 1);
        emit LogAB(2, 1);
        emit LogA(1);
        emit Log();
    }
}