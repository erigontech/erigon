pragma solidity ^0.5.0;

contract Storage {
    uint value = 42;
    
    function set(uint x) public {
        value = x;
    }
}
