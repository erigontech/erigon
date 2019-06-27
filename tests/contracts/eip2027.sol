pragma solidity ^0.5.0;
contract eip2027 {
    mapping(address => uint) public balances;

    function create() public {
        balances[msg.sender] = 1;
    }

    function update(uint newBalance) public {
        balances[msg.sender] = newBalance;
    }

    function remove() public {
        delete balances[msg.sender];
    }
}