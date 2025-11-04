pragma solidity >=0.5.0;
contract testcontract {
    mapping(address => uint) public balances;

    constructor() public {
        balances[msg.sender] = 100;
    }

    function create(uint newBalance) public {
        balances[msg.sender] = newBalance;
    }

    function update(uint newBalance) public {
        balances[msg.sender] = newBalance;
    }

    function remove() public {
        delete balances[msg.sender];
    }

    function createAndRevert(uint newBalance) public {
        balances[msg.sender] = newBalance;
        revert();
    }

    function updateAndRevert(uint newBalance) public {
        balances[msg.sender] = newBalance;
        revert();
    }

    function removeAndRevert() public {
        delete balances[msg.sender];
        revert();
    }

    function createAndException(uint newBalance) public {
        balances[msg.sender] = newBalance;
        assert(false);
    }

    function updateAndException(uint newBalance) public {
        balances[msg.sender] = newBalance;
        assert(false);
    }

    function removeAndException() public {
        delete balances[msg.sender];
        assert(false);
    }
}