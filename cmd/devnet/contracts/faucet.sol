// SPDX-License-Identifier: LGPL-3.0

pragma solidity ^0.8.0;

contract faucet {
	mapping (address => uint256) public sources;
	mapping (address => uint256) public destinations;

	constructor() {}

	event sent(address _destination, uint256 _amount);
	event received(address _source, uint256 _amount);

	receive() external payable
	{
        sources[msg.sender] += msg.value;
		emit received(msg.sender, msg.value);
	}

    function send(address payable _destination, uint256 _requested) public payable
    {
        if (address(this).balance == 0) {
            return;
        }

        uint256 amount = 0;
        
        if (address(this).balance > _requested){
            amount = _requested;
            _destination.transfer(_requested);   
        }
        else{
            amount = address(this).balance;
            _destination.transfer(amount);
        }
        
        destinations[_destination] += amount;
        emit sent(_destination, amount);
    }
}