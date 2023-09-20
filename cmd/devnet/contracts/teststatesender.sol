// SPDX-License-Identifier: GPL-3.0

pragma solidity ^0.8.2;

contract TestStateSender {

    uint256 public counter;
    mapping(address => address) public registrations;

    event NewRegistration(
        address indexed user,
        address indexed sender,
        address indexed receiver
    );
    event RegistrationUpdated(
        address indexed user,
        address indexed sender,
        address indexed receiver
    );
    event StateSynced(
        uint256 indexed id,
        address indexed contractAddress,
        bytes data
    );

    modifier onlyRegistered(address receiver) {
        //require(registrations[receiver] == msg.sender, "Invalid sender");
        _;
    }

    function syncState(address receiver, bytes calldata data)
        external
        onlyRegistered(receiver)
    {
        counter = counter = counter + 1;
        emit StateSynced(counter, receiver, data);
    }

    // register new contract for state sync
    function register(address sender, address receiver) public {
        registrations[receiver] = sender;
        if (registrations[receiver] == address(0)) {
            emit NewRegistration(msg.sender, sender, receiver);
        } else {
            emit RegistrationUpdated(msg.sender, sender, receiver);
        }
    }
}