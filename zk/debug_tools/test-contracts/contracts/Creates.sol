// SPDX-License-Identifier: GPL-3.0

pragma solidity >=0.7.0 <0.9.0;

contract Creates {
    function opCreate(bytes memory bytecode, uint length) public returns(address) {
        address addr;
        assembly {
            addr := create(0, 0xa0, length)
            sstore(0x0, addr)
        }
        return addr;
    }

    function opCreate2(bytes memory bytecode, uint length) public returns(address) {
        address addr;
        assembly {
            addr := create2(0, 0xa0, length, 0x2)
            sstore(0x0, addr)
        }
        return addr;
    }

    function opCreate2Complex(bytes memory bytecode, uint length) public returns(address, uint256) {
        uint256 number = add(1, 2);

        address addr;
        assembly {
            addr := create2(0, add(bytecode, 0x20), length, 0x2)
            sstore(0x0, addr)
        }

        number = add(2, 4);

        return (addr, number);
    }

    function add(uint256 a, uint256 b) public pure returns(uint256) {
        return a + b;
    }

    function sendValue() public payable {
        uint bal;
        assembly{
            bal := add(bal,callvalue())
            sstore(0x1, bal)
        }
    }

    function opCreateValue(bytes memory bytecode, uint length) public payable returns(address) {
        address addr;
        assembly {
            addr := create(500, 0xa0, length)
            sstore(0x0, addr)
        }
        return addr;
    }

    function opCreate2Value(bytes memory bytecode, uint length) public payable returns(address) {
        address addr;
        assembly {
            addr := create2(300, 0xa0, length, 0x55555)
            sstore(0x0, addr)
        }
        return addr;
    }
}