// SPDX-License-Identifier: GPL-3.0
pragma solidity >=0.7.0 <0.9.0;

contract ChainCallLevel2 {
    receive() external payable {}

    function transfers(address payable level3Addr) public payable {
        level3Addr.transfer(msg.value);

        bool sent = level3Addr.send(msg.value);
        require(sent, "Failed to send Ether via send");

        (sent, ) = level3Addr.call{value: msg.value}("");
        require(sent, "Failed to send Ether via call");
    }

    function exec(address level3Addr, address level4Addr) public payable {
        bool ok;
        (ok, ) = level3Addr.call(
            abi.encodeWithSignature("exec(address)", level4Addr)
        );
        require(ok, "failed to perform call to level 3");

        (ok, ) = level3Addr.delegatecall(
            abi.encodeWithSignature("exec(address)", level4Addr)
        );
        require(ok, "failed to perform delegate call to level 3");
    }

    function callRevert(address level3Addr, address level4Addr) public payable {
        bool ok;
        (ok, ) = level3Addr.call(
            abi.encodeWithSignature("callRevert(address)", level4Addr)
        );
        require(ok, "failed to perform call to level 3");
    }

    function delegateCallRevert(
        address level3Addr,
        address level4Addr
    ) public payable {
        bool ok;
        (ok, ) = level3Addr.delegatecall(
            abi.encodeWithSignature("delegateCallRevert(address)", level4Addr)
        );
        require(ok, "failed to perform delegate call to level 3");
    }

    function get(
        address level3Addr,
        address level4Addr
    ) public view returns (string memory t) {
        bool ok;
        bytes memory result;
        (ok, result) = level3Addr.staticcall(
            abi.encodeWithSignature("get(address)", level4Addr)
        );
        require(ok, "failed to perform static call to level 3");

        t = abi.decode(result, (string));

        (ok, result) = level4Addr.staticcall(abi.encodeWithSignature("get()"));
        require(ok, "failed to perform static call to level 4 from level 2");

        t = abi.decode(result, (string));
    }
}