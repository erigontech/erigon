// SPDX-License-Identifier: GPL-3.0
pragma solidity >=0.7.0 <0.9.0;

contract ChainCallLevel3 {
    receive() external payable {}

    function exec(address level4Addr) public payable {
        bool ok;
        (ok, ) = level4Addr.call(abi.encodeWithSignature("exec()"));
        require(ok, "failed to perform call to level 4");

        (ok, ) = level4Addr.delegatecall(abi.encodeWithSignature("exec()"));
        require(ok, "failed to perform delegate call to level 4");
    }

    function callRevert(address level4Addr) public payable {
        bool ok;
        (ok, ) = level4Addr.call(abi.encodeWithSignature("execRevert()"));
        require(ok, "failed to perform call to level 4");
    }

    function delegateCallRevert(address level4Addr) public payable {
        bool ok;
        (ok, ) = level4Addr.delegatecall(
            abi.encodeWithSignature("execRevert()")
        );
        require(ok, "failed to perform delegate call to level 4");
    }

    function get(address level4Addr) public view returns (string memory t) {
        bool ok;
        bytes memory result;

        (ok, result) = level4Addr.staticcall(abi.encodeWithSignature("get()"));
        require(ok, "failed to perform static call to level 4");

        t = abi.decode(result, (string));
    }
}