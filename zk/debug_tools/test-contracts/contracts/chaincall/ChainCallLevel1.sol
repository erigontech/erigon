// SPDX-License-Identifier: GPL-3.0
pragma solidity >=0.7.0 <0.9.0;

contract ChainCallLevel1 {
    receive() external payable {}

    function delegateTransfer(
        address level2Addr,
        address level3Addr,
        address level4Addr
    ) public payable {
        bool ok;
        (ok, ) = level2Addr.delegatecall(
            abi.encodeWithSignature("transfers(address)", level3Addr)
        );
        require(ok, "failed to perform delegate call to level 2");
    }

    function exec(
        address level2Addr,
        address level3Addr,
        address level4Addr
    ) public payable {
        bool ok;
        (ok, ) = level2Addr.call(
            abi.encodeWithSignature(
                "exec(address,address)",
                level3Addr,
                level4Addr
            )
        );
        require(ok, "failed to perform call to level 2");

        (ok, ) = level2Addr.delegatecall(
            abi.encodeWithSignature(
                "exec(address,address)",
                level3Addr,
                level4Addr
            )
        );
        require(ok, "failed to perform delegate call to level 2");

        bytes memory result;
        (ok, result) = level2Addr.staticcall(
            abi.encodeWithSignature(
                "get(address,address)",
                level3Addr,
                level4Addr
            )
        );
        require(ok, "failed to perform static call to level 2");

        string memory t;
        (t) = abi.decode(result, (string));
    }

    function callRevert(
        address level2Addr,
        address level3Addr,
        address level4Addr
    ) public payable {
        bool ok;
        (ok, ) = level2Addr.call(
            abi.encodeWithSignature(
                "callRevert(address,address)",
                level3Addr,
                level4Addr
            )
        );
        require(ok, "failed to perform call to level 2");
    }

    function delegateCallRevert(
        address level2Addr,
        address level3Addr,
        address level4Addr
    ) public payable {
        bool ok;
        (ok, ) = level2Addr.delegatecall(
            abi.encodeWithSignature(
                "delegateCallRevert(address,address)",
                level3Addr,
                level4Addr
            )
        );
        require(ok, "failed to perform delegate call to level 2");
    }
}