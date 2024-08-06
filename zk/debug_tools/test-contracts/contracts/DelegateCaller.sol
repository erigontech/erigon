// SPDX-License-Identifier: GPL-3.0
pragma solidity >=0.7.0 <0.9.0;

contract DelegateCaller {
    function call(address _contract, uint _num) public payable {
        bool ok;
        (ok, ) = _contract.call(
            abi.encodeWithSignature("setVars(uint256)", _num)
        );
        require(ok, "failed to perform call");
    }

    function delegateCall(address _contract, uint _num) public payable {
        bool ok;
        (ok, ) = _contract.delegatecall(
            abi.encodeWithSignature("setVars(uint256)", _num)
        );
        require(ok, "failed to perform delegate call");
    }

    function staticCall(address _contract) public payable {
        bool ok;
        bytes memory result;
        (ok, result) = _contract.staticcall(
            abi.encodeWithSignature("getVars()")
        );
        require(ok, "failed to perform static call");

        uint256 num;
        address sender;
        uint256 value;

        (num, sender, value) = abi.decode(result, (uint256, address, uint256));
    }

    function invalidStaticCallMoreParameters(address _contract) view public {
        bool ok;
        (ok,) = _contract.staticcall(
            abi.encodeWithSignature("getVarsAndVariable(uint256)", 1, 2)
        );
        require(!ok, "static call was supposed to fail with more parameters");
    }

    function invalidStaticCallLessParameters(address _contract) view public {
        bool ok;
        (ok,) = _contract.staticcall(
            abi.encodeWithSignature("getVarsAndVariable(uint256)")
        );
        require(!ok, "static call was supposed to fail with less parameters");
    }

    function invalidStaticCallWithInnerCall(address _contract) view public {
        bool ok;
        (ok,) = _contract.staticcall(
            abi.encodeWithSignature("getVarsAndVariable(uint256)")
        );
        require(!ok, "static call was supposed to fail with less parameters");
    }

    function multiCall(address _contract, uint _num) public payable {
        call(_contract, _num);
        delegateCall(_contract, _num);
        staticCall(_contract);
    }

    function preEcrecover_0() pure public {
        bytes32 messHash = 0x456e9aea5e197a1f1af7a3e85a3212fa4049a3ba34c2289b4c860fc0b0c64ef3;
        uint8 v = 28;
        bytes32 r = 0x9242685bf161793cc25603c231bc2f568eb630ea16aa137d2664ac8038825608;
        bytes32 s = 0x4f8ae3bd7535248d0bd448298cc2e2071e56992d0774dc340c368ae950852ada;

        ecrecover(messHash, v, r, s);
    } 
}