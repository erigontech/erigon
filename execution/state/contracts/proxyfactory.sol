// SPDX-License-Identifier: LGPL-3.0
pragma solidity >=0.7.0;

// solc --allow-paths ., --abi --bin --overwrite --optimize -o execution/state/contracts/build execution/state/contracts/proxyfactory.sol
// ./build/bin/abigen -abi execution/state/contracts/build/Proxy.abi -bin execution/state/contracts/build/Proxy.bin -pkg contracts -type proxy -out execution/state/contracts/gen_proxy.go
// ./build/bin/abigen -abi execution/state/contracts/build/ProxyFactory.abi -bin execution/state/contracts/build/ProxyFactory.bin -pkg contracts -type proxyFactory -out execution/state/contracts/gen_proxyfactory.go

// Proxy is a minimal Gnosis-Safe-style delegatecall proxy: it forwards every
// call to the singleton held in slot 0. It carries no constructor arguments so
// its CREATE2 init code is deterministic, letting a factory deploy it onto an
// address that was already funded while empty (EIP-7610).
contract Proxy {
    address internal singleton;

    receive() external payable {}

    fallback() external payable {
        address impl = singleton;
        assembly {
            calldatacopy(0, 0, calldatasize())
            let ok := delegatecall(gas(), impl, 0, calldatasize(), 0, 0)
            returndatacopy(0, 0, returndatasize())
            if iszero(ok) { revert(0, returndatasize()) }
            return(0, returndatasize())
        }
    }
}

// ProxyFactory CREATE2-deploys Proxy instances under a caller-supplied salt,
// mirroring a Gnosis-Safe proxy factory.
contract ProxyFactory {
    event ProxyCreation(Proxy proxy);

    function createProxy(bytes32 salt) public returns (Proxy proxy) {
        proxy = new Proxy{salt: salt}();
        emit ProxyCreation(proxy);
    }
}
