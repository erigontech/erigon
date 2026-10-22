// SPDX-License-Identifier: LGPL-3.0
pragma solidity >=0.8.0;

contract HistoryStorageStub {
    fallback() external payable {
        assembly {
            switch caller()
            case 0xfffffffffffffffffffffffffffffffffffffffe {
                sstore(mod(sub(number(), 1), 8191), calldataload(0))
            }
            default {
                mstore(0, sload(calldataload(0)))
                return(0, 32)
            }
        }
    }
}
