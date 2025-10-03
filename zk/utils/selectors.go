package utils

import (
    libcrypto "github.com/erigontech/erigon-lib/crypto"
)

// ERC-20 function signatures
const (
    SigERC20Transfer     = "transfer(address,uint256)"
    SigERC20TransferFrom = "transferFrom(address,address,uint256)"
)

// Selectors are the first 4 bytes of the keccak256 hash of the signature
var (
    SelectorERC20Transfer     = calcSelector(SigERC20Transfer)
    SelectorERC20TransferFrom = calcSelector(SigERC20TransferFrom)
)

func calcSelector(sig string) [4]byte {
    h := libcrypto.Keccak256([]byte(sig))
    var sel [4]byte
    copy(sel[:], h[:4])
    return sel
}

// HasSelector returns true if the calldata begins with the given 4-byte selector.
func HasSelector(data []byte, selector [4]byte) bool {
    return len(data) >= 4 &&
        data[0] == selector[0] &&
        data[1] == selector[1] &&
        data[2] == selector[2] &&
        data[3] == selector[3]
}

