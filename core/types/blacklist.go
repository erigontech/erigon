package types

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

// This is introduced because of the Tendermint IAVL Merkle Proof verification exploitation.
var NanoBlackList = []libcommon.Address{
	libcommon.HexToAddress("0x489A8756C18C0b8B24EC2a2b9FF3D4d447F79BEc"),
	libcommon.HexToAddress("0xFd6042Df3D74ce9959922FeC559d7995F3933c55"),
	// Test Account
	libcommon.HexToAddress("0xdb789Eb5BDb4E559beD199B8b82dED94e1d056C9"),
}
