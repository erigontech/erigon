package types

import "github.com/ledgerwatch/erigon/common"

// This is introduced because of the Tendermint IAVL Merkle Proof verification exploitation.
var NanoBlackList = []common.Address{
	common.HexToAddress("0x489A8756C18C0b8B24EC2a2b9FF3D4d447F79BEc"),
	common.HexToAddress("0xFd6042Df3D74ce9959922FeC559d7995F3933c55"),
	// Test Account
	common.HexToAddress("0xdb789Eb5BDb4E559beD199B8b82dED94e1d056C9"),
}
