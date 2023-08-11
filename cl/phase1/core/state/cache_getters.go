package state

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

func (b *CachingBeaconState) ValidatorIndexByPubkey(key [48]byte) (uint64, bool) {
	val, ok := b.publicKeyIndicies[key]
	return val, ok
}

// PreviousStateRoot gets the previously saved state root and then deletes it.
func (b *CachingBeaconState) PreviousStateRoot() libcommon.Hash {
	ret := b.previousStateRoot
	b.previousStateRoot = libcommon.Hash{}
	return ret
}
