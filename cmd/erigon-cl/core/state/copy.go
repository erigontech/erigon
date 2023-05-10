package state

import (
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state/raw"
)

func (b *BeaconState) CopyInto(bs *BeaconState) (err error) {
	if bs.BeaconState == nil {
		bs.BeaconState = raw.New(b.BeaconConfig())
	}
	err = b.BeaconState.CopyInto(bs.BeaconState)
	if err != nil {
		return err
	}
	err = b.copyCachesInto(bs)
	if err != nil {
		return err
	}
	return nil
}

func (b *BeaconState) copyCachesInto(bs *BeaconState) error {
	if b.Version() == clparams.Phase0Version {
		return bs.initBeaconState()
	}
	bs.publicKeyIndicies = make(map[[48]byte]uint64)
	for pk, index := range b.publicKeyIndicies {
		bs.publicKeyIndicies[pk] = index
	}
	// Sync caches
	if err := bs.initCaches(); err != nil {
		return err
	}
	copyLRU(bs.activeValidatorsCache, b.activeValidatorsCache)
	copyLRU(bs.shuffledSetsCache, b.shuffledSetsCache)
	copyLRU(bs.committeeCache, b.committeeCache)

	if b.totalActiveBalanceCache != nil {
		bs.totalActiveBalanceCache = new(uint64)
		*bs.totalActiveBalanceCache = *b.totalActiveBalanceCache
		bs.totalActiveBalanceRootCache = b.totalActiveBalanceRootCache
	}
	return nil
}

func (b *BeaconState) Copy() (bs *BeaconState, err error) {
	copied := New(b.BeaconConfig())
	err = b.CopyInto(copied)
	if err != nil {
		return nil, err
	}
	return copied, nil
}
