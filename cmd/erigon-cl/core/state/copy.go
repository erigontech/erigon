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
	if bs.publicKeyIndicies == nil {
		bs.publicKeyIndicies = make(map[[48]byte]uint64)
	}
	for k := range bs.publicKeyIndicies {
		delete(bs.publicKeyIndicies, k)
	}
	for pk, index := range b.publicKeyIndicies {
		bs.publicKeyIndicies[pk] = index
	}
	// Sync caches
	bs.activeValidatorsCache = copyLRU(bs.activeValidatorsCache, b.activeValidatorsCache)
	bs.shuffledSetsCache = copyLRU(bs.shuffledSetsCache, b.shuffledSetsCache)
	bs.committeeCache = copyLRU(bs.committeeCache, b.committeeCache)

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
