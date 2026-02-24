package pool

import (
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/common"
)

const (
	epbsPreferencesPoolSize = 64  // ~2 epochs of slots
	epbsHighestBidsPoolSize = 128 // multiple builders × parent hashes × a few slots
)

// HighestBidKey identifies a bid market: a specific slot and parent block hash.
// Different parent hashes create separate bid markets.
type HighestBidKey struct {
	Slot            uint64
	ParentBlockHash common.Hash
}

// EpbsPool holds EPBS-related gossip data caches.
// [New in Gloas:EIP7732]
type EpbsPool struct {
	// ProposerPreferences stores validated SignedProposerPreferences keyed by proposal slot.
	// Written by the proposer_preferences gossip service, read by the execution_payload_bid service.
	ProposerPreferences *lru.Cache[uint64, *cltypes.SignedProposerPreferences]

	// HighestBids stores the highest bid seen per (slot, parent_block_hash).
	// Written and read by the execution_payload_bid gossip service.
	HighestBids *lru.Cache[HighestBidKey, *cltypes.SignedExecutionPayloadBid]
}

func NewEpbsPool() *EpbsPool {
	preferencesCache, err := lru.New[uint64, *cltypes.SignedProposerPreferences]("proposerPreferencesPool", epbsPreferencesPoolSize)
	if err != nil {
		panic(err)
	}
	highestBidsCache, err := lru.New[HighestBidKey, *cltypes.SignedExecutionPayloadBid]("highestBidsPool", epbsHighestBidsPoolSize)
	if err != nil {
		panic(err)
	}
	return &EpbsPool{
		ProposerPreferences: preferencesCache,
		HighestBids:         highestBidsCache,
	}
}
