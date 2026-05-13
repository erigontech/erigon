package pool

import (
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/common"
)

const (
	epbsPreferencesPoolSize         = 64  // ~2 epochs of slots
	epbsHighestBidsPoolSize         = 128 // multiple builders × parent hashes × a few slots
	epbsPayloadAttestationsPoolSize = 512 // one slot's worth of PTC votes
)

// ProposerPreferencesKey identifies a proposer preferences entry by slot and dependent root.
// Different dependent roots (different forks) must not overwrite each other.
type ProposerPreferencesKey struct {
	Slot          uint64
	DependentRoot common.Hash
}

// PayloadAttestationKey identifies a payload attestation by slot and validator.
type PayloadAttestationKey struct {
	Slot           uint64
	ValidatorIndex uint64
}

// HighestBidKey identifies a bid market: a specific slot, parent block hash, and parent block root.
// Different parent hashes or beacon block roots create separate bid markets.
// Spec: consensus-specs PR #5001 — prevents cross-fork bid interference.
type HighestBidKey struct {
	Slot            uint64
	ParentBlockHash common.Hash
	ParentBlockRoot common.Hash
}

// EpbsPool holds EPBS-related gossip data caches.
// [New in Gloas:EIP7732]
type EpbsPool struct {
	// ProposerPreferences stores validated SignedProposerPreferences keyed by (slot, dependent_root).
	// Written by the proposer_preferences gossip service, read by the execution_payload_bid service.
	ProposerPreferences *lru.Cache[ProposerPreferencesKey, *cltypes.SignedProposerPreferences]

	// HighestBids stores the highest bid seen per (slot, parent_block_hash, parent_block_root).
	// Written and read by the execution_payload_bid gossip service.
	HighestBids *lru.Cache[HighestBidKey, *cltypes.SignedExecutionPayloadBid]

	// PayloadAttestations stores recently validated PayloadAttestationMessages for beacon API serving.
	// Short-lived cache (~1 slot), keyed by (slot, validatorIndex).
	PayloadAttestations *lru.Cache[PayloadAttestationKey, *cltypes.PayloadAttestationMessage]
}

func NewEpbsPool() *EpbsPool {
	preferencesCache, err := lru.New[ProposerPreferencesKey, *cltypes.SignedProposerPreferences]("proposerPreferencesPool", epbsPreferencesPoolSize)
	if err != nil {
		panic(err)
	}
	highestBidsCache, err := lru.New[HighestBidKey, *cltypes.SignedExecutionPayloadBid]("highestBidsPool", epbsHighestBidsPoolSize)
	if err != nil {
		panic(err)
	}
	payloadAttestationsCache, err := lru.New[PayloadAttestationKey, *cltypes.PayloadAttestationMessage]("payloadAttestationsPool", epbsPayloadAttestationsPoolSize)
	if err != nil {
		panic(err)
	}
	return &EpbsPool{
		ProposerPreferences: preferencesCache,
		HighestBids:         highestBidsCache,
		PayloadAttestations: payloadAttestationsCache,
	}
}

// GetPreferencesForSlot returns all stored proposer preferences that match the given slot,
// regardless of dependent_root. This is used by the bid service which needs to find any
// valid preferences for a slot across different fork views.
func (p *EpbsPool) GetPreferencesForSlot(slot uint64) []*cltypes.SignedProposerPreferences {
	var results []*cltypes.SignedProposerPreferences
	for _, key := range p.ProposerPreferences.Keys() {
		if key.Slot != slot {
			continue
		}
		if msg, ok := p.ProposerPreferences.Get(key); ok && msg != nil {
			results = append(results, msg)
		}
	}
	return results
}
