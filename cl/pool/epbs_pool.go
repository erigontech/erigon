package pool

import (
	"sync"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/common"
)

const (
	epbsPayloadAttestationsPoolSize = 512 // one slot's worth of PTC votes
)

type slotMap[K comparable, V any] struct {
	mu      sync.RWMutex
	values  map[K]V
	slotFor func(K) uint64
}

func newSlotMap[K comparable, V any](slotFor func(K) uint64) *slotMap[K, V] {
	return &slotMap[K, V]{values: make(map[K]V), slotFor: slotFor}
}

func (m *slotMap[K, V]) Add(key K, value V) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.values[key] = value
	return false
}

func (m *slotMap[K, V]) Get(key K) (V, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, ok := m.values[key]
	return value, ok
}

func (m *slotMap[K, V]) Keys() []K {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]K, 0, len(m.values))
	for key := range m.values {
		keys = append(keys, key)
	}
	return keys
}

func (m *slotMap[K, V]) PruneSlotsBefore(slot uint64) {
	m.PruneSlots(func(entrySlot uint64) bool { return entrySlot < slot })
}

func (m *slotMap[K, V]) PruneSlots(remove func(uint64) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key := range m.values {
		if remove(m.slotFor(key)) {
			delete(m.values, key)
		}
	}
}

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
	ProposerPreferences *slotMap[ProposerPreferencesKey, *cltypes.SignedProposerPreferences]

	// HighestBids stores the highest bid seen per (slot, parent_block_hash, parent_block_root).
	// Written and read by the execution_payload_bid gossip service.
	HighestBids *slotMap[HighestBidKey, *cltypes.SignedExecutionPayloadBid]

	// PayloadAttestations stores recently validated PayloadAttestationMessages for beacon API serving.
	// Short-lived cache (~1 slot), keyed by (slot, validatorIndex).
	PayloadAttestations *lru.Cache[PayloadAttestationKey, *cltypes.PayloadAttestationMessage]
}

func NewEpbsPool() *EpbsPool {
	preferencesCache := newSlotMap[ProposerPreferencesKey, *cltypes.SignedProposerPreferences](func(key ProposerPreferencesKey) uint64 { return key.Slot })
	highestBidsCache := newSlotMap[HighestBidKey, *cltypes.SignedExecutionPayloadBid](func(key HighestBidKey) uint64 { return key.Slot })
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

func (p *EpbsPool) GetPreference(slot uint64, dependentRoot common.Hash) (*cltypes.SignedProposerPreferences, bool) {
	return p.ProposerPreferences.Get(ProposerPreferencesKey{Slot: slot, DependentRoot: dependentRoot})
}
