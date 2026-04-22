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
	bySlot  map[uint64]map[K]struct{}
	slotFor func(K) uint64
}

func newSlotMap[K comparable, V any](slotFor func(K) uint64) *slotMap[K, V] {
	return &slotMap[K, V]{values: make(map[K]V), bySlot: make(map[uint64]map[K]struct{}), slotFor: slotFor}
}

func (m *slotMap[K, V]) Add(key K, value V) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.values[key] = value
	slot := m.slotFor(key)
	if m.bySlot[slot] == nil {
		m.bySlot[slot] = make(map[K]struct{})
	}
	m.bySlot[slot][key] = struct{}{}
	return false
}

func (m *slotMap[K, V]) ValuesForSlot(slot uint64) []V {
	m.mu.RLock()
	defer m.mu.RUnlock()
	values := make([]V, 0, len(m.bySlot[slot]))
	for key := range m.bySlot[slot] {
		values = append(values, m.values[key])
	}
	return values
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

func (m *slotMap[K, V]) PruneSlots(remove func(uint64) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for slot, keys := range m.bySlot {
		if remove(slot) {
			for key := range keys {
				delete(m.values, key)
			}
			delete(m.bySlot, slot)
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

	// OnPreferencesReceived is an optional callback invoked by the proposer
	// preferences gossip service after a validated preference is stored.
	// The ePBS builder wires this to PreferencesWatcher.OnPreferencesReceived
	// so the builder loop wakes up without polling the LRU cache.
	OnPreferencesReceived func(slot uint64, prefs *cltypes.SignedProposerPreferences)
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
	values := p.ProposerPreferences.ValuesForSlot(slot)
	results := make([]*cltypes.SignedProposerPreferences, 0, len(values))
	for _, msg := range values {
		if msg != nil {
			results = append(results, msg)
		}
	}
	return results
}

func (p *EpbsPool) GetPreference(slot uint64, dependentRoot common.Hash) (*cltypes.SignedProposerPreferences, bool) {
	return p.ProposerPreferences.Get(ProposerPreferencesKey{Slot: slot, DependentRoot: dependentRoot})
}
