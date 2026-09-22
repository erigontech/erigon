package pool

import (
	"errors"
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

func (m *slotMap[K, V]) Add(key K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.add(key, value)
}

func (m *slotMap[K, V]) add(key K, value V) {
	m.values[key] = value
	slot := m.slotFor(key)
	if m.bySlot[slot] == nil {
		m.bySlot[slot] = make(map[K]struct{})
	}
	m.bySlot[slot][key] = struct{}{}
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

func (m *slotMap[K, V]) Remove(key K) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.remove(key)
}

func (m *slotMap[K, V]) remove(key K) bool {
	if _, ok := m.values[key]; !ok {
		return false
	}
	delete(m.values, key)
	slot := m.slotFor(key)
	delete(m.bySlot[slot], key)
	if len(m.bySlot[slot]) == 0 {
		delete(m.bySlot, slot)
	}
	return true
}

func (m *slotMap[K, V]) PruneSlotsBefore(slot uint64) {
	m.PruneSlots(func(entrySlot uint64) bool { return entrySlot < slot })
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
	HighestBids *slotMap[HighestBidKey, *cltypes.SignedExecutionPayloadBid]

	// ProposerPreferences stores validated SignedProposerPreferences keyed by (slot, dependent_root).
	// Written by the proposer_preferences gossip service, read by the execution_payload_bid service.
	ProposerPreferences *slotMap[ProposerPreferencesKey, *cltypes.SignedProposerPreferences]
	// PayloadAttestations stores recently validated PayloadAttestationMessages for beacon API serving.
	// Short-lived cache (~1 slot), keyed by (slot, validatorIndex).
	PayloadAttestations *lru.Cache[PayloadAttestationKey, *cltypes.PayloadAttestationMessage]
}

func (p *EpbsPool) GetHighestBid(key HighestBidKey) (*cltypes.SignedExecutionPayloadBid, bool) {
	return p.HighestBids.Get(key)
}

func (p *EpbsPool) HighestBidKeys() []HighestBidKey {
	return p.HighestBids.Keys()
}

// StoreHighestBid replaces the current entry for key.
func (p *EpbsPool) StoreHighestBid(key HighestBidKey, bid *cltypes.SignedExecutionPayloadBid) {
	p.HighestBids.Add(key, bid)
}

// RemoveHighestBid preserves a concurrently stored replacement for the same key.
func (p *EpbsPool) RemoveHighestBid(key HighestBidKey, bid *cltypes.SignedExecutionPayloadBid) bool {
	p.HighestBids.mu.Lock()
	defer p.HighestBids.mu.Unlock()
	current, found := p.HighestBids.values[key]
	if !found || current != bid {
		return false
	}
	return p.HighestBids.remove(key)
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

// InsertProposerPreference keeps the first preference for a slot and dependent root.
// It returns false without mutation for an identical signed retry, or an error for a conflict.
func (p *EpbsPool) InsertProposerPreference(preference *cltypes.SignedProposerPreferences) (inserted bool, err error) {
	preferences := p.ProposerPreferences
	preferences.mu.Lock()
	defer preferences.mu.Unlock()
	key := ProposerPreferencesKey{Slot: preference.Message.ProposalSlot, DependentRoot: preference.Message.DependentRoot}
	if stored, ok := preferences.values[key]; ok {
		if stored != nil && stored.Message != nil && *stored.Message == *preference.Message && stored.Signature == preference.Signature {
			return false, nil
		}
		return false, errors.New("different proposer preferences already stored for this slot and dependent root")
	}
	preferences.add(key, preference)
	return true, nil
}
