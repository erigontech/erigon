// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package cache

import (
	"github.com/erigontech/erigon/db/kv"
)

// Frontier reports the exclusive txNum bound of one transaction's read view
// per domain. ok=false means the view has no exact frontier for the domain
// (remote or history-disabled backends, dependency-clamped values views);
// fills sourced from such a view are skipped.
//
// An implementation may report a stale-low bound only for a coherent,
// monotonically extended view — then it merely over-rejects fills. A view
// serving mixed-age reads has no exact frontier and must answer ok=false.
// Overstating what the tx can currently read is never safe: admission rests
// on that.
type Frontier interface {
	DomainVisibleEnd(domain kv.Domain) (visibleEnd uint64, ok bool)
}

// stateVersionFrontier identifies the durable state snapshot behind a
// frontier. A StateCache initialized with a state version admits fills only
// from frontiers that report the same version.
type stateVersionFrontier interface {
	Frontier
	StateVersion() uint64
}

type frontierWithStateVersion struct {
	Frontier
	stateVersion uint64
}

func (f frontierWithStateVersion) StateVersion() uint64 { return f.stateVersion }

// FrontierWithStateVersion attaches the durable state identity used when a
// StateCache decides whether a frontier may fill.
func FrontierWithStateVersion(frontier Frontier, stateVersion uint64) Frontier {
	if frontier == nil {
		return nil
	}
	return frontierWithStateVersion{Frontier: frontier, stateVersion: stateVersion}
}

// FrontierFunc adapts a function to the Frontier interface.
type FrontierFunc func(domain kv.Domain) (visibleEnd uint64, ok bool)

func (f FrontierFunc) DomainVisibleEnd(domain kv.Domain) (uint64, bool) { return f(domain) }

// rejectedFrontier distinguishes a non-retryable rejection from a nil, retryable
// binding without growing ReadView, which is embedded in every state getter.
type rejectedFrontier struct{}

func (rejectedFrontier) DomainVisibleEnd(kv.Domain) (uint64, bool) { return 0, false }

// ReadView is the read-and-fill handle of a StateCache, bound to one
// transaction's read view: values filled through it are vouched for by that
// view's frontier, and it must not outlive the transaction. Without an accepted
// frontier, Fill and SeedAddrCodeHash are no-ops. FillCodeSize remains available
// because code size is content-addressed. The zero value is inert: reads miss,
// fills no-op.
//
// A ReadView does not isolate reads: the cache holds latest-applied state, so
// a hit can be newer than the view — the same direction the exec overlay
// already serves. In the forward direction the cache's invariant is
// monotonicity (content never regresses behind the applied frontier),
// enforced on the fill side; unwinds invalidate stored entries by their
// per-cache entry epoch and floor.
//
// Each view also snapshots the StateCache read-view epoch. An unwind advances
// that epoch, so older views can still read but cannot fill from the discarded
// fork. State version is checked when the frontier is bound, not on every fill:
// continuous forward publication keeps the view eligible, while the domain
// frontier rejects values older than the latest update. A discontinuity also
// advances the epoch and revokes every previously bound view.
// During publication, reads remain available. Existing eligible views cannot
// fill until the complete update batch is installed. A view bound during
// publication has no frontier and remains fill-inert until explicitly rebound.
// Snapshot-isolated caching is kvcache's job (node/shards).
type ReadView struct {
	c             *StateCache
	frontier      Frontier
	readViewEpoch uint64
}

// View creates a ReadView vouched for by f. If the cache has a durable state
// version, f must report the same version when it is bound. A stale or
// versionless frontier is not retried automatically. A nil frontier,
// publication in progress, or a cache behind f may be retried later.
func (c *StateCache) View(f Frontier) ReadView {
	if c == nil {
		return ReadView{}
	}
	if f == nil {
		return ReadView{c: c, readViewEpoch: c.readViewEpoch.Load()}
	}
	c.admissionMu.RLock()
	defer c.admissionMu.RUnlock()
	return ReadView{
		c:             c,
		frontier:      c.bindFrontierLocked(f),
		readViewEpoch: c.readViewEpoch.Load(),
	}
}

// WithFrontier binds f while preserving the original view's read-view epoch.
// Binding is serialized with publication boundaries so a transaction from an
// older durable state cannot gain fill authority after an unwind commits.
func (v ReadView) WithFrontier(f Frontier) ReadView {
	if v.c == nil {
		return v
	}
	if f == nil {
		v.frontier = nil
		return v
	}
	v.c.admissionMu.RLock()
	defer v.c.admissionMu.RUnlock()
	v.frontier = v.c.bindFrontierLocked(f)
	return v
}

func (c *StateCache) bindFrontierLocked(frontier Frontier) Frontier {
	if frontier == nil || c.publishing {
		return nil
	}
	if !c.stateVersionKnown {
		return frontier
	}
	versioned, ok := frontier.(stateVersionFrontier)
	if !ok {
		return rejectedFrontier{}
	}
	stateVersion := versioned.StateVersion()
	if stateVersion < c.stateVersion {
		return rejectedFrontier{}
	}
	if stateVersion > c.stateVersion {
		return nil
	}
	return frontier
}

// Get retrieves data for the given domain and key.
// Returns (value, true) on cache hit — including (nil, true) for cached negatives —
// and (nil, false) on cache miss.
func (v ReadView) Get(domain kv.Domain, key []byte) ([]byte, bool) {
	if v.c == nil {
		return nil, false
	}
	return v.c.get(domain, key)
}

// GetWithTxNum is Get plus the txNum the cached value reflects, so the read
// path can bound a hit by step against an in-flight unwind's maxStep.
func (v ReadView) GetWithTxNum(domain kv.Domain, key []byte) ([]byte, uint64, bool) {
	if v.c == nil {
		return nil, 0, false
	}
	return v.c.getWithTxNum(domain, key)
}

// GetCodeByHash retrieves code bytes by their Ethereum codeHash (keccak256),
// bypassing the addr-keyed CodeDomain lookup. Returns (nil, false) on miss.
func (v ReadView) GetCodeByHash(codeHash []byte) ([]byte, bool) {
	if v.c == nil {
		return nil, false
	}
	return v.c.getCodeByHash(codeHash)
}

// GetCodeSizeByHash returns the cached code length for codeHash.
func (v ReadView) GetCodeSizeByHash(codeHash []byte) (int, bool) {
	if v.c == nil {
		return 0, false
	}
	return v.c.getCodeSizeByHash(codeHash)
}

// GetAddrCodeHash returns the Ethereum codeHash for addr without an
// account-domain round-trip. The hash is zero when ok is false.
func (v ReadView) GetAddrCodeHash(addr []byte) ([32]byte, bool) {
	if v.c == nil {
		return [32]byte{}, false
	}
	return v.c.getAddrCodeHash(addr)
}

// CanFill reports whether this view carries an accepted frontier, i.e. Fill and
// SeedAddrCodeHash can admit values through it.
func (v ReadView) CanFill() bool {
	if v.c == nil || v.frontier == nil {
		return false
	}
	_, rejected := v.frontier.(rejectedFrontier)
	return !rejected
}

// NeedsFrontier reports whether rebinding could make this view fill-eligible.
func (v ReadView) NeedsFrontier() bool { return v.c != nil && v.frontier == nil }

// Fill offers a value read from this view without replacing an authoritative
// entry. Admission is checked against the view's frontier for the domain;
// views without an exact frontier skip the fill. A code fill also checks the
// accounts frontier: an addr-keyed code entry derives from the account — an
// account deletion drops it without advancing the code frontier — so a view
// that predates the deletion must not refill it (mirrors SeedAddrCodeHash).
func (v ReadView) Fill(domain kv.Domain, key []byte, value []byte, readTxNum uint64) {
	if v.c == nil || v.c.disableFills || v.frontier == nil {
		return
	}
	visibleEnd, ok := v.frontier.DomainVisibleEnd(domain)
	if !ok {
		return
	}
	if domain == kv.CodeDomain {
		accountsEnd, ok := v.frontier.DomainVisibleEnd(kv.AccountsDomain)
		if !ok {
			return
		}
		v.c.fillCodeIfFresh(key, value, readTxNum, visibleEnd, accountsEnd, v.readViewEpoch)
		return
	}
	v.c.fillIfFresh(domain, key, value, readTxNum, visibleEnd, v.readViewEpoch)
}

// SeedAddrCodeHash offers an addr → codeHash mapping derived from an account
// record read from this view, so admission checks the accounts frontier even
// though the mapping lives in the code cache.
func (v ReadView) SeedAddrCodeHash(addr []byte, h [32]byte, txNum uint64) {
	if v.c == nil || v.c.disableFills || v.frontier == nil {
		return
	}
	visibleEnd, ok := v.frontier.DomainVisibleEnd(kv.AccountsDomain)
	if !ok {
		return
	}
	v.c.seedAddrCodeHash(addr, h, txNum, visibleEnd, v.readViewEpoch)
}

// FillCodeSize records the code length for codeHash. Content-addressed and
// immutable for a given hash, so it needs no admission and no frontier — but
// it is still a reader write, so the fills switch covers it.
func (v ReadView) FillCodeSize(codeHash []byte, size int, txNum uint64) {
	if v.c == nil || v.c.disableFills {
		return
	}
	v.c.putCodeSizeByHash(codeHash, size, txNum)
}

// Applier is the authoritative writer handle of a StateCache: post-commit
// publications, unwinds and clears. It belongs to the authoritative mutation path
// — the SharedDomains commit/unwind code. The zero value is a no-op.
type Applier struct {
	c *StateCache
}

// StateUpdate is one committed domain mutation published to StateCache.
type StateUpdate struct {
	Domain kv.Domain
	Key    []byte
	Value  []byte
	TxNum  uint64
}

// Applier creates the writer handle.
func (c *StateCache) Applier() Applier { return Applier{c: c} }

// Initialize establishes the first durable version or moves the cache forward
// when a newer read view proves that a publication was missed. Moving forward
// without a complete delta clears entries; an equal or older view does nothing.
func (a Applier) Initialize(stateVersion uint64) {
	if a.c == nil {
		return
	}
	a.c.initialize(stateVersion)
}

// Publish applies one successful commit and advances the cache from its source
// state version to the committed state version. Admission-gated fills are
// disabled while the update batch is incomplete, but readers do not wait for
// the batch. Source continuity lets unchanged entries survive even if one
// commit advances the durable counter more than once.
func (a Applier) Publish(sourceStateVersion, committedStateVersion uint64, updates []StateUpdate) {
	if a.c == nil {
		return
	}
	a.c.publish(sourceStateVersion, committedStateVersion, 0, false, updates)
}

// PublishUnwind republishes an unwind at commit so fills admitted after the
// staged invalidation cannot survive into the committed state version. An
// older rejected publication still invalidates without moving the version.
func (a Applier) PublishUnwind(sourceStateVersion, committedStateVersion, unwindToTxNum uint64, updates []StateUpdate) {
	if a.c == nil {
		return
	}
	a.c.publish(sourceStateVersion, committedStateVersion, unwindToTxNum, true, updates)
}

// Unwind invalidates, across all caches, entries reflecting state above
// unwindToTxNum on a now-dead fork, and lowers the applied frontiers.
func (a Applier) Unwind(unwindToTxNum uint64) {
	if a.c == nil {
		return
	}
	a.c.unwind(unwindToTxNum)
}

// Clear removes all mutable entries from all caches. The applied frontiers
// survive — clearing is not a canonical-state rewind (that is Unwind).
func (a Applier) Clear() {
	if a.c == nil {
		return
	}
	a.c.clear()
}
