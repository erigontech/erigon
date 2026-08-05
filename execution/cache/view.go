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
// per domain. ok=false means the backend has no exact frontier for the domain
// (remote, history-disabled); fills sourced from such a view are skipped.
//
// An implementation may report a stale-low bound — that only over-rejects
// fills — but must never overstate what its tx can currently read: admission
// safety rests on that.
type Frontier interface {
	DomainVisibleEnd(domain kv.Domain) (visibleEnd uint64, ok bool)
}

// FrontierFunc adapts a function to the Frontier interface.
type FrontierFunc func(domain kv.Domain) (visibleEnd uint64, ok bool)

func (f FrontierFunc) DomainVisibleEnd(domain kv.Domain) (uint64, bool) { return f(domain) }

// ReadView is the read-and-fill handle of a StateCache, bound to one
// transaction's read view: values filled through it are vouched for by that
// view's frontier alone, and it must not outlive the transaction. A nil
// frontier disables the admission-gated fills (Fill, SeedAddrCodeHash);
// FillCodeSize is content-addressed and works on any view. The zero value is
// inert: reads miss, fills no-op.
//
// A ReadView does not isolate reads: the cache holds latest-applied state, so
// a hit can be newer than the view — the same direction the exec overlay
// already serves. In the forward direction the cache's invariant is
// monotonicity (content never regresses behind the applied frontier),
// enforced on the fill side; unwinds invalidate by epoch and floor.
// Snapshot-isolated caching is kvcache's job (node/shards).
type ReadView struct {
	c        *StateCache
	frontier Frontier
}

// View creates a ReadView vouched for by f. A nil f disables admission-gated fills.
func (c *StateCache) View(f Frontier) ReadView { return ReadView{c: c, frontier: f} }

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

// CanFill reports whether this view carries a frontier, i.e. Fill and
// SeedAddrCodeHash can admit values through it.
func (v ReadView) CanFill() bool { return v.c != nil && v.frontier != nil }

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
		v.c.fillCodeIfFresh(key, value, readTxNum, visibleEnd, accountsEnd)
		return
	}
	v.c.fillIfFresh(domain, key, value, readTxNum, visibleEnd)
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
	v.c.seedAddrCodeHash(addr, h, txNum, visibleEnd)
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
// applies, unwinds and clears. It belongs to the authoritative mutation path
// — the SharedDomains commit/unwind code. The zero value is a no-op.
type Applier struct {
	c *StateCache
}

// Applier creates the writer handle.
func (c *StateCache) Applier() Applier { return Applier{c: c} }

// Apply makes a committed domain update authoritative for subsequent fills:
// it advances the domain's applied frontier and mutates the cache in the same
// critical section, so a fill from an older read view can never land on top.
func (a Applier) Apply(domain kv.Domain, key, value []byte, txNum uint64) {
	if a.c == nil {
		return
	}
	a.c.apply(domain, key, value, txNum)
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
