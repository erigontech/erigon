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

package seqsig

import (
	"bytes"
	"cmp"
	"slices"
	"sync"

	"github.com/erigontech/erigon/common"
)

const (
	// MaxFutureMs is how far ahead of local time a head's epoch may be.
	MaxFutureMs = 10_000
	// MaxSources bounds the tracked sources; the lowest epoch goes past it.
	MaxSources = 8
)

// Source is one signing process: a signer and its start epoch.
type Source struct {
	Signer  common.Address
	EpochMs uint64
}

func (s Source) compare(o Source) int {
	if c := cmp.Compare(s.EpochMs, o.EpochMs); c != 0 {
		return c
	}
	return bytes.Compare(s.Signer[:], o.Signer[:])
}

// Observation is what a head is to its source's latest.
type Observation uint8

const (
	// ObservedNew is a head above its source's latest, now the latest.
	ObservedNew Observation = iota
	// ObservedDuplicate is the source's latest again.
	ObservedDuplicate
	// ObservedStale is a replay below the source's latest, or a new source
	// evicted at once for having the lowest epoch.
	ObservedStale
	// ObservedFuture is a head whose epoch is more than MaxFutureMs ahead.
	ObservedFuture
	// ObservedEquivocation is a second head under the latest's version.
	ObservedEquivocation
)

func (o Observation) String() string {
	switch o {
	case ObservedNew:
		return "new"
	case ObservedDuplicate:
		return "duplicate"
	case ObservedStale:
		return "stale"
	case ObservedFuture:
		return "future"
	case ObservedEquivocation:
		return "equivocation"
	}
	return "unknown"
}

// Tracker keeps the latest head of each source. It is safe for concurrent
// use.
type Tracker struct {
	mu     sync.Mutex
	latest map[Source]*VerifiedHead
}

// Observe records h at local time nowMs. On ObservedEquivocation it also
// returns the hash of the head kept, the first one seen.
func (t *Tracker) Observe(h *VerifiedHead, nowMs uint64) (Observation, common.Hash) {
	if h.Head.EpochMs > nowMs && h.Head.EpochMs-nowMs > MaxFutureMs {
		return ObservedFuture, common.Hash{}
	}
	s := Source{Signer: h.Signer, EpochMs: h.Head.EpochMs}
	t.mu.Lock()
	defer t.mu.Unlock()
	if cur, ok := t.latest[s]; ok {
		switch c := cmp.Compare(h.Head.Counter, cur.Head.Counter); {
		case c < 0:
			return ObservedStale, common.Hash{}
		case c == 0 && h.Head.BlockHash == cur.Head.BlockHash:
			return ObservedDuplicate, common.Hash{}
		case c == 0:
			return ObservedEquivocation, cur.Head.BlockHash
		}
		t.latest[s] = h
		return ObservedNew, common.Hash{}
	}
	if t.latest == nil {
		t.latest = make(map[Source]*VerifiedHead)
	}
	t.latest[s] = h
	if len(t.latest) > MaxSources {
		oldest := s
		for o := range t.latest {
			if o.compare(oldest) < 0 {
				oldest = o
			}
		}
		delete(t.latest, oldest)
		if oldest == s {
			return ObservedStale, common.Hash{}
		}
	}
	return ObservedNew, common.Hash{}
}

// Candidates returns the latest head of every source, highest version first,
// ties broken by the higher signer.
func (t *Tracker) Candidates() []*VerifiedHead {
	t.mu.Lock()
	heads := make([]*VerifiedHead, 0, len(t.latest))
	for _, h := range t.latest {
		heads = append(heads, h)
	}
	t.mu.Unlock()
	slices.SortFunc(heads, func(a, b *VerifiedHead) int {
		if c := b.Head.Version().Compare(a.Head.Version()); c != 0 {
			return c
		}
		return bytes.Compare(b.Signer[:], a.Signer[:])
	})
	return heads
}
