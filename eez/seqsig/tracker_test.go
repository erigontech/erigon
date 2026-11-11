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

package seqsig_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/eez/seqsig"
)

// tracked is a verified head of signer 0x01…01 unless signer is set.
type tracked struct {
	epochMs, counter uint64
	hash             byte
	signer           byte
}

func (h tracked) verified() *seqsig.VerifiedHead {
	signer := h.signer
	if signer == 0 {
		signer = 1
	}
	return &seqsig.VerifiedHead{
		Head: seqsig.Head{
			RollupID:  1,
			Number:    h.counter,
			BlockHash: repeat(h.hash),
			EpochMs:   h.epochMs,
			Counter:   h.counter,
		},
		Signer: common.BytesToAddress(bytes.Repeat([]byte{signer}, length.Addr)),
	}
}

func repeat(b byte) common.Hash {
	return common.BytesToHash(bytes.Repeat([]byte{b}, length.Hash))
}

type observed struct {
	head tracked
	now  uint64
	want seqsig.Observation
}

// candidate is the (epochMs, counter, hash) of a candidate.
type candidate struct {
	epochMs, counter uint64
	hash             byte
}

func TestTracker(t *testing.T) {
	for _, c := range []struct {
		name       string
		observe    []observed
		candidates []candidate
		kept       common.Hash // the head kept on the last observation's equivocation
	}{
		{
			name: "higher counter replaces within source",
			observe: []observed{
				{tracked{100, 1, 1, 0}, 100, seqsig.ObservedNew},
				{tracked{100, 2, 2, 0}, 100, seqsig.ObservedNew},
			},
			candidates: []candidate{{100, 2, 2}},
		},
		{
			name: "lower counter is stale",
			observe: []observed{
				{tracked{100, 5, 1, 0}, 100, seqsig.ObservedNew},
				{tracked{100, 3, 2, 0}, 100, seqsig.ObservedStale},
			},
			candidates: []candidate{{100, 5, 1}},
		},
		{
			name: "same counter, same hash is a duplicate",
			observe: []observed{
				{tracked{100, 5, 1, 0}, 100, seqsig.ObservedNew},
				{tracked{100, 5, 1, 0}, 100, seqsig.ObservedDuplicate},
			},
			candidates: []candidate{{100, 5, 1}},
		},
		{
			name: "same counter, other hash is an equivocation that keeps the first",
			observe: []observed{
				{tracked{100, 5, 1, 0}, 100, seqsig.ObservedNew},
				{tracked{100, 5, 2, 0}, 100, seqsig.ObservedEquivocation},
			},
			candidates: []candidate{{100, 5, 1}},
			kept:       repeat(1),
		},
		{
			name: "restarted source outranks the old epoch",
			observe: []observed{
				{tracked{100, 10_000, 1, 0}, 200, seqsig.ObservedNew},
				{tracked{200, 1, 2, 0}, 200, seqsig.ObservedNew},
			},
			candidates: []candidate{{200, 1, 2}, {100, 10_000, 1}},
		},
		{
			name: "replay after restart is superseded",
			observe: []observed{
				{tracked{100, 3, 1, 0}, 100, seqsig.ObservedNew},
				{tracked{100, 9, 2, 0}, 100, seqsig.ObservedNew},
			},
			candidates: []candidate{{100, 9, 2}},
		},
		{
			name: "epoch more than 10 s ahead is rejected",
			observe: []observed{
				{tracked{11_001, 1, 1, 0}, 1_000, seqsig.ObservedFuture},
			},
		},
		{
			name: "epoch exactly 10 s ahead is accepted",
			observe: []observed{
				{tracked{11_000, 1, 1, 0}, 1_000, seqsig.ObservedNew},
			},
			candidates: []candidate{{11_000, 1, 1}},
		},
		{
			name: "a future head of a known source leaves its latest",
			observe: []observed{
				{tracked{11_000, 1, 1, 0}, 1_000, seqsig.ObservedNew},
				{tracked{11_000, 2, 2, 0}, 900, seqsig.ObservedFuture},
			},
			candidates: []candidate{{11_000, 1, 1}},
		},
		{
			name: "a future head leaves its source's latest",
			observe: []observed{
				{tracked{100, 1, 1, 0}, 100, seqsig.ObservedNew},
				{tracked{20_000, 2, 2, 0}, 100, seqsig.ObservedFuture},
			},
			candidates: []candidate{{100, 1, 1}},
		},
		{
			name: "oldest source is evicted past the cap",
			observe: []observed{
				{tracked{1, 1, 1, 0}, 100, seqsig.ObservedNew},
				{tracked{2, 1, 1, 0}, 100, seqsig.ObservedNew},
				{tracked{3, 1, 1, 0}, 100, seqsig.ObservedNew},
				{tracked{4, 1, 1, 0}, 100, seqsig.ObservedNew},
				{tracked{5, 1, 1, 0}, 100, seqsig.ObservedNew},
				{tracked{6, 1, 1, 0}, 100, seqsig.ObservedNew},
				{tracked{7, 1, 1, 0}, 100, seqsig.ObservedNew},
				{tracked{8, 1, 1, 0}, 100, seqsig.ObservedNew},
				{tracked{9, 1, 1, 0}, 100, seqsig.ObservedNew},
			},
			candidates: []candidate{{9, 1, 1}, {8, 1, 1}, {7, 1, 1}, {6, 1, 1}, {5, 1, 1}, {4, 1, 1}, {3, 1, 1}, {2, 1, 1}},
		},
		{
			name: "a new source below every tracked epoch is evicted at once",
			observe: []observed{
				{tracked{2, 1, 1, 0}, 100, seqsig.ObservedNew},
				{tracked{3, 1, 1, 0}, 100, seqsig.ObservedNew},
				{tracked{4, 1, 1, 0}, 100, seqsig.ObservedNew},
				{tracked{5, 1, 1, 0}, 100, seqsig.ObservedNew},
				{tracked{6, 1, 1, 0}, 100, seqsig.ObservedNew},
				{tracked{7, 1, 1, 0}, 100, seqsig.ObservedNew},
				{tracked{8, 1, 1, 0}, 100, seqsig.ObservedNew},
				{tracked{9, 1, 1, 0}, 100, seqsig.ObservedNew},
				{tracked{1, 1, 1, 0}, 100, seqsig.ObservedStale},
			},
			candidates: []candidate{{9, 1, 1}, {8, 1, 1}, {7, 1, 1}, {6, 1, 1}, {5, 1, 1}, {4, 1, 1}, {3, 1, 1}, {2, 1, 1}},
		},
		{
			name: "eviction breaks an epoch tie on the lower signer",
			observe: []observed{
				{tracked{5, 1, 1, 2}, 100, seqsig.ObservedNew},
				{tracked{5, 1, 2, 1}, 100, seqsig.ObservedNew},
				{tracked{6, 1, 3, 0}, 100, seqsig.ObservedNew},
				{tracked{7, 1, 3, 0}, 100, seqsig.ObservedNew},
				{tracked{8, 1, 3, 0}, 100, seqsig.ObservedNew},
				{tracked{9, 1, 3, 0}, 100, seqsig.ObservedNew},
				{tracked{10, 1, 3, 0}, 100, seqsig.ObservedNew},
				{tracked{11, 1, 3, 0}, 100, seqsig.ObservedNew},
				{tracked{12, 1, 3, 0}, 100, seqsig.ObservedNew},
			},
			candidates: []candidate{{12, 1, 3}, {11, 1, 3}, {10, 1, 3}, {9, 1, 3}, {8, 1, 3}, {7, 1, 3}, {6, 1, 3}, {5, 1, 1}},
		},
		{
			name: "candidates order by version across sources, then by signer",
			observe: []observed{
				{tracked{100, 7, 1, 1}, 100, seqsig.ObservedNew},
				{tracked{100, 9, 2, 2}, 100, seqsig.ObservedNew},
				{tracked{100, 9, 3, 3}, 100, seqsig.ObservedNew},
				{tracked{50, 1_000, 4, 4}, 100, seqsig.ObservedNew},
			},
			candidates: []candidate{{100, 9, 3}, {100, 9, 2}, {100, 7, 1}, {50, 1_000, 4}},
		},
		{
			name: "the same epoch under two signers is two sources",
			observe: []observed{
				{tracked{100, 5, 1, 1}, 100, seqsig.ObservedNew},
				{tracked{100, 5, 2, 2}, 100, seqsig.ObservedNew},
			},
			candidates: []candidate{{100, 5, 2}, {100, 5, 1}},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			var tr seqsig.Tracker
			var kept common.Hash
			for i, o := range c.observe {
				got, k := tr.Observe(o.head.verified(), o.now)
				require.Equal(t, o.want, got, "observation %d", i)
				kept = k
			}
			require.Equal(t, c.kept, kept)
			var got []candidate
			for _, h := range tr.Candidates() {
				got = append(got, candidate{h.Head.EpochMs, h.Head.Counter, h.Head.BlockHash[0]})
			}
			require.Equal(t, c.candidates, got)
		})
	}
}
