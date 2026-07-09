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

package commitment

import (
	"context"
	"encoding/hex"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

// buildSubsetTouchedWhale builds two batches for one whale account. Batch 1 fills
// storage across the wide first-storage-nibbles; batch 2 adds fresh slots under
// only the touch subset (plus a balance bump so the account leaf is touched). The
// wide-minus-touch nibbles stay untouched on disk and must survive batch 2.
func buildSubsetTouchedWhale(seed int64, wide, touch []byte, perNibble1, perNibble2 int) (k1 [][]byte, u1 []Update, k2 [][]byte, u2 []Update) {
	rnd := rand.New(rand.NewSource(seed))
	addr := make([]byte, length.Addr)
	rnd.Read(addr)
	a := hex.EncodeToString(addr)

	firstStorageNibble := func(loc []byte) byte {
		pk := make([]byte, 0, length.Addr+len(loc))
		pk = append(pk, addr...)
		pk = append(pk, loc...)
		return KeyToHexNibbleHash(pk)[64]
	}
	genSlot := func(want byte) (string, string) {
		for {
			loc := make([]byte, length.Hash)
			rnd.Read(loc)
			if firstStorageNibble(loc) == want {
				val := make([]byte, 32)
				rnd.Read(val)
				return hex.EncodeToString(loc), hex.EncodeToString(val)
			}
		}
	}

	ub1 := NewUpdateBuilder()
	ub1.Balance(a, 1)
	for _, n := range wide {
		for range perNibble1 {
			l, v := genSlot(n)
			ub1.Storage(a, l, v)
		}
	}
	k1, u1 = ub1.Build()

	ub2 := NewUpdateBuilder()
	ub2.Balance(a, 2)
	for _, n := range touch {
		for range perNibble2 {
			l, v := genSlot(n)
			ub2.Storage(a, l, v)
		}
	}
	k2, u2 = ub2.Build()
	return k1, u1, k2, u2
}

// A pre-existing on-disk whale whose storage spans many first-storage-nibbles, with
// only a subset touched in the next block, drives the deep fold over the touched nibbles
// only. The deep fold must still preserve the untouched on-disk first-nibble siblings, so
// the parallel/streaming root matches sequential. Filler accounts give the whale real trie
// context.
func TestDeepFold_PreExistingWhale_SubsetTouched(t *testing.T) {
	wide := nibs(0, 1, 2, 3, 4, 5, 6, 7)
	touch := nibs(0, 1, 2)
	// batch 2 crosses deepStorageThreshold on a subset of the wide nibbles, triggering the
	// deep fold while the untouched first-nibbles stay on disk.
	k1, u1, k2, u2 := buildSubsetTouchedWhale(20260622, wide, touch, 60, 420)
	fk, fu := buildMixedCorpus(7777, 200)
	k1 = append(append([][]byte{}, fk...), k1...)
	u1 = append(append([]Update{}, fu...), u1...)
	requireAllEnginesParity(t, k1, u1, k2, u2, 4)
}

// A pre-existing on-disk whale whose storage all sits under a SINGLE first-storage-nibble
// has no branch record exactly at the account prefix — its storage top is a deeper
// extension. The next block touches other first-nibbles, crossing deepStorageThreshold and
// driving the deep storage fold. seedBaseAtPrefix finds no branch at the account prefix;
// it must still recover the untouched single-nibble subtree rather than seeding an empty
// base and dropping it, so the parallel/streaming root matches sequential. Regression for
// the empty-seed sibling drop (#22113).
func TestDeepFold_PreExistingWhale_SingleNibbleOnDisk(t *testing.T) {
	onDisk := nibs(0)   // all existing slots under one first-nibble -> no branch at the account prefix
	touch := nibs(3, 7) // next block touches disjoint first-nibbles, crossing the deep-fold threshold
	k1, u1, k2, u2 := buildSubsetTouchedWhale(20260702, onDisk, touch, 120, 700)
	fk, fu := buildMixedCorpus(4242, 200)
	k1 = append(append([][]byte{}, fk...), k1...)
	u1 = append(append([]Update{}, fu...), u1...)
	requireAllEnginesParity(t, k1, u1, k2, u2, 4)
}

// A FRESH whale — its account absent from the pre-state trie — provably has nothing on
// disk beneath its storage prefix, so the deep fold seeds an empty base and folds the
// slots concurrently instead of demoting to serial streaming.
func TestDeepFold_FreshWhaleFoldsParallel(t *testing.T) {
	k1, u1, _, _ := buildSubsetTouchedWhale(20260707, nibs(3, 7), nil, 700, 0)
	fk, fu := buildMixedCorpus(555, 200)
	keys := append(append([][]byte{}, fk...), k1...)
	upds := append(append([]Update{}, fu...), u1...)

	seqRoot, _ := engineRoot(t, modeSeq, 0, keys, upds)

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	require.NoError(t, ms.applyPlainUpdates(keys, upds))
	sc := newStreamCommitter(t, ms, 4, false)
	defer sc.Release()
	touchAll(sc, keys)
	got, err := sc.Process(context.Background())
	require.NoError(t, err)
	require.Equal(t, seqRoot, got, "fresh-whale concurrent fold diverged from sequential")
	require.Positive(t, sc.DeepLocalFolds(), "a fresh whale must take the concurrent deep fold, not the serial demotion")

	parRoot, _ := engineRoot(t, modeParallel, 4, keys, upds)
	require.Equal(t, seqRoot, parRoot)
}

// The demotion gate stays for accounts present in the pre-state without a branch record
// at their prefix (single embedded slot): the influx still streams serially.
func TestDeepFold_ExistingWhaleStillDemotes(t *testing.T) {
	k1, u1, k2, u2 := buildSubsetTouchedWhale(20260708, nibs(0), nibs(3, 7), 1, 700)
	fk, fu := buildMixedCorpus(556, 200)
	k1 = append(append([][]byte{}, fk...), k1...)
	u1 = append(append([]Update{}, fu...), u1...)

	seqRoot, _ := incrementalRoot(t, modeSeq, 0, k1, u1, k2, u2)

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	sc := newStreamCommitter(t, ms, 4, false)
	defer sc.Release()
	require.NoError(t, ms.applyPlainUpdates(k1, u1))
	touchAll(sc, k1)
	_, err := sc.Process(context.Background())
	require.NoError(t, err)
	require.NoError(t, ms.applyPlainUpdates(k2, u2))
	touchAll(sc, k2)
	got, err := sc.Process(context.Background())
	require.NoError(t, err)
	require.Equal(t, seqRoot, got)
	require.Zero(t, sc.DeepLocalFolds(), "an account present in the pre-state must keep the serial demotion")
}
