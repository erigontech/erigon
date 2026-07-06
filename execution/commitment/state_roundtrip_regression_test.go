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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

// requireRestartParity folds the batches through every engine with a node restart between
// batches (runEngineBatches carries state via EncodeCurrentState/SetState) and requires
// every batch root to match the sequential arm, whose final root must match the combined
// single-batch oracle.
func requireRestartParity(t *testing.T, batches []engineBatch, combinedK [][]byte, combinedU []Update) {
	t.Helper()
	oracle, _ := engineRoot(t, modeSeq, 0, combinedK, combinedU)
	seqRoots, _ := runEngineBatches(t, modeSeq, 0, batches)
	require.Equal(t, oracle, seqRoots[len(seqRoots)-1], "sequential restart lifecycle diverged from the combined oracle")

	for _, tc := range []struct {
		name    string
		mode    runMode
		workers int
	}{
		{"parallel_w1", modeParallel, 1},
		{"parallel_w4", modeParallel, 4},
		{"streaming_committer_w4", modeStreaming, 4},
		{"streaming_scheduled_w4", modeStreamingScheduled, 4},
		{"streaming_public_w4", modeStreamingPublic, 4},
	} {
		roots, _ := runEngineBatches(t, tc.mode, tc.workers, batches)
		for i := range batches {
			require.Equalf(t, seqRoots[i], roots[i],
				"%s: batch %d root diverged from sequential under the restart lifecycle", tc.name, i+1)
		}
	}
}

// singleNibbleCorpus builds a trie whose accounts all hash under first nibble 7, so the
// root row folds via propagate and no root branch record ever reaches disk.
func singleNibbleCorpus() (k1 [][]byte, u1 []Update, k2 [][]byte, u2 []Update, kc [][]byte, uc []Update) {
	var addrs []string
	for i := 0; i < 6; i++ {
		addrs = append(addrs, addrHex(findAddressForNibble(7, 100+i)))
	}
	newcomer := addrHex(findAddressForNibble(7, 4711))

	ub1 := NewUpdateBuilder()
	for i, a := range addrs {
		ub1.Balance(a, uint64(1000+i))
	}
	for i := 0; i < 3; i++ {
		ub1.Storage(addrs[0], hex.EncodeToString(slotHashBytes(i)), "0badc0de")
	}
	ub1.Storage(addrs[1], hex.EncodeToString(slotHashBytes(7)), "cafe")
	k1, u1 = ub1.Build()

	ub2 := NewUpdateBuilder().Balance(addrs[2], 2222).Balance(addrs[3], 3333).Balance(newcomer, 42)
	k2, u2 = ub2.Build()

	ubc := NewUpdateBuilder()
	for i, a := range addrs {
		bal := uint64(1000 + i)
		switch i {
		case 2:
			bal = 2222
		case 3:
			bal = 3333
		}
		ubc.Balance(a, bal)
	}
	ubc.Balance(newcomer, 42)
	for i := 0; i < 3; i++ {
		ubc.Storage(addrs[0], hex.EncodeToString(slotHashBytes(i)), "0badc0de")
	}
	ubc.Storage(addrs[1], hex.EncodeToString(slotHashBytes(7)), "cafe")
	kc, uc = ubc.Build()
	return k1, u1, k2, u2, kc, uc
}

// A trie whose accounts all hash under ONE first nibble folds its root row via propagate:
// the root becomes an extension-topped cell and no root branch record exists on disk. The
// persisted trie state must round-trip so the next block after a restart navigates through
// the root cell's extension instead of demanding the absent root record.
func TestStateRoundTrip_PropagateRootSingleNibble(t *testing.T) {
	t.Parallel()
	k1, u1, k2, u2, kc, uc := singleNibbleCorpus()
	requireRestartParity(t, []engineBatch{{k1, u1}, {k2, u2}}, kc, uc)
}

// The same propagate-root shape with the collapsed extension starting at nibble ZERO and
// running two nibbles deep: the engines' root probe key must tolerate an unfold that
// consumes more of the extension than the probe's length instead of panicking on the
// out-of-range read, and the top-nibble mount wall must stay at depth one.
func TestStateRoundTrip_PropagateRootZeroNibblePrefix(t *testing.T) {
	t.Parallel()
	var addrs []string
	for i := 0; i < 6; i++ {
		addrs = append(addrs, addrHex(findAddressForHexPrefix([]byte{0, 0xa}, 800+i)))
	}

	ub1 := NewUpdateBuilder()
	for i, a := range addrs {
		ub1.Balance(a, uint64(3000+i))
	}
	ub1.Storage(addrs[0], hex.EncodeToString(slotHashBytes(4)), "dead")
	k1, u1 := ub1.Build()

	ub2 := NewUpdateBuilder().Balance(addrs[1], 3111).Balance(addrs[4], 3444)
	k2, u2 := ub2.Build()

	ubc := NewUpdateBuilder()
	for i, a := range addrs {
		bal := uint64(3000 + i)
		switch i {
		case 1:
			bal = 3111
		case 4:
			bal = 3444
		}
		ubc.Balance(a, bal)
	}
	ubc.Storage(addrs[0], hex.EncodeToString(slotHashBytes(4)), "dead")
	kc, uc := ubc.Build()

	requireRestartParity(t, []engineBatch{{k1, u1}, {k2, u2}}, kc, uc)
}

// A root row that WAS a branch collapses to one surviving first nibble when a block
// deletes every account under the other nibbles: the propagate fold removes the root
// branch record and leaves the collapsed shape only in the trie state. The restart
// round-trip must survive the collapse and keep the survivors' untouched storage.
func TestStateRoundTrip_DeleteCollapseToSingleNibble(t *testing.T) {
	t.Parallel()
	var gone, kept []string
	for i := 0; i < 4; i++ {
		gone = append(gone, addrHex(findAddressForNibble(3, 200+i)))
		kept = append(kept, addrHex(findAddressForNibble(7, 300+i)))
	}

	ub1 := NewUpdateBuilder()
	for i, a := range gone {
		ub1.Balance(a, uint64(100+i))
	}
	for i, a := range kept {
		ub1.Balance(a, uint64(500+i))
	}
	ub1.Storage(kept[0], hex.EncodeToString(slotHashBytes(1)), "beef")
	ub1.Storage(kept[0], hex.EncodeToString(slotHashBytes(2)), "f00d")
	k1, u1 := ub1.Build()

	ub2 := NewUpdateBuilder().Balance(kept[1], 5511)
	for _, a := range gone {
		ub2.Delete(a)
	}
	k2, u2 := ub2.Build()

	ub3 := NewUpdateBuilder().Balance(kept[2], 5522)
	k3, u3 := ub3.Build()

	ubc := NewUpdateBuilder().
		Balance(kept[0], 500).Balance(kept[1], 5511).Balance(kept[2], 5522).Balance(kept[3], 503)
	ubc.Storage(kept[0], hex.EncodeToString(slotHashBytes(1)), "beef")
	ubc.Storage(kept[0], hex.EncodeToString(slotHashBytes(2)), "f00d")
	kc, uc := ubc.Build()

	requireRestartParity(t, []engineBatch{{k1, u1}, {k2, u2}, {k3, u3}}, kc, uc)
}

// Restoring the template AFTER a scheduler already built its base must not fold against
// the stale base: the changed seed drops it so Process rebuilds from the restored root.
func TestStateRoundTrip_SeedAfterSchedulerStart(t *testing.T) {
	t.Parallel()
	k1, u1, k2, u2, kc, uc := singleNibbleCorpus()
	oracle, _ := engineRoot(t, modeSeq, 0, kc, uc)

	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	_, blob := processModeBatchState(t, ms, modeStreaming, 4, k1, u1, nil)

	require.NoError(t, ms.applyPlainUpdates(k2, u2))
	sc := NewStreamingCommitter(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	defer sc.Release()
	sc.SetNumWorkers(4)
	require.NoError(t, sc.StartScheduler(context.Background()))

	tmpl := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer tmpl.Release()
	require.NoError(t, tmpl.SetState(blob))
	sc.SeedRootFrom(tmpl)

	for _, k := range k2 {
		sc.TouchKey(KeyToHexNibbleHash(k), k, nil)
	}
	got, err := sc.Process(context.Background())
	require.NoError(t, err)
	require.Equal(t, oracle, got, "seed arriving after StartScheduler was folded against the stale base")
}

// A fresh trie with NO carried state blob must still bootstrap from the on-disk branch
// records alone when the root is a real branch — the rebuild/reset shape the blob-carrying
// helpers no longer exercise.
func TestFreshTrieBootstrapOverDiskState(t *testing.T) {
	t.Parallel()
	k1, u1 := buildMixedCorpus(31337, 120)
	ub2 := NewUpdateBuilder()
	n := 0
	for _, k := range k1 {
		if len(k) != length.Addr {
			continue
		}
		ub2.Balance(hex.EncodeToString(k), uint64(90_000+n))
		if n++; n == 5 {
			break
		}
	}
	k2, u2 := ub2.Build()
	require.NotEmpty(t, k2)

	bootstrapRoot := func(mode runMode, workers int) []byte {
		ms := NewMockState(t)
		if mode != modeSeq {
			ms.SetConcurrentCommitment(true)
		}
		processModeBatchState(t, ms, mode, workers, k1, u1, nil)
		root, _ := processModeBatchState(t, ms, mode, workers, k2, u2, nil)
		return root
	}

	want := bootstrapRoot(modeSeq, 0)
	for _, tc := range []struct {
		name    string
		mode    runMode
		workers int
	}{
		{"parallel_w4", modeParallel, 4},
		{"streaming_committer_w4", modeStreaming, 4},
		{"streaming_public_w4", modeStreamingPublic, 4},
	} {
		require.Equalf(t, want, bootstrapRoot(tc.mode, tc.workers), "%s: blobless bootstrap diverged", tc.name)
	}
}
