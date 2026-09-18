package commitment

import (
	"encoding/hex"
	"math/rand"
	"testing"

	"github.com/erigontech/erigon/common/length"
	"github.com/stretchr/testify/require"
)

// TestIncrementalFoldEquivalence pins the correctness the per-tx commitment
// overlap relies on: folding a block's updates in sequential mid-block chunks
// (each chunk Processed on the accumulating trie via SetState resume) yields the
// same root as folding the whole block at once. It is the same mechanism
// multi-block accumulation already uses (Process per block, SetState carries the
// trie forward), exercised one granularity finer — per sub-block instead of per
// block. If this holds, a mid-block forward-progress fold at any tx cadence is
// root-equivalent to the block-end fold.
func TestIncrementalFoldEquivalence(t *testing.T) {
	t.Parallel()
	for _, mode := range []runMode{modeSeq, modeParallel} {
		mode := mode
		name := "seq"
		if mode == modeParallel {
			name = "parallel"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rnd := rand.New(rand.NewSource(20260918))
			ub := NewUpdateBuilder()
			for range 60 {
				addRandomAccount(ub, rnd, 4)
			}
			keys, upds := ub.Build()

			workers := 0
			if mode == modeParallel {
				workers = 4
			}

			msBatch := NewMockState(t)
			rootBatch := processModeBatch(t, msBatch, mode, workers, keys, upds)

			msIncr := NewMockState(t)
			var blob, rootIncr []byte
			const chunks = 5
			n := len(keys)
			for c := 0; c < chunks; c++ {
				lo := c * n / chunks
				hi := (c + 1) * n / chunks
				if lo == hi {
					continue
				}
				rootIncr, blob = processModeBatchState(t, msIncr, mode, workers, keys[lo:hi], upds[lo:hi], blob)
			}

			require.Equal(t, rootBatch, rootIncr,
				"incremental %d-chunk fold must equal single batch fold", chunks)
		})
	}
}

// TestIncrementalFoldEquivalenceHotKeys covers the realistic overlap case a
// disjoint split does not: the same accounts re-touched in every chunk (a hot
// contract updated by many txs across the block). The incremental fold sees each
// account's intermediate value at each mid-block checkpoint; the batch fold sees
// only the final value. The root must still match — last-write-wins on the trie
// path — or a mid-block forward-progress fold would diverge from block-end.
func TestIncrementalFoldEquivalenceHotKeys(t *testing.T) {
	t.Parallel()
	rnd := rand.New(rand.NewSource(771))
	const nAcct = 24
	addrs := make([]string, nAcct)
	for i := range addrs {
		b := make([]byte, length.Addr)
		rnd.Read(b)
		addrs[i] = hex.EncodeToString(b)
	}

	const chunks = 6
	finalBal := make(map[string]uint64, nAcct)
	msIncr := NewMockState(t)
	var blob, rootIncr []byte
	for c := 0; c < chunks; c++ {
		ub := NewUpdateBuilder()
		for i, a := range addrs {
			bal := uint64((c+1)*100_000 + i) // every account re-touched, new value each chunk
			ub.Balance(a, bal)
			finalBal[a] = bal
		}
		keys, upds := ub.Build()
		rootIncr, blob = processModeBatchState(t, msIncr, modeSeq, 0, keys, upds, blob)
	}

	ub := NewUpdateBuilder()
	for _, a := range addrs {
		ub.Balance(a, finalBal[a])
	}
	keys, upds := ub.Build()
	msBatch := NewMockState(t)
	rootBatch := processModeBatch(t, msBatch, modeSeq, 0, keys, upds)

	require.Equal(t, rootBatch, rootIncr,
		"incremental fold of re-touched hot keys must equal single batch fold of their final values")
}
