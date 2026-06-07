package commitment

import (
	"bytes"
	"context"
	"encoding/hex"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

// buildBigAccountCorpus: several small accounts + one account with bigSlots
// storage entries (> deepStorageThreshold) that triggers the deep fan-out.
func buildBigAccountCorpus(bigSlots int) (pk [][]byte, upds []Update) {
	rnd := rand.New(rand.NewSource(771))
	ub := NewUpdateBuilder()
	addAcc := func(slots int) {
		addr := make([]byte, length.Addr)
		rnd.Read(addr)
		a := hex.EncodeToString(addr)
		ub.Balance(a, rnd.Uint64()+1)
		for s := 0; s < slots; s++ {
			loc := make([]byte, length.Hash)
			rnd.Read(loc)
			val := make([]byte, 32)
			rnd.Read(val)
			ub.Storage(a, hex.EncodeToString(loc), hex.EncodeToString(val))
		}
	}
	for i := 0; i < 8; i++ {
		addAcc(3)
	}
	addAcc(bigSlots)
	for i := 0; i < 8; i++ {
		addAcc(2)
	}
	return ub.Build()
}

func TestDeepIntegration_Parity(t *testing.T) {
	t.Setenv("ERIGON_CMT_MOUNT", "1")
	old := cmtDeep
	cmtDeep = true
	defer func() { cmtDeep = old }()

	pk, upds := buildBigAccountCorpus(15_000)
	ctx := context.Background()

	seqMs := NewMockState(t)
	require.NoError(t, seqMs.applyPlainUpdates(pk, upds))
	seq := NewHexPatriciaHashed(length.Addr, seqMs, DefaultTrieConfig())
	sUpd := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, pk, upds)
	seqRoot, err := seq.Process(ctx, sUpd, "", nil, WarmupConfig{})
	require.NoError(t, err)
	sUpd.Close()

	for _, workers := range []int{1, 4, 8} {
		parMs := NewMockState(t)
		parMs.SetConcurrentCommitment(true)
		require.NoError(t, parMs.applyPlainUpdates(pk, upds))
		pph := NewParallelPatriciaHashed(mockTrieCtxFactory(parMs), length.Addr, DefaultTrieConfig())
		pph.SetNumWorkers(workers)
		pph.ResetContext(parMs)
		pUpd := WrapKeyUpdates(t, ModeParallel, KeyToHexNibbleHash, pk, upds)
		parRoot, err := pph.Process(ctx, pUpd, "", nil, WarmupConfig{})
		require.NoError(t, err)
		pUpd.Close()
		pph.Release()
		require.Equalf(t, seqRoot, parRoot, "deep parallel(workers=%d) root != sequential", workers)
	}
}

// TestDeepIntegration_BranchParity checks not just the root but every stored
// branch — execution writes branches to the DB and the next block reads them, so
// a matching root with wrong branch metadata still breaks the chain.
func TestDeepIntegration_BranchParity(t *testing.T) {
	t.Setenv("ERIGON_CMT_MOUNT", "1")
	old := cmtDeep
	cmtDeep = true
	defer func() { cmtDeep = old }()

	pk, upds := buildBigAccountCorpus(15_000)
	ctx := context.Background()

	seqMs := NewMockState(t)
	require.NoError(t, seqMs.applyPlainUpdates(pk, upds))
	seq := NewHexPatriciaHashed(length.Addr, seqMs, DefaultTrieConfig())
	sUpd := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, pk, upds)
	seqRoot, err := seq.Process(ctx, sUpd, "", nil, WarmupConfig{})
	require.NoError(t, err)
	sUpd.Close()

	parMs := NewMockState(t)
	parMs.SetConcurrentCommitment(true)
	require.NoError(t, parMs.applyPlainUpdates(pk, upds))
	pph := NewParallelPatriciaHashed(mockTrieCtxFactory(parMs), length.Addr, DefaultTrieConfig())
	pph.SetNumWorkers(8)
	pph.ResetContext(parMs)
	pUpd := WrapKeyUpdates(t, ModeParallel, KeyToHexNibbleHash, pk, upds)
	parRoot, err := pph.Process(ctx, pUpd, "", nil, WarmupConfig{})
	require.NoError(t, err)
	pUpd.Close()
	pph.Release()

	require.Equal(t, seqRoot, parRoot, "root")

	mism := 0
	seen := map[string]struct{}{}
	for k := range seqMs.cm {
		seen[k] = struct{}{}
	}
	for k := range parMs.cm {
		seen[k] = struct{}{}
	}
	for k := range seen {
		sb, sok := seqMs.cm[k]
		pb, pok := parMs.cm[k]
		if !sok || !pok || !bytes.Equal(sb, pb) {
			mism++
		}
	}
	t.Logf("seq branches=%d par branches=%d mismatched=%d", len(seqMs.cm), len(parMs.cm), mism)
	if mism != 0 {
		branchDiff(t, seqMs, parMs)
	}
	require.Zero(t, mism, "stored branch metadata differs between deep-parallel and sequential")
}
