//go:build gosentry

package commitment

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

// Struct-aware variant of Fuzz_ProcessUpdate. The []byte form has to reject
// every input whose accounts are not exactly length.Addr, which throws away
// most executions; fixed-size arrays let the fuzzer spend them on the trie.
type processUpdateInput struct {
	BalanceA uint64
	AccountA [length.Addr]byte
	BalanceB uint64
	AccountB [length.Addr]byte
}

func Fuzz_ProcessUpdateStruct(f *testing.F) {
	ha, _ := hex.DecodeString("13ccfe8074645cab4cb42b423625e055f0293c87")
	hb, _ := hex.DecodeString("73f822e709a0016bfaed8b5e81b5f86de31d6895")

	seed := processUpdateInput{BalanceA: 2, BalanceB: 1235105}
	copy(seed.AccountA[:], ha)
	copy(seed.AccountB[:], hb)
	f.Add(seed)

	ctx := context.Background()
	f.Fuzz(func(t *testing.T, in processUpdateInput) {
		builder := NewUpdateBuilder().
			Balance(hex.EncodeToString(in.AccountA[:]), in.BalanceA).
			Balance(hex.EncodeToString(in.AccountB[:]), in.BalanceB)

		ms := NewMockState(t)
		ms2 := NewMockState(t)
		hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
		hphAnother := NewHexPatriciaHashed(length.Addr, ms2, DefaultTrieConfig())

		plainKeys, updates := builder.Build()
		require.NoError(t, ms.applyPlainUpdates(plainKeys, updates))
		require.NoError(t, ms2.applyPlainUpdates(plainKeys, updates))

		upds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
		rootHashDirect, err := hph.Process(ctx, upds, "", nil, WarmupConfig{})
		require.NoError(t, err)
		require.Len(t, rootHashDirect, length.Hash, "invalid root hash length")
		upds.Close()

		anotherUpds := WrapKeyUpdates(t, ModeUpdate, KeyToHexNibbleHash, plainKeys, updates)
		rootHashUpdate, err := hphAnother.Process(ctx, anotherUpds, "", nil, WarmupConfig{})
		require.NoError(t, err)
		require.Len(t, rootHashUpdate, length.Hash, "invalid root hash length")
		require.Equal(t, rootHashDirect, rootHashUpdate, "storage-based and update-based rootHash mismatch")
		anotherUpds.Close()
	})
}
