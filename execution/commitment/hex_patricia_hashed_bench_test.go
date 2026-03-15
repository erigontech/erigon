// Copyright 2024 The Erigon Authors
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
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

func Benchmark_HexPatriciaHashed_Process(b *testing.B) {
	b.SetParallelism(1)

	rnd := rand.New(rand.NewSource(133777))
	keysCount := rnd.Intn(100_0000)

	// generate updates
	b.Logf("keys count: %d", keysCount)
	builder := NewUpdateBuilder()
	for i := 0; i < keysCount; i++ {
		key := make([]byte, length.Addr)
		rnd.Read(key)

		builder.Balance(hex.EncodeToString(key), rnd.Uint64())
	}
	pk, updates := builder.Build()
	b.Logf("%d keys generated", keysCount)
	ms := NewMockState(b)
	err := ms.applyPlainUpdates(pk, updates)
	require.NoError(b, err)

	hph := NewHexPatriciaHashed(length.Addr, ms)
	upds := WrapKeyUpdates(b, ModeDirect, KeyToHexNibbleHash, nil, nil)
	defer upds.Close()

	ctx := context.Background()
	for i := 0; b.Loop(); i++ {
		if i+5 >= len(pk) {
			i = 0
		}

		WrapKeyUpdatesInto(b, upds, pk[i:i+5], updates[i:i+5])
		_, err := hph.Process(ctx, upds, "", nil, WarmupConfig{})
		require.NoError(b, err)
	}
}

// Benchmark_HexPatriciaHashed_Process_Batch benchmarks Process with realistic
// batch sizes (100/500 keys). Larger batches multiply unfoldBranchNode calls,
// making the lazy deriveHashedKeys saving more measurable.
func Benchmark_HexPatriciaHashed_Process_Batch(b *testing.B) {
	const keysCount = 100_000

	for _, batchSize := range []int{100, 500} {
		batchSize := batchSize
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			b.SetParallelism(1)
			rnd := rand.New(rand.NewSource(133777))

			builder := NewUpdateBuilder()
			for i := 0; i < keysCount; i++ {
				key := make([]byte, length.Addr)
				rnd.Read(key)
				builder.Balance(hex.EncodeToString(key), rnd.Uint64())
			}
			pk, updates := builder.Build()
			ms := NewMockState(b)
			require.NoError(b, ms.applyPlainUpdates(pk, updates))

			hph := NewHexPatriciaHashed(length.Addr, ms)
			upds := WrapKeyUpdates(b, ModeDirect, KeyToHexNibbleHash, nil, nil)
			defer upds.Close()

			ctx := context.Background()
			for i := 0; b.Loop(); i++ {
				if i+batchSize >= len(pk) {
					i = 0
				}
				WrapKeyUpdatesInto(b, upds, pk[i:i+batchSize], updates[i:i+batchSize])
				_, err := hph.Process(ctx, upds, "", nil, WarmupConfig{})
				require.NoError(b, err)
			}
		})
	}
}

// Benchmark_HexPatriciaHashed_Unfold_Isolated isolates unfold cost by
// resetting HPH to root before each Process call, forcing a full root→leaf
// unfold for a single key every iteration. This maximises the unfold/fold
// ratio and directly measures the lazy deriveHashedKeys saving.
//
// Unlike the sliding-window benchmarks where the HPH grid retains hot paths
// across iterations, here every iteration starts cold: root → unfoldBranchNode
// at every trie level → leaf. The trie has 100k keys so sibling cells at each
// branch carry plain-key references, which is exactly what lazy derivation skips.
func Benchmark_HexPatriciaHashed_Unfold_Isolated(b *testing.B) {
	const keysCount = 100_000
	b.SetParallelism(1)
	rnd := rand.New(rand.NewSource(133777))

	builder := NewUpdateBuilder()
	for i := 0; i < keysCount; i++ {
		key := make([]byte, length.Addr)
		rnd.Read(key)
		builder.Balance(hex.EncodeToString(key), rnd.Uint64())
	}
	pk, updates := builder.Build()
	ms := NewMockState(b)
	require.NoError(b, ms.applyPlainUpdates(pk, updates))

	// Prime the MockState branch cache with a full commit pass.
	hph := NewHexPatriciaHashed(length.Addr, ms)
	upds := WrapKeyUpdates(b, ModeDirect, KeyToHexNibbleHash, nil, nil)
	defer upds.Close()
	ctx := context.Background()
	WrapKeyUpdatesInto(b, upds, pk, updates)
	_, err := hph.Process(ctx, upds, "", nil, WarmupConfig{})
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if i >= len(pk) {
			i = 0
		}
		// SetState(nil) fully clears grid, depths, touchMap, afterMap and root
		// so next Process unfolds from scratch exactly as a cold trie would.
		if err := hph.SetState(nil); err != nil {
			b.Fatal(err)
		}
		WrapKeyUpdatesInto(b, upds, pk[i:i+1], updates[i:i+1])
		_, err := hph.Process(ctx, upds, "", nil, WarmupConfig{})
		require.NoError(b, err)
	}
}
