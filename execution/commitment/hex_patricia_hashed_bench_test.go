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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common/length"
)

func Benchmark_HexPatriciaHashed_Process(b *testing.B) {
	b.SetParallelism(1)
	metricsFile = "./ericom"

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
	ms := NewMockState(&testing.T{})
	err := ms.applyPlainUpdates(pk, updates)
	require.NoError(b, err)

	hph := NewHexPatriciaHashed(length.Addr, ms)
	upds := WrapKeyUpdates(b, ModeDirect, KeyToHexNibbleHash, nil, nil)
	defer upds.Close()

	b.ResetTimer()

	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		if i+5 >= len(pk) {
			i = 0
		}

		WrapKeyUpdatesInto(b, upds, pk[i:i+5], updates[i:i+5])
		_, err := hph.Process(ctx, upds, "")
		require.NoError(b, err)
	}
}
