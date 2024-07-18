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

	"github.com/erigontech/erigon-lib/common/length"
)

func Benchmark_HexPatriciaHashed_ReviewKeys(b *testing.B) {
	ms := NewMockState(&testing.T{})
	ctx := context.Background()
	hph := NewHexPatriciaHashed(length.Addr, ms, ms.TempDir())
	hph.SetTrace(false)

	builder := NewUpdateBuilder()

	rnd := rand.New(rand.NewSource(133777))
	keysCount := rnd.Int31n(10_000_0)

	// generate updates
	for i := int32(0); i < keysCount; i++ {
		key := make([]byte, length.Addr)

		for j := 0; j < len(key); j++ {
			key[j] = byte(rnd.Intn(256))
		}
		builder.Balance(hex.EncodeToString(key), rnd.Uint64())
	}

	pk, _ := builder.Build()

	b.Run("review_keys", func(b *testing.B) {
		for i, j := 0, 0; i < b.N; i, j = i+1, j+1 {
			if j >= len(pk) {
				j = 0
			}

			hph.ProcessKeys(ctx, pk[j:j+1], "")
		}
	})
}
