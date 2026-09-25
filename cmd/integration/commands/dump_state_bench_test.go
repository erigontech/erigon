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

package commands

import (
	"context"
	"io"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

func BenchmarkDumpStateToTSV(b *testing.B) {
	const accountCount = 20_000
	tx := seedManyAccounts(b, accountCount)
	logger := log.New()

	b.Run("count_only", func(b *testing.B) {
		for b.Loop() {
			res, err := dumpStateToTSV(context.Background(), tx, 1, true, 0, nil, io.Discard, logger)
			require.NoError(b, err)
			require.Equal(b, uint64(accountCount), res.Matched)
		}
	})

	b.Run("decode_no_write", func(b *testing.B) {
		for b.Loop() {
			res, err := dumpStateToTSV(context.Background(), tx, 1, true, 0, uint256.NewInt(0), io.Discard, logger)
			require.NoError(b, err)
			require.Equal(b, uint64(accountCount), res.Matched)
		}
	})

	b.Run("decode_and_write", func(b *testing.B) {
		for b.Loop() {
			res, err := dumpStateToTSV(context.Background(), tx, 1, false, 0, nil, io.Discard, logger)
			require.NoError(b, err)
			require.Equal(b, uint64(accountCount), res.Matched)
		}
	})

	// The codec is built once, as it is for a real dump: a zstd encoder allocates its
	// window up front, which would otherwise dominate at this payload size.
	for _, kind := range []string{"gzip", "zstd"} {
		b.Run("write_"+kind, func(b *testing.B) {
			comp, err := newCompressor(kind, io.Discard)
			require.NoError(b, err)
			defer comp.Close() //nolint:errcheck // benchmark teardown

			for b.Loop() {
				res, err := dumpStateToTSV(context.Background(), tx, 1, false, 0, nil, comp, logger)
				require.NoError(b, err)
				require.Equal(b, uint64(accountCount), res.Matched)
			}
		})
	}
}
