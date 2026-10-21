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

package state_test

import (
	"bytes"
	"fmt"
	"runtime"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func pbinRangeWriterBenchLeaves(accounts int) []state.PBinLeaf {
	leaves := make([]state.PBinLeaf, 0, 4*accounts)
	for i := range accounts {
		address := make([]byte, 20)
		address[0], address[1], address[2] = byte(i>>16), byte(i>>8), byte(i)
		seed := byte(i%250 + 1)
		leaves = append(leaves,
			state.PBinLeaf{Key: eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey), Value: pbinRangeWriterBasicValue(seed), Stamp: uint64(i % 24)},
			state.PBinLeaf{Key: eip8297.TreeKeyAccount(address, eip8297.HeaderStorageOffset), Value: pbinRangeWriterHeaderValue(seed), Stamp: uint64((i + 3) % 24)},
			state.PBinLeaf{Key: eip8297.TreeKeyCodeChunk(common.BytesToHash(append(bytes.Repeat([]byte{seed}, 29), address[:3]...)), 0), Value: pbinRangeWriterCodeValue(seed), Stamp: uint64((i + 6) % 24)},
			state.PBinLeaf{Key: eip8297.TreeKeyStorage(address, []byte{seed}), Value: pbinRangeWriterValue(seed), Stamp: uint64((i + 9) % 24)},
		)
	}
	sort.Slice(leaves, func(i, j int) bool { return bytes.Compare(leaves[i].Key, leaves[j].Key) < 0 })
	return leaves
}

func BenchmarkPBinRangeWriterWorkers(b *testing.B) {
	selectPBinRangeWriterHash(b)
	leaves := pbinRangeWriterBenchLeaves(75_000)
	for _, workers := range []int{1, 8} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			factoryWorkers := workers
			if workers == 1 {
				factoryWorkers = 0
			}
			b.StopTimer()
			for range b.N {
				run := openPBinRangeWriterRun(b, 100_000, factoryWorkers, 23)
				runtime.GC()
				b.StartTimer()
				_, err := run.write(b.Context(), leaves)
				b.StopTimer()
				require.NoError(b, err)
				if factoryWorkers > 0 {
					require.Greater(b, run.activity.Load(), int32(1))
				}
			}
		})
	}
}

func BenchmarkPBinVerifyWorkers(b *testing.B) {
	selectPBinRangeWriterHash(b)
	statecfg.ExperimentalCommitmentV3 = true
	run := openPBinRangeWriterRun(b, 100_000, 0, 23)
	_, err := run.write(b.Context(), pbinRangeWriterBenchLeaves(75_000))
	require.NoError(b, err)
	run.tx.Rollback()
	workers := dbg.TipTrieWarmupers
	b.Cleanup(func() { dbg.TipTrieWarmupers = workers })
	for _, workers := range []int{1, 8} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			dbg.TipTrieWarmupers = workers
			for b.Loop() {
				tx, err := run.db.BeginTemporalRo(b.Context()) //nolint:gocritic
				require.NoError(b, err)
				_, err = state.VerifyPBinDomainRoot(b.Context(), tx, run.agg, kv.CommitmentBinDomain)
				tx.Rollback()
				require.NoError(b, err)
			}
		})
	}
}
