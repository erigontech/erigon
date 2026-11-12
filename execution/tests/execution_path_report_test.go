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

package executiontests

import (
	"sync"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/node/ethconfig"
)

func TestExecutionPathReporter(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	_, chain, err := GenerateBlocks(t, data.genesisSpec, map[int]txn{
		0: {getBlockTx(from, common.Address{1}, uint256.NewInt(1000)), fromKey},
	})
	require.NoError(t, err)
	block := chain.Blocks[0]

	cases := []struct {
		name    string
		workers int
		serial  bool
		want    ethconfig.BlockExecutionPath
	}{
		{"parallel", 2, false, ethconfig.BlockExecutionPath{Path: "parallel"}},
		{"one worker", 1, false, ethconfig.BlockExecutionPath{Path: "sequential", Reason: "single-worker"}},
		{"serial exec", 2, true, ethconfig.BlockExecutionPath{Path: "sequential", Reason: "serial-exec"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.serial {
				prevParallel := dbg.Exec3Parallel
				dbg.Exec3Parallel = false
				t.Cleanup(func() { dbg.Exec3Parallel = prevParallel })
			}
			var (
				mu      sync.Mutex
				reports []ethconfig.BlockExecutionPath
			)
			m := execmoduletester.New(t,
				execmoduletester.WithGenesisSpec(data.genesisSpec),
				execmoduletester.WithKey(fromKey),
				execmoduletester.WithExecWorkerCount(tc.workers),
				execmoduletester.WithExecutionPathReporter(func(p ethconfig.BlockExecutionPath) {
					mu.Lock()
					defer mu.Unlock()
					reports = append(reports, p)
				}),
			)
			require.NoError(t, m.InsertChain(chain))

			want := tc.want
			want.Number = block.NumberU64()
			want.Hash = block.Hash()
			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, []ethconfig.BlockExecutionPath{want}, reports)
		})
	}
}
