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

package stagedsync_test

import (
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
)

func BenchmarkSendersStage(b *testing.B) {
	for _, shape := range []struct{ blocks, txs int }{{2000, 4}, {100, 200}} {
		b.Run(fmt.Sprintf("blocks=%d/txs=%d", shape.blocks, shape.txs), func(b *testing.B) {
			m := execmoduletester.New(b)
			key, err := crypto.GenerateKey()
			require.NoError(b, err)
			signer := types.MakeSigner(chain.TestChainBerlinConfig, 1, 0)
			tx, err := m.DB.BeginRw(m.Ctx)
			require.NoError(b, err)
			defer tx.Rollback()
			for n := 1; n <= shape.blocks; n++ {
				txns := make([]types.Transaction, shape.txs)
				for i := range txns {
					txns[i], err = types.SignTx(&types.LegacyTx{
						CommonTx: types.CommonTx{Nonce: uint64(n*shape.txs + i), GasLimit: 21_000, Value: u256.Num1},
						GasPrice: u256.Num1,
					}, *signer, key)
					require.NoError(b, err)
				}
				header := &types.Header{Number: *uint256.NewInt(uint64(n))}
				hash := header.Hash()
				require.NoError(b, rawdb.WriteHeader(tx, header))
				require.NoError(b, rawdb.WriteBody(tx, hash, uint64(n), &types.Body{Transactions: txns}))
				require.NoError(b, rawdb.WriteCanonicalHash(tx, hash, uint64(n)))
			}
			require.NoError(b, stages.SaveStageProgress(tx, stages.Bodies, uint64(shape.blocks)))
			require.NoError(b, tx.Commit())

			cfg := stagedsync.StageSendersCfg(chain.TestChainBerlinConfig, true, b.TempDir(), m.BlockReader, exec.NewBlockReadAheader())
			runStage := func() error {
				tx, err := m.DB.BeginRw(m.Ctx)
				if err != nil {
					return err
				}
				defer tx.Rollback()
				return stagedsync.SpawnRecoverSendersStage(cfg, &stagedsync.StageState{ID: stages.Senders}, nil, tx, uint64(shape.blocks), m.Ctx, log.New())
			}
			for b.Loop() {
				require.NoError(b, runStage())
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(shape.blocks*shape.txs), "ns/tx")
		})
	}
}
