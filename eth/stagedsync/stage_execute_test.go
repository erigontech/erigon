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

package stagedsync

import (
	"context"
	"math/big"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	libstate "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/core/exec"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/params"
)

func apply(tx kv.RwTx, logger log.Logger) (beforeBlock, afterBlock testGenHook, w state.StateWriter) {
	domains, err := libstate.NewSharedDomains(tx, logger)
	if err != nil {
		panic(err)
	}
	rs := state.NewStateV3Buffered(state.NewStateV3(domains, logger))
	stateWriter := state.NewStateWriterBufferedV3(rs, nil)
	stateWriter.SetTx(tx)

	return func(n, from, numberOfBlocks uint64) {
			stateWriter.SetTxNum(context.Background(), n)
			stateWriter.ResetWriteSet()
		}, func(n, from, numberOfBlocks uint64) {
			txTask := &exec.TxTask{
				TxNum:   n,
				TxIndex: 0,
				Header: &types.Header{
					Number: big.NewInt(int64(n)),
				},
			}
			// Unused
			//txTask.AccountPrevs, txTask.AccountDels, txTask.StoragePrevs, txTask.CodePrevs = stateWriter.PrevAndDels()
			rs.SetTxNum(txTask.TxNum, txTask.BlockNumber())
			if err := rs.ApplyState4(context.Background(), tx, txTask.BlockNumber(), txTask.TxNum, nil, txTask.BalanceIncreaseSet,
				nil, nil, nil, txTask.Config, params.TestRules, txTask.HistoryExecution); err != nil {
				panic(err)
			}
			_, err := rs.Domains().ComputeCommitment(context.Background(), tx, true, txTask.BlockNumber(), "")
			if err != nil {
				panic(err)
			}

			if n == from+numberOfBlocks-1 {
				if err := domains.Flush(context.Background(), tx, false); err != nil {
					panic(err)
				}
			}
		}, stateWriter
}
