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

package stagedsync

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// pinnedHistoryReader reads the account nonce history at its txNum, as a restart-replay worker does.
type pinnedHistoryReader struct {
	*state.NoopReader
	addr  accounts.Address
	txNum uint64
	nonce func(txNum uint64) uint64
}

func (r *pinnedHistoryReader) GetTxNum() uint64      { return r.txNum }
func (r *pinnedHistoryReader) SetTxNum(txNum uint64) { r.txNum = txNum }
func (r *pinnedHistoryReader) ReadsHistory()         {}
func (r *pinnedHistoryReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	if addr != r.addr {
		return nil, nil
	}
	acc := accounts.NewAccount()
	acc.Nonce = r.nonce(r.txNum)
	return &acc, nil
}

// A history-mode worker reads at its own txNum, which already holds the writes of the block's
// earlier tasks; the origin it seeds must still be the account the block's first task reads.
func TestHistoryTaskSeedsTheFirstTaskOrigin(t *testing.T) {
	const blockStart = 100
	addr := accounts.InternAddress([20]byte{0x25, 0x75, 0x6})
	for _, c := range []struct {
		wrapped   bool
		firstTask int // txIndex of the block's first executed task; a resumed block skips the committed prefix
		want      uint64
	}{{false, -1, 3}, {true, -1, 3}, {false, 1, 4}} {
		wrapped := c.wrapped
		versionMap := state.NewVersionMap(nil)
		read := func(txIndex int) uint64 {
			reader := &pinnedHistoryReader{NoopReader: state.NewNoopReader(), addr: addr, nonce: func(txNum uint64) uint64 {
				switch {
				case txNum <= blockStart:
					return 3
				case txNum <= blockStart+2: // after txIndex 0
					return 4
				default: // after txIndex 1 or 2
					return 5
				}
			}}
			var sr state.StateReader = reader
			if wrapped {
				sr = state.NewPrevBlockReader(reader, state.NewPrevBlockList())
			}
			ibs := state.New(sr)
			defer ibs.Close()
			task := &taskVersion{
				execTask:   &execTask{Task: &exec.TxTask{Header: &types.Header{Number: *uint256.NewInt(1)}, TxIndex: -1, Logger: log.New()}, index: txIndex - c.firstTask},
				version:    state.Version{BlockNum: 1, TxNum: blockStart + uint64(txIndex) + 1, TxIndex: txIndex},
				versionMap: versionMap,
			}
			reader.SetTxNum(task.version.TxNum)
			require.NoError(t, task.Reset(&vm.EVM{}, ibs, nil))
			nonce, err := ibs.GetNonce(addr)
			require.NoError(t, err)
			_, err = ibs.FinalizedWrites(&chain.Rules{})
			require.NoError(t, err)
			return nonce
		}
		read(3)
		require.Equal(t, c.want, read(1), "%+v: txIndex 1 read the origin a later tx published", c)
	}
}
