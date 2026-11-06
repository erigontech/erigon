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

package rules_test

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/consensuschain"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/eez/protocol"
	execprotocol "github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// balanceWrites records the balances a state writer receives for addr.
type balanceWrites struct {
	*state.NoopWriter
	addr     accounts.Address
	balances []uint256.Int
}

func (w *balanceWrites) UpdateAccountData(address accounts.Address, _, account *accounts.Account) error {
	if address == w.addr {
		w.balances = append(w.balances, account.Balance)
	}
	return nil
}

// TestGroupEndBurnEphemeralWrites checks that ephemeral execution hands its
// state writer each transaction's state as canonical execution does: with
// EEZL2 as coinbase, the tip of the transaction ending a group is burnt with
// it and never reaches the writer, and stays without a group end.
func TestGroupEndBurnEphemeralWrites(t *testing.T) {
	for _, tc := range afterFeesCases() {
		if tc.tipped < 0 {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			c := newGroupEndChain(t, tc.alloc)
			m, _, built := c.build(tc.coinbase, tc.txs(c))
			w := &balanceWrites{NoopWriter: state.NewNoopWriter(), addr: accounts.InternAddress(protocol.RollupXEEZL2Address)}
			require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
				start, err := m.BlockReader.TxnumReader().Min(t.Context(), tx, 1)
				if err != nil {
					return err
				}
				header := built.Block.Header()
				getHeader := func(hash common.Hash, number uint64) (*types.Header, error) {
					return m.BlockReader.Header(t.Context(), tx, hash, number)
				}
				_, err = execprotocol.ExecuteBlockEphemerally(m.ChainConfig, &vm.Config{}, execprotocol.GetHashFn(header, getHeader),
					m.Engine, built.Block, state.NewHistoryReaderV3(tx, start), w,
					consensuschain.NewReader(m.ChainConfig, tx, m.BlockReader, log.New()), nil, log.New())
				return err
			}))
			tip := new(uint256.Int).Mul(uint256.NewInt(groupEndTipCap), uint256.NewInt(built.Receipts[tc.tipped].GasUsed))
			written := false
			for _, b := range w.balances {
				written = written || b.Eq(tip)
			}
			require.Equal(t, !tc.burnt, written, "tip %v among EEZL2 balances written %v", tip, w.balances)
		})
	}
}
