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
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/consensuschain"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/internal/stateless"
	"github.com/erigontech/erigon/eez/protocol"
	execprotocol "github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

// branchingReaderCase ends a group, then calls a reader that reads the
// balance of one of two accounts as EEZL2's balance is zero or not: a witness
// recorded without the burn holds the other account.
func branchingReaderCase() groupEndCase {
	eezl2 := protocol.RollupXEEZL2Address
	reader, unburnt, burnt := common.HexToAddress("0x8eb0"), common.HexToAddress("0x8eb1"), common.HexToAddress("0x8eb2")
	code := append([]byte{byte(vm.PUSH20)}, eezl2[:]...)
	code = append(code, byte(vm.BALANCE), byte(vm.ISZERO), byte(vm.PUSH1), 52, byte(vm.JUMPI))
	code = append(storeOf(code, unburnt, vm.BALANCE, 0), byte(vm.STOP), byte(vm.JUMPDEST))
	code = append(storeOf(code, burnt, vm.BALANCE, 0), byte(vm.STOP))
	alloc := types.GenesisAlloc{
		derivertest.UserAddress(): groupEndRich,
		reader:                    {Code: code},
		unburnt:                   {Balance: big.NewInt(1)},
		burnt:                     {Balance: big.NewInt(2)},
	}
	recipient := common.HexToAddress("0x7ec1")
	return groupEndCase{"reader branching on the burn", alloc, eezl2, func(c *groupEndChain) []types.Transaction {
		return []types.Transaction{
			c.nativeTx(protocol.LoadExecutionTableSelector, 0),
			c.userTx(derivertest.UserKey, 0, recipient, 0, 21_000),
			c.userTx(derivertest.UserKey, 1, reader, 0, 100_000),
		}
	}}
}

// TestGroupEndBurnStateless checks that the witness of a v0.1 block proves it,
// to its own stateless check and to the stateless executor, when a group-end
// burn changes what a later transaction reads.
func TestGroupEndBurnStateless(t *testing.T) {
	// The witness reads the parent's commitment from history.
	schema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() { statecfg.Schema = schema })
	cases := []groupEndCase{branchingReaderCase()}
	for _, tc := range afterFeesCases() {
		cases = append(cases, tc.groupEndCase)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newGroupEndChain(t, tc.alloc)
			m, _, built := c.build(tc.coinbase, tc.txs(c))
			require.NoError(t, m.DB.Update(t.Context(), func(tx kv.RwTx) error {
				return rawdb.WriteDBCommitmentHistoryEnabled(tx, true)
			}))
			api := jsonrpc.NewPrivateDebugAPI(
				jsonrpc.NewBaseApi(nil, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs}),
				m.DB, nil, &rpccfg.DebugApiConfig{})
			number := rpc.BlockNumber(1)
			// The witness checks itself statelessly under assertions only.
			res, err := func() (*jsonrpc.ExecutionWitnessResult, error) {
				defer func(prev bool) { dbg.AssertEnabled = prev }(dbg.AssertEnabled)
				dbg.AssertEnabled = true
				return api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHash{BlockNumber: &number}, nil)
			}()
			require.NoError(t, err)
			raw := func(bs []hexutil.Bytes) [][]byte {
				out := make([][]byte, len(bs))
				for i, b := range bs {
					out[i] = b
				}
				return out
			}
			w := &stateless.Witness{State: raw(res.State), Codes: raw(res.Codes), Headers: raw(res.Headers)}
			_, err = stateless.Execute(m.ChainConfig, m.Engine, built.Block, w)
			require.NoError(t, err)
		})
	}
}

// TestGroupEndBurnReceipts checks that receipts regenerated for a v0.1 block
// carry the gas its later transactions used on the burnt state.
func TestGroupEndBurnReceipts(t *testing.T) {
	for _, tc := range afterFeesCases() {
		t.Run(tc.name, func(t *testing.T) {
			c := newGroupEndChain(t, tc.alloc)
			chain, built := c.serial(tc.coinbase, tc.txs(c))
			got, err := chain.Receipts(t.Context(), built.Block)
			require.NoError(t, err)
			require.Len(t, got, len(built.Receipts))
			for i, want := range built.Receipts {
				require.Equal(t, want.Status, got[i].Status, "transaction %d", i)
				require.Equal(t, want.GasUsed, got[i].GasUsed, "transaction %d", i)
				require.Equal(t, want.CumulativeGasUsed, got[i].CumulativeGasUsed, "transaction %d", i)
			}
		})
	}
}

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
