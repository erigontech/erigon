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

package receipts_test

import (
	"math"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/receipts"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var rewardEscrow = accounts.InternAddress(common.HexToAddress("0xfffffffffffffffffffffffffffffffffffffffe"))

type systemTxEngine struct {
	rules.Engine
	systemContract accounts.Address
}

func (e systemTxEngine) IsSystemTransaction(txn types.Transaction, header *types.Header) (bool, error) {
	to := txn.GetTo()
	return to != nil && *to == e.systemContract.Value(), nil
}

func (e systemTxEngine) ApplySystemTx(txn types.Transaction, ibs *state.IntraBlockState, header *types.Header) error {
	if err := ibs.SubBalance(rewardEscrow, *txn.GetValue(), tracing.BalanceChangeUnspecified); err != nil {
		return err
	}
	return ibs.AddBalance(accounts.InternAddress(header.Coinbase), *txn.GetValue(), tracing.BalanceChangeUnspecified)
}

// A system transaction carries a gas limit far above the block's; replay must
// run it outside the block gas pool, as execution does.
func TestDeriveBlockReceiptsAppliesSystemTx(t *testing.T) {
	t.Parallel()

	// LOG0 with empty data: PUSH1 0, PUSH1 0, LOG0.
	code := hexutil.MustDecode("0x60006000a0")
	systemContract := accounts.InternAddress(common.HexToAddress("0x0000000000000000000000000000000000001000"))
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	validator := accounts.InternAddress(crypto.PubkeyToAddress(key.PublicKey))

	cfg := chain.TestChainBerlinConfig
	header := &types.Header{Number: *uint256.NewInt(1), GasLimit: 10_000_000, Difficulty: *uint256.NewInt(1), Coinbase: validator.Value()}
	signer := types.MakeSigner(cfg, 1, 0)
	reward := uint256.NewInt(1_000)
	userTx, err := types.SignTx(types.NewTransaction(0, systemContract.Value(), uint256.NewInt(0), 100_000, uint256.NewInt(1), nil), *signer, key)
	require.NoError(t, err)
	systemTx, err := types.SignTx(types.NewTransaction(1, systemContract.Value(), reward, math.MaxInt64, uint256.NewInt(0), nil), *signer, key)
	require.NoError(t, err)

	engine := systemTxEngine{Engine: ethash.NewFaker(), systemContract: systemContract}

	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	require.NoError(t, ibs.CreateAccount(systemContract, true))
	require.NoError(t, ibs.SetCode(systemContract, code, tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.AddBalance(validator, *uint256.NewInt(1_000_000_000), tracing.BalanceChangeUnspecified))
	require.NoError(t, ibs.AddBalance(rewardEscrow, *reward, tracing.BalanceChangeUnspecified))

	gp := new(protocol.GasPool).AddGas(header.GasLimit)
	got, err := receipts.DeriveBlockReceipts(t.Context(), cfg, engine, header, types.Transactions{userTx, systemTx}, ibs, gp, nil)
	require.NoError(t, err)
	require.Len(t, got, 2)

	sys := got[1]
	require.Equal(t, types.ReceiptStatusSuccessful, sys.Status)
	require.Len(t, sys.Logs, 1)
	require.Positive(t, sys.GasUsed)
	require.Less(t, sys.GasUsed, uint64(21_000))
	require.Equal(t, got[0].GasUsed+sys.GasUsed, sys.CumulativeGasUsed)

	nonce, err := ibs.GetNonce(validator)
	require.NoError(t, err)
	require.Equal(t, uint64(2), nonce)
	balance, err := ibs.GetBalance(systemContract)
	require.NoError(t, err)
	require.Equal(t, *reward, balance)
	escrow, err := ibs.GetBalance(rewardEscrow)
	require.NoError(t, err)
	require.True(t, escrow.IsZero())
}
