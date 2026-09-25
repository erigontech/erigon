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

type overrideEngine struct {
	rules.Engine
	blockNum  uint64
	txIndex   int
	overrides []state.StorageOverride
}

func (e overrideEngine) StorageOverrides(blockNum uint64, txIndex int) []state.StorageOverride {
	if blockNum != e.blockNum || txIndex != e.txIndex {
		return nil
	}
	return e.overrides
}

// A regenerated receipt must reproduce a patched transaction's canonical gas,
// and the transaction after it must see the state the patched one left behind.
func TestDeriveBlockReceiptsAppliesStorageOverrides(t *testing.T) {
	t.Parallel()

	// slot[0] += 1: PUSH1 0, SLOAD, PUSH1 1, ADD, PUSH1 0, SSTORE.
	code := hexutil.MustDecode("0x600054600101600055")
	contract := accounts.InternAddress(common.HexToAddress("0x89791428868131eb109e42340ad01eb8987526b2"))
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	sender := accounts.InternAddress(crypto.PubkeyToAddress(key.PublicKey))

	cfg := chain.TestChainBerlinConfig
	header := &types.Header{Number: *uint256.NewInt(1), GasLimit: 10_000_000, Difficulty: *uint256.NewInt(1)}
	signer := types.MakeSigner(cfg, 1, 0)
	txns := make(types.Transactions, 2)
	for nonce := range txns {
		txns[nonce], err = types.SignTx(types.NewTransaction(uint64(nonce), contract.Value(), uint256.NewInt(0), 100_000, uint256.NewInt(1), nil), *signer, key)
		require.NoError(t, err)
	}

	engine := overrideEngine{
		Engine:    ethash.NewFaker(),
		blockNum:  1,
		txIndex:   0,
		overrides: []state.StorageOverride{{Address: contract, Key: accounts.ZeroKey, Value: *uint256.NewInt(5)}},
	}

	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	require.NoError(t, ibs.CreateAccount(contract, true))
	require.NoError(t, ibs.SetCode(contract, code, tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.AddBalance(sender, *uint256.NewInt(1_000_000_000), tracing.BalanceChangeUnspecified))

	gp := new(protocol.GasPool).AddGas(header.GasLimit)
	got, err := receipts.DeriveBlockReceipts(t.Context(), cfg, engine, header, txns, ibs, gp, nil)
	require.NoError(t, err)
	require.Len(t, got, 2)

	// Both transactions reset a non-zero slot to another non-zero value; without
	// the override the first one would pay for setting a zero slot instead.
	require.Equal(t, got[1].GasUsed, got[0].GasUsed)
	require.Equal(t, 2*got[0].GasUsed, got[1].CumulativeGasUsed)

	value, err := ibs.GetState(contract, accounts.ZeroKey)
	require.NoError(t, err)
	require.Equal(t, uint64(7), value.Uint64())
}
