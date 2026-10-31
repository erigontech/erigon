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

package jsonrpc

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

func TestTraceTransactionSelfDestructDepth(t *testing.T) {
	contract := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	callee := common.HexToAddress("0x00000000000000000000000000000000000000bb")
	beneficiary := common.HexToAddress("0x00000000000000000000000000000000000000cc")

	code := []byte{0x60, 0x00, 0x60, 0x00, 0x60, 0x00, 0x60, 0x00, 0x60, 0x00, 0x73}
	code = append(code, callee[:]...)
	code = append(code, 0x5a, 0xf1, 0x50, 0x73)
	code = append(code, beneficiary[:]...)
	code = append(code, 0xff)

	m := execmoduletester.New(
		t,
		execmoduletester.WithGenesisSpec(&types.Genesis{
			Config: chain.TestChainBerlinConfig,
			Alloc: types.GenesisAlloc{
				testAddr: {Balance: big.NewInt(1_000_000_000_000)},
				contract: {Balance: big.NewInt(7), Code: code},
			},
		}),
		execmoduletester.WithKey(testKey),
	)
	signer := types.LatestSignerForChainID(nil)
	var txHash common.Hash
	c, err := m.GenerateChain(1, func(i int, block *blockgen.BlockGen) {
		txn, err := types.SignTx(types.NewTransaction(block.TxNonce(testAddr), contract, uint256.NewInt(0), 200_000, uint256.NewInt(1), nil), *signer, testKey)
		require.NoError(t, err)
		block.AddTx(txn)
		txHash = txn.Hash()
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(c))

	api := NewOtterscanAPI(newBaseApiForTest(m), m.DB, 25)
	entries, err := api.TraceTransaction(m.Ctx, txHash)
	require.NoError(t, err)
	require.Len(t, entries, 3)

	require.Equal(t, "CALL", entries[0].Type)
	require.Equal(t, 0, entries[0].Depth)
	require.Equal(t, "CALL", entries[1].Type)
	require.Equal(t, 1, entries[1].Depth)
	require.Equal(t, callee, entries[1].To)
	require.Equal(t, "SELFDESTRUCT", entries[2].Type)
	require.Equal(t, 1, entries[2].Depth, "SELFDESTRUCT is issued by the top-level frame, not by the preceding sibling call")
	require.Equal(t, contract, entries[2].From)
	require.Equal(t, beneficiary, entries[2].To)
}
