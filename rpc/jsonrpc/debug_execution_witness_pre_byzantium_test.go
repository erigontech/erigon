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
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tests/testforks"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/internal/commitmenttest/commitmentflags"
	"github.com/erigontech/erigon/rpc"
)

func TestMPTWitnessPreByzantiumKeepsReceiptPostState(t *testing.T) {
	commitmentflags.Restore(t)
	previousAssert := dbg.AssertEnabled
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() { dbg.AssertEnabled = previousAssert })
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	from := crypto.PubkeyToAddress(key.PublicKey)
	config := testforks.Forks["SpuriousDragon"]
	genesis := &types.Genesis{Config: config, Difficulty: uint256.NewInt(1), GasLimit: 8_000_000, Alloc: types.GenesisAlloc{from: {Balance: big.NewInt(1_000_000_000_000_000_000)}}}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(key))
	require.NoError(t, m.DB.Update(t.Context(), func(tx kv.RwTx) error { return rawdb.WriteDBCommitmentHistoryEnabled(tx, true) }))
	pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
		txn, err := types.SignTx(types.NewTransaction(b.TxNonce(from), common.HexToAddress("0x1000000000000000000000000000000000000001"), uint256.NewInt(1), 21_000, uint256.NewInt(1), nil), *types.MakeSigner(config, 1, 0), key)
		require.NoError(t, err)
		b.AddTx(txn)
	})
	require.NoError(t, err)
	header := pack.Blocks[0].Header()
	postState := common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	header.ReceiptHash = types.DeriveSha(types.Receipts{&types.Receipt{PostState: postState[:], CumulativeGasUsed: header.GasUsed}})
	pack.Blocks[0] = pack.Blocks[0].WithSeal(header)
	pack.Headers[0] = pack.Blocks[0].HeaderNoCopy()
	pack.TopBlock = pack.Blocks[0]
	require.NoError(t, m.InsertChain(pack))
	api := newDebugApiForTest(m)
	dbg.AssertEnabled = true
	_, err = api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(1)), nil, nil)
	require.NoError(t, err)
}
