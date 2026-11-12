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
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
)

// Recreating identical storage must replace its pending branch tombstone. The
// next block must still be able to read and update the persisted branch.
func TestRecreatedStorageBranchSurvivesNextBlock(t *testing.T) {
	victimCode := common.FromHex("3615601d57366020146013576020355f3555005b5f35545f5260205ff35b33ff")
	initCode := program.New().Sstore(0, 7).ReturnViaCodeCopy(victimCode).Bytes()
	factory := common.HexToAddress("0xfac1")
	factoryCode := program.New().Create2(initCode, 0).Op(vm.STOP).Bytes()
	victim := types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(initCode)))

	create := revivalCall{to: factory}
	write := revivalCall{to: victim, data: common.FromHex(fmt.Sprintf("%064x%064x", 0x32, 0x593d))}
	destroy := revivalCall{to: victim}
	touch := revivalCall{to: victim, data: common.FromHex(fmt.Sprintf("%064x%064x", 0x32, 0x1234))}
	blocks := [][]revivalCall{{create, write}, {destroy, create, write}, {touch}}

	config := chain.TestChainOsakaConfig.Copy()
	config.CancunTime, config.PragueTime, config.OsakaTime = nil, nil, nil

	alloc := types.GenesisAlloc{factory: {Nonce: 1, Code: factoryCode}}
	keys := revivalKeys(t, alloc, 6)
	gspec := &types.Genesis{Config: config, GasLimit: 30_000_000, Alloc: alloc}

	gen := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(keys[0]))
	signer := types.LatestSignerForChainID(config.ChainID)
	chainPack, err := gen.GenerateChain(len(blocks), func(i int, b *blockgen.BlockGen) {
		for j, call := range blocks[i] {
			sender := crypto.PubkeyToAddress(keys[j].PublicKey)
			txn, err := types.SignTx(types.NewTransaction(b.TxNonce(sender), call.to, uint256.NewInt(0), 1_000_000, uint256.NewInt(100_000_000_000), call.data), *signer, keys[j])
			require.NoError(t, err)
			b.AddTx(txn)
		}
	})
	require.NoError(t, err)

	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(keys[0]))
	for blk := range len(blocks) {
		require.NoErrorf(t, m.InsertChain(chainPack.Slice(blk, blk+1)), "insert block %d", blk+1)
	}
	require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		defer st.Close()
		require.Equal(t, *uint256.NewInt(7), storageAt(t, st, victim, 0))
		require.Equal(t, *uint256.NewInt(0x1234), storageAt(t, st, victim, 0x32))
		return nil
	}))
}
