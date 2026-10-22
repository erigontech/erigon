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

package execmodule_test

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
)

func TestImportCoinbaseIsBurntContractWithSelfdestruct(t *testing.T) {
	testImportCoinbaseIsBurntContract(t, true)
}

func TestImportCoinbaseIsBurntContract(t *testing.T) {
	testImportCoinbaseIsBurntContract(t, false)
}

func testImportCoinbaseIsBurntContract(t *testing.T, selfdestruct bool) {
	t.Helper()

	config := chain.AllProtocolChanges.Copy()
	config.CancunTime = nil
	config.PragueTime = nil
	config.OsakaTime = nil
	config.AmsterdamTime = nil

	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	x := common.Address{0x33}
	beneficiary := common.Address{0x44}
	config.BurntContract = map[string]common.Address{"0": x}

	code := []byte{byte(vm.STOP)}
	if selfdestruct {
		code = append([]byte{byte(vm.PUSH20)}, beneficiary[:]...)
		code = append(code, byte(vm.SELFDESTRUCT))
	}
	genesis := &types.Genesis{
		Config: config,
		Alloc: types.GenesisAlloc{
			sender: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
			x:      {Balance: big.NewInt(1), Code: code},
		},
	}

	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(key))
	signer := types.LatestSignerForChainID(config.ChainID)
	blocks, err := m.GenerateChain(2, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(x)
		gasPrice := new(uint256.Int).Add(b.GetHeader().BaseFee, uint256.NewInt(1_000))
		txn, err := types.SignTx(types.NewTransaction(uint64(i), x, uint256.NewInt(0), 100_000, gasPrice, nil), *signer, key)
		require.NoError(t, err)
		b.AddTx(txn)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), blocks.Blocks))
}
