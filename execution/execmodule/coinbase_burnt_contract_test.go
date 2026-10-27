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
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

func TestImportCoinbaseIsBurntContractWithSelfdestruct(t *testing.T) {
	t.Run("pre_amsterdam", func(t *testing.T) { testImportCoinbaseIsBurntContract(t, false, true) })
	t.Run("amsterdam", func(t *testing.T) { testImportCoinbaseIsBurntContract(t, true, true) })
}

func TestImportCoinbaseIsBurntContract(t *testing.T) {
	t.Run("pre_amsterdam", func(t *testing.T) { testImportCoinbaseIsBurntContract(t, false, false) })
	t.Run("amsterdam", func(t *testing.T) { testImportCoinbaseIsBurntContract(t, true, false) })
}

func testImportCoinbaseIsBurntContract(t *testing.T, amsterdam, selfdestruct bool) {
	t.Helper()

	config := chain.AllProtocolChanges.Copy()
	if !amsterdam {
		config.CancunTime = nil
		config.PragueTime = nil
		config.OsakaTime = nil
		config.AmsterdamTime = nil
	}

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

// Before Cancun a burnt-fee contract that self-destructs loses the burn of the same
// tx: serial credits it inside the tx, before the destruct takes effect. That holds
// also for a burnt-fee contract the tx creates.
func TestImportBurntContractSelfdestructLosesBurn(t *testing.T) {
	beneficiary := common.Address{0x44}
	destruct := append([]byte{byte(vm.PUSH20)}, beneficiary[:]...)
	destruct = append(destruct, byte(vm.SELFDESTRUCT))
	// factory CREATE2s a contract whose initcode is destruct.
	factory := common.Address{0x66}
	factoryCode := append([]byte{byte(vm.PUSH1) + byte(len(destruct)) - 1}, destruct...)
	factoryCode = append(factoryCode, byte(vm.PUSH1), 0, byte(vm.MSTORE),
		byte(vm.PUSH1), 0, byte(vm.PUSH1), byte(len(destruct)), byte(vm.PUSH1), byte(32-len(destruct)), byte(vm.PUSH1), 0,
		byte(vm.CREATE2), byte(vm.STOP))
	created := types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(destruct)))

	t.Run("existing contract", func(t *testing.T) {
		x := common.Address{0x33}
		importBurntContractBlock(t, x, x, types.GenesisAlloc{x: {Balance: big.NewInt(1), Code: destruct}})
	})
	t.Run("created in the tx", func(t *testing.T) {
		importBurntContractBlock(t, created, factory, types.GenesisAlloc{factory: {Code: factoryCode}})
	})
}

// importBurntContractBlock builds a pre-Cancun block serially with burnt as the
// burnt-fee contract and a tx to to, then imports it with parallel execution.
func importBurntContractBlock(t *testing.T, burnt, to common.Address, alloc types.GenesisAlloc) {
	prevParallel := dbg.Exec3Parallel
	t.Cleanup(func() { dbg.Exec3Parallel = prevParallel })

	config := chain.AllProtocolChanges.Copy()
	config.CancunTime = nil
	config.PragueTime = nil
	config.OsakaTime = nil
	config.AmsterdamTime = nil
	config.BurntContract = map[string]common.Address{"0": burnt}

	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	alloc[sender] = types.GenesisAccount{Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)}
	genesis := &types.Genesis{Config: config, Alloc: alloc}

	dbg.Exec3Parallel = false
	gen := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(key))
	signer := types.LatestSignerForChainID(config.ChainID)
	blocks, err := gen.GenerateChain(1, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{0x55})
		gasPrice := new(uint256.Int).Add(b.GetHeader().BaseFee, uint256.NewInt(1_000))
		for _, to := range []common.Address{to, {0xbe, 0xef}} {
			txn, err := types.SignTx(types.NewTransaction(b.TxNonce(sender), to, uint256.NewInt(0), 200_000, gasPrice, nil), *signer, key)
			require.NoError(t, err)
			b.AddTx(txn)
		}
	})
	require.NoError(t, err)

	dbg.Exec3Parallel = true
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(key))
	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), blocks.Blocks))
}
