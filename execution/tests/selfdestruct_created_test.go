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
	"crypto/ecdsa"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
)

var sdcFactoryAddr = common.HexToAddress("0xfac7")

// sdcFactory CREATEs contracts whose runtime is COINBASE SELFDESTRUCT and calls
// each one, until less than 100k gas is left.
func sdcFactory() []byte {
	initCode := program.New().ReturnData([]byte{byte(vm.COINBASE), byte(vm.SELFDESTRUCT)}).Bytes()
	p, loop := program.New().Mstore(initCode, 0).Jumpdest()
	p.Push(len(initCode)).Push(0).Push(0).Op(vm.CREATE)
	p.Push(0).Push(0).Push(0).Push(0).Push(0).Op(vm.DUP6).Op(vm.GAS).Op(vm.CALL).Op(vm.POP, vm.POP)
	return p.Push(100_000).Op(vm.GAS, vm.GT).Push(loop).Op(vm.JUMPI, vm.STOP).Bytes()
}

func sdcChain(tb testing.TB, txs int, funded ...common.Address) (*types.Genesis, *ecdsa.PrivateKey, *blockgen.ChainPack) {
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(tb, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	config := chain.TestChainOsakaConfig.Copy()
	config.AmsterdamTime = common.NewUint64(0)
	gspec := &types.Genesis{
		Config:   config,
		GasLimit: 300_000_000,
		Alloc: types.GenesisAlloc{
			sender:         {Balance: new(big.Int).Lsh(big.NewInt(1), 80)},
			sdcFactoryAddr: {Code: sdcFactory()},
		},
	}
	for _, addr := range funded {
		gspec.Alloc[addr] = types.GenesisAccount{Balance: big.NewInt(1)}
	}
	signer := types.LatestSignerForChainID(config.ChainID)
	m := execmoduletester.New(tb, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))
	chainPack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
		for range txs {
			txn, err := types.SignTx(types.NewTransaction(b.TxNonce(sender), sdcFactoryAddr, uint256.NewInt(0), 16_000_000, uint256.NewInt(10_000_000_000), nil), *signer, key)
			require.NoError(tb, err)
			b.AddTx(txn)
		}
	})
	require.NoError(tb, err)
	for _, r := range chainPack.Receipts[0] {
		require.Equal(tb, uint64(types.ReceiptStatusSuccessful), r.Status)
	}
	return gspec, key, chainPack
}

// Each tx creates and destroys many contracts; the state root and the BAL hash
// pin the block, and the parallel import must agree with it.
func TestSelfdestructCreatedBlockIsPinned(t *testing.T) {
	gspec, key, chainPack := sdcChain(t, 3)
	header := chainPack.TopBlock.Header()
	require.NotNil(t, header.BlockAccessListHash)
	require.Equal(t, "0x097110a8b33689ceae80606b17e7e76de823d2ff18f87fac65b12f144b427347", header.Root.Hex())
	require.Equal(t, "0xd8d1b2d1f860afec75577b81d2cf0b7d85c6f05c94e3212ab9667881d60ce852", header.BlockAccessListHash.Hex())
	require.Equal(t, uint64(43678440), header.GasUsed)
	importInParallel(t, gspec, key, chainPack, func(t *testing.T, st *state.IntraBlockState) {
		nonce, err := st.GetNonce(accounts.InternAddress(sdcFactoryAddr))
		require.NoError(t, err)
		require.Equal(t, uint64(234), nonce)
		exist, err := st.Exist(accounts.InternAddress(types.CreateAddress(sdcFactoryAddr, 5)))
		require.NoError(t, err)
		require.False(t, exist)
	})
}

// A contract created and destroyed at an address that held a balance before the
// tx removes the account: its balance goes to the beneficiary.
func TestSelfdestructCreatedAtFundedAddressIsRemoved(t *testing.T) {
	funded := types.CreateAddress(sdcFactoryAddr, 5)
	gspec, key, chainPack := sdcChain(t, 1, funded)
	require.Equal(t, "0x4235d16b26fcf278875db8f015b5525ecd5480b35ca0880e63be4f66ba98c5a3", chainPack.TopBlock.Root().Hex())
	importInParallel(t, gspec, key, chainPack, func(t *testing.T, st *state.IntraBlockState) {
		exist, err := st.Exist(accounts.InternAddress(funded))
		require.NoError(t, err)
		require.False(t, exist)
	})
}
