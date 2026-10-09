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
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
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

// Before Cancun, a coinbase contract destroyed in the block is revived as a new
// account by a later tx's fee credit. Parallel execution must accept the block
// built by the serial generator.
func TestFeeCreditRevivesDestroyedCoinbase(t *testing.T) {
	prev := dbg.Exec3Parallel
	t.Cleanup(func() { dbg.Exec3Parallel = prev })
	dbg.Exec3Parallel = false

	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	runtime := program.New().Op(vm.CALLER, vm.SELFDESTRUCT).Bytes()
	initCode := program.New().ReturnViaCodeCopy(runtime).Bytes()
	factory := common.HexToAddress("0xaaaa")
	contract := types.CreateAddress2(factory, [32]byte{31: 1}, accounts.InternCodeHash(crypto.Keccak256Hash(initCode)))

	config := chain.TestChainOsakaConfig.Copy()
	config.CancunTime, config.PragueTime, config.OsakaTime = nil, nil, nil
	gspec := &types.Genesis{Config: config, GasLimit: 30_000_000, Alloc: types.GenesisAlloc{
		sender:  {Balance: big.NewInt(1_000_000_000_000_000_000)},
		factory: {Code: program.New().Create2(initCode, 1).Op(vm.POP).Bytes()},
	}}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))
	signer := types.LatestSignerForChainID(config.ChainID)
	chainPack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
		b.SetCoinbase(contract)
		// Deploy, destroy, then tip the destroyed coinbase with a plain transfer.
		for _, to := range []common.Address{factory, contract, common.HexToAddress("0xbbbb")} {
			txn, err := types.SignTx(types.NewTransaction(b.TxNonce(sender), to, uint256.NewInt(1), 500_000, uint256.NewInt(10_000_000_000), nil), *signer, key)
			require.NoError(t, err)
			b.AddTx(txn)
		}
	})
	require.NoError(t, err)

	dbg.Exec3Parallel = true
	require.NoError(t, m.InsertChain(chainPack))
	require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		defer st.Close()
		hash, err := st.GetCodeHash(accounts.InternAddress(contract))
		require.NoError(t, err)
		require.Equal(t, accounts.EmptyCodeHash, hash)
		return nil
	}))
}
