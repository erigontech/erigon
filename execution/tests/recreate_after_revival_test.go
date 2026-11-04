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
	"fmt"
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
	"github.com/erigontech/erigon/node/ethconfig"
)

// buildBlockSerially builds one block with serial execution. Each call is a tx
// from its own sender.
func buildBlockSerially(t *testing.T, gspec *types.Genesis, keys []*ecdsa.PrivateKey, coinbase common.Address, gasPrice uint64, calls []revivalCall) *blockgen.ChainPack {
	t.Helper()
	prevParallel := dbg.Exec3Parallel
	defer func() { dbg.Exec3Parallel = prevParallel }()
	dbg.Exec3Parallel = false
	signer := types.LatestSignerForChainID(gspec.Config.ChainID)
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(keys[0]))
	chainPack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
		b.SetCoinbase(coinbase)
		for i, call := range calls {
			sender := crypto.PubkeyToAddress(keys[i].PublicKey)
			txn, err := types.SignTx(types.NewTransaction(b.TxNonce(sender), call.to, uint256.NewInt(call.value), 1_000_000, uint256.NewInt(gasPrice), call.data), *signer, keys[i])
			require.NoError(t, err)
			b.AddTx(txn)
		}
	})
	require.NoError(t, err)
	for _, receipt := range chainPack.Receipts[0] {
		require.Equal(t, uint64(types.ReceiptStatusSuccessful), receipt.Status)
	}
	return chainPack
}

type revivalCall struct {
	to    common.Address
	value uint64
	data  []byte
}

// importInParallel imports the block with parallel execution at several worker
// counts and checks the resulting state.
func importInParallel(t *testing.T, gspec *types.Genesis, key *ecdsa.PrivateKey, chainPack *blockgen.ChainPack, check func(t *testing.T, st *state.IntraBlockState)) {
	t.Helper()
	prevParallel, prevWorkers := dbg.Exec3Parallel, ethconfig.Defaults.Sync.ExecWorkerCount
	t.Cleanup(func() { dbg.Exec3Parallel, ethconfig.Defaults.Sync.ExecWorkerCount = prevParallel, prevWorkers })
	for _, workers := range []int{1, 2, 4, 8} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			dbg.Exec3Parallel = true
			ethconfig.Defaults.Sync.ExecWorkerCount = workers
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))
			require.NoError(t, m.InsertChain(chainPack))
			require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
				st := state.New(m.NewStateReader(tx))
				defer st.Close()
				check(t, st)
				return nil
			}))
		})
	}
}

func revivalKeys(t *testing.T, alloc types.GenesisAlloc, n int) []*ecdsa.PrivateKey {
	t.Helper()
	keys := make([]*ecdsa.PrivateKey, n)
	for i := range keys {
		key, err := crypto.ToECDSA(common.LeftPadBytes([]byte{byte(i + 1)}, 32))
		require.NoError(t, err)
		keys[i] = key
		alloc[crypto.PubkeyToAddress(key.PublicKey)] = types.GenesisAccount{Balance: big.NewInt(1_000_000_000_000_000_000)}
	}
	return keys
}

// The coinbase is created by CREATE2 and destroyed in the same tx, a later
// tx's fee revives it with a balance only, and a third tx re-creates it by the
// same CREATE2, this time with code. Parallel import must accept the block that
// serial execution built, and the coinbase must end with the code.
func TestCoinbaseRecreatedWithCodeAfterFeeRevival(t *testing.T) {
	// The constructor self-destructs while the account has no balance and
	// deploys 32 bytes of code once a fee has revived it.
	initCode := program.New().Op(vm.SELFBALANCE).Push(8).Op(vm.JUMPI).Selfdestruct(0xdead)
	_, deploy := initCode.Jumpdest()
	require.Equal(t, uint64(8), deploy)
	runtime := make([]byte, 32)
	runtime[31] = 1
	initCode.ReturnData(runtime)

	factory := common.HexToAddress("0xfac7")
	coinbase := types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(initCode.Bytes())))
	for _, amsterdam := range []bool{false, true} {
		t.Run(fmt.Sprintf("amsterdam=%v", amsterdam), func(t *testing.T) {
			config := chain.TestChainOsakaConfig.Copy()
			if amsterdam {
				config.AmsterdamTime = common.NewUint64(0)
			}
			alloc := types.GenesisAlloc{factory: {Code: program.New().Create2(initCode.Bytes(), 0).Op(vm.STOP).Bytes()}}
			keys := revivalKeys(t, alloc, 3)
			gspec := &types.Genesis{Config: config, GasLimit: 30_000_000, Alloc: alloc}
			chainPack := buildBlockSerially(t, gspec, keys, coinbase, 10_000_000_000, []revivalCall{
				{to: factory},
				{to: common.HexToAddress("0xbeef")},
				{to: factory},
			})
			importInParallel(t, gspec, keys[0], chainPack, func(t *testing.T, st *state.IntraBlockState) {
				code, err := st.GetCode(accounts.InternAddress(coinbase))
				require.NoError(t, err)
				require.Equal(t, runtime, code)
			})
		})
	}
}

// Before Cancun, one block: a CREATE2 deploys X, the next tx makes X destroy
// itself, a 1-wei transfer revives X as a plain account, and the same CREATE2
// deploys X again. The revived account has no nonce and no code, so the second
// CREATE2 succeeds. The factory records each deployed address in the slot given
// by the calldata.
func TestCreate2AfterDestructAndTransferRevival(t *testing.T) {
	runtime := program.New().Op(vm.CALLER, vm.SELFDESTRUCT).Bytes()
	initCode := program.New().ReturnData(runtime).Bytes()
	factory := common.HexToAddress("0xfac7")
	x := types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(initCode)))

	alloc := types.GenesisAlloc{factory: {Code: program.New().Create2(initCode, 0).Push(0).Op(vm.CALLDATALOAD, vm.SSTORE, vm.STOP).Bytes()}}
	keys := revivalKeys(t, alloc, 4)
	gspec := &types.Genesis{Config: chain.TestChainBerlinConfig, GasLimit: 30_000_000, Alloc: alloc}
	chainPack := buildBlockSerially(t, gspec, keys, common.Address{1}, 1, []revivalCall{
		{to: factory, data: common.LeftPadBytes([]byte{0}, 32)},
		{to: x},
		{to: x, value: 1},
		{to: factory, data: common.LeftPadBytes([]byte{1}, 32)},
	})
	importInParallel(t, gspec, keys[0], chainPack, func(t *testing.T, st *state.IntraBlockState) {
		for slot := range byte(2) {
			deployed, err := st.GetState(accounts.InternAddress(factory), accounts.InternKey(common.BytesToHash([]byte{slot})))
			require.NoError(t, err)
			require.Equal(t, *new(uint256.Int).SetBytes(x[:]), deployed, "CREATE2 %d", slot)
		}
		code, err := st.GetCode(accounts.InternAddress(x))
		require.NoError(t, err)
		require.Equal(t, runtime, code)
	})
}

// Before Cancun, in one block: a slow tx destroys a pre-existing contract that is
// the coinbase, the next tx's fee revives it as a plain account, and a third tx
// re-creates it by CREATE2. Parallel import must accept the block that serial
// execution built.
func TestCreate2AfterCoinbaseDestructAndFeeRevival(t *testing.T) {
	initCode := []byte{byte(vm.STOP)}
	factory := common.HexToAddress("0xfac7")
	coinbase := types.CreateAddress2(factory, [32]byte{}, accounts.NewCode(initCode).Hash)
	alloc := types.GenesisAlloc{}
	keys := revivalKeys(t, alloc, 3)
	beneficiary := crypto.PubkeyToAddress(keys[2].PublicKey)
	alloc[factory] = types.GenesisAccount{Nonce: 1, Code: countdownLoop(program.New().Push(1_000)).Create2(initCode, 0).Op(vm.POP, vm.STOP).Bytes()}
	alloc[coinbase] = types.GenesisAccount{Nonce: 1, Code: countdownLoop(program.New().Push(30_000)).Selfdestruct(beneficiary).Bytes()}

	gspec := &types.Genesis{Config: shanghaiConfig(), GasLimit: 60_000_000, Alloc: alloc}
	chainPack := buildBlockSerially(t, gspec, keys, coinbase, 100_000_000_000, []revivalCall{
		{to: coinbase},
		{to: common.HexToAddress("0xbeef")},
		{to: factory},
	})
	importInParallel(t, gspec, keys[0], chainPack, func(t *testing.T, st *state.IntraBlockState) {
		nonce, err := st.GetNonce(accounts.InternAddress(coinbase))
		require.NoError(t, err)
		require.Equal(t, uint64(1), nonce, "the CREATE2 re-creates the revived coinbase")
	})
}
