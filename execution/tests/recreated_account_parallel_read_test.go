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

// Under Amsterdam the block access list lets a worker read a later tx's
// writes before that tx commits. One tx destroys a CREATE2 account, a long tx
// re-creates it with code, and a shorter tx reads its code hash and calls it
// while the re-creation is still running. Parallel import must accept the
// block the serial generator built.
func TestParallelReadOfRecreatedAccountBeforeRecreatorCommits(t *testing.T) {
	prevParallel, prevWorkers := dbg.Exec3Parallel, ethconfig.Defaults.Sync.ExecWorkerCount
	t.Cleanup(func() { dbg.Exec3Parallel, ethconfig.Defaults.Sync.ExecWorkerCount = prevParallel, prevWorkers })

	// The constructor self-destructs while the account still holds its
	// pre-block wei and deploys 32 bytes of code once that wei is gone.
	initCode := program.New().Op(vm.SELFBALANCE, vm.ISZERO).Push(9).Op(vm.JUMPI).Push(0xdead).Op(vm.SELFDESTRUCT)
	_, deploy := initCode.Jumpdest()
	require.Equal(t, uint64(9), deploy)
	runtime := make([]byte, 32)
	runtime[31] = 1
	initCode.ReturnData(runtime)

	factory := common.HexToAddress("0xaaaa")
	reader := common.HexToAddress("0xcccc")
	contract := types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(initCode.Bytes())))
	config := chain.TestChainOsakaConfig.Copy()
	config.AmsterdamTime = common.NewUint64(0)
	signer := types.LatestSignerForChainID(config.ChainID)

	alloc := types.GenesisAlloc{
		factory:  {Code: countdownLoop(program.New().Push(0).Op(vm.CALLDATALOAD)).Create2(initCode.Bytes(), 0).Op(vm.POP).Bytes()},
		reader:   {Code: countdownLoop(program.New().Push(230_000)).Push(contract).Op(vm.EXTCODEHASH).Push(contract).Op(vm.SSTORE).StaticCall(nil, contract, 0, 0, 0, 0).Op(vm.POP).Bytes()},
		contract: {Balance: big.NewInt(1)},
	}
	keys := make([]*ecdsa.PrivateKey, 3)
	for i := range keys {
		key, err := crypto.ToECDSA(common.LeftPadBytes([]byte{byte(i + 1)}, 32))
		require.NoError(t, err)
		keys[i] = key
		alloc[crypto.PubkeyToAddress(key.PublicKey)] = types.GenesisAccount{Balance: big.NewInt(1_000_000_000_000_000_000)}
	}

	dbg.Exec3Parallel = false
	ethconfig.Defaults.Sync.ExecWorkerCount = 4
	gspec := &types.Genesis{Config: config, GasLimit: 30_000_000, Alloc: alloc}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(keys[0]))
	chainPack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
		for i, call := range []struct {
			to   common.Address
			gas  uint64
			data []byte
		}{
			{factory, 300_000, iterations(1)},
			{factory, 16_777_216, iterations(550_000)},
			{reader, 8_000_000, nil},
		} {
			sender := crypto.PubkeyToAddress(keys[i].PublicKey)
			txn, err := types.SignTx(types.NewTransaction(b.TxNonce(sender), call.to, uint256.NewInt(0), call.gas, uint256.NewInt(10_000_000_000), call.data), *signer, keys[i])
			require.NoError(t, err)
			b.AddTx(txn)
		}
	})
	require.NoError(t, err)
	for _, receipt := range chainPack.Receipts[0] {
		require.Equal(t, uint64(types.ReceiptStatusSuccessful), receipt.Status)
	}

	dbg.Exec3Parallel = true
	require.NoError(t, m.InsertChain(chainPack))
	require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		defer st.Close()
		code, err := st.GetCode(accounts.InternAddress(contract))
		require.NoError(t, err)
		require.Equal(t, runtime, code)
		seen, err := st.GetState(accounts.InternAddress(reader), accounts.InternKey(common.BytesToHash(contract[:])))
		require.NoError(t, err)
		require.Equal(t, *new(uint256.Int).SetBytes(crypto.Keccak256(runtime)), seen)
		return nil
	}))
}

// countdownLoop pops an iteration count from the stack and loops that many times.
func countdownLoop(p *program.Program) *program.Program {
	_, loop := p.Jumpdest()
	return p.Push(1).Op(vm.SWAP1, vm.SUB, vm.DUP1).Push(loop).Op(vm.JUMPI, vm.POP)
}

func iterations(n uint64) []byte {
	return common.LeftPadBytes(new(uint256.Int).SetUint64(n).Bytes(), 32)
}

// A tx's account fold reads the account base from the shared state, where an earlier
// tx of the block is already folded: a field the later tx does not write must keep
// the earlier tx's value. tx0 credits A; tx1, still running when tx0 folds, only
// bumps A's nonce with a CREATE2.
func TestParallelFoldKeepsEarlierTxFieldOfSameAccount(t *testing.T) {
	prevParallel, prevWorkers := dbg.Exec3Parallel, ethconfig.Defaults.Sync.ExecWorkerCount
	t.Cleanup(func() { dbg.Exec3Parallel, ethconfig.Defaults.Sync.ExecWorkerCount = prevParallel, prevWorkers })

	a := common.HexToAddress("0xaaaa")
	config := chain.TestChainOsakaConfig.Copy()
	config.AmsterdamTime = common.NewUint64(0)
	signer := types.LatestSignerForChainID(config.ChainID)
	alloc := types.GenesisAlloc{
		// Without calldata: stop. With n: count n down, then CREATE2 with empty init code.
		a: {Code: common.FromHex("0x6000358015601d575b6001900380600857506000600060006000f550005b00"), Nonce: 1, Balance: big.NewInt(1)},
	}
	keys := make([]*ecdsa.PrivateKey, 2)
	for i := range keys {
		key, err := crypto.ToECDSA(common.LeftPadBytes([]byte{byte(i + 1)}, 32))
		require.NoError(t, err)
		keys[i] = key
		alloc[crypto.PubkeyToAddress(key.PublicKey)] = types.GenesisAccount{Balance: big.NewInt(1_000_000_000_000_000_000)}
	}

	dbg.Exec3Parallel = false
	ethconfig.Defaults.Sync.ExecWorkerCount = 4
	gspec := &types.Genesis{Config: config, GasLimit: 30_000_000, Alloc: alloc}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(keys[0]))
	chainPack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
		for i, call := range []struct {
			value uint64
			gas   uint64
			data  []byte
		}{
			{1, 100_000, nil},
			{0, 8_000_000, iterations(200_000)},
		} {
			sender := crypto.PubkeyToAddress(keys[i].PublicKey)
			txn, err := types.SignTx(types.NewTransaction(b.TxNonce(sender), a, uint256.NewInt(call.value), call.gas, uint256.NewInt(10_000_000_000), call.data), *signer, keys[i])
			require.NoError(t, err)
			b.AddTx(txn)
		}
	})
	require.NoError(t, err)
	for _, receipt := range chainPack.Receipts[0] {
		require.Equal(t, uint64(types.ReceiptStatusSuccessful), receipt.Status)
	}

	dbg.Exec3Parallel = true
	require.NoError(t, m.InsertChain(chainPack))
	require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		defer st.Close()
		balance, err := st.GetBalance(accounts.InternAddress(a))
		require.NoError(t, err)
		require.Equal(t, *uint256.NewInt(2), balance)
		nonce, err := st.GetNonce(accounts.InternAddress(a))
		require.NoError(t, err)
		require.Equal(t, uint64(2), nonce)
		return nil
	}))
}

// A BAL block dispatches a bounded window of txs past the commit frontier. A block
// of many more txs than the window, each depending on the one before it, must still
// complete and match the serial result.
func TestParallelBALBlockPastDispatchWindow(t *testing.T) {
	prevParallel, prevWorkers := dbg.Exec3Parallel, ethconfig.Defaults.Sync.ExecWorkerCount
	t.Cleanup(func() { dbg.Exec3Parallel, ethconfig.Defaults.Sync.ExecWorkerCount = prevParallel, prevWorkers })

	const txs = 80
	counter := common.HexToAddress("0xc0c0")
	config := chain.TestChainOsakaConfig.Copy()
	config.AmsterdamTime = common.NewUint64(0)
	signer := types.LatestSignerForChainID(config.ChainID)
	key, err := crypto.ToECDSA(common.LeftPadBytes([]byte{1}, 32))
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	alloc := types.GenesisAlloc{
		counter: {Code: program.New().Push(0).Op(vm.SLOAD).Push(1).Op(vm.ADD).Push(0).Op(vm.SSTORE).Bytes()},
		sender:  {Balance: big.NewInt(1_000_000_000_000_000_000)},
	}

	dbg.Exec3Parallel = false
	ethconfig.Defaults.Sync.ExecWorkerCount = 4
	gspec := &types.Genesis{Config: config, GasLimit: 30_000_000, Alloc: alloc}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))
	chainPack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
		for range txs {
			txn, err := types.SignTx(types.NewTransaction(b.TxNonce(sender), counter, uint256.NewInt(0), 300_000, uint256.NewInt(10_000_000_000), nil), *signer, key)
			require.NoError(t, err)
			b.AddTx(txn)
		}
	})
	require.NoError(t, err)
	for _, receipt := range chainPack.Receipts[0] {
		require.Equal(t, uint64(types.ReceiptStatusSuccessful), receipt.Status)
	}

	dbg.Exec3Parallel = true
	require.NoError(t, m.InsertChain(chainPack))
	require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		defer st.Close()
		v, err := st.GetState(accounts.InternAddress(counter), accounts.InternKey(common.Hash{}))
		require.NoError(t, err)
		require.Equal(t, *uint256.NewInt(txs), v)
		return nil
	}))
}
