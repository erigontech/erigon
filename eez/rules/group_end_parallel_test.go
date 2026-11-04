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

package rules_test

import (
	"crypto/ecdsa"
	"fmt"
	"maps"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/empty"
	eezconfig "github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/networks"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/systemtx"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/ethconfig"
)

const groupEndTipCap, groupEndFeeCap uint64 = common.GWei, 100 * common.GWei

// groupEndChain builds v0.1 blocks on a 699169 genesis with alloc added,
// serially and in parallel.
type groupEndChain struct {
	t    *testing.T
	spec chainspec.Spec
}

func newGroupEndChain(t *testing.T, alloc types.GenesisAlloc) *groupEndChain {
	prevParallel, prevWorkers := dbg.Exec3Parallel, ethconfig.Defaults.Sync.ExecWorkerCount
	t.Cleanup(func() { dbg.Exec3Parallel, ethconfig.Defaults.Sync.ExecWorkerCount = prevParallel, prevWorkers })
	spec := networks.Chiado699169
	genesis := *spec.Genesis
	genesis.Alloc = maps.Clone(genesis.Alloc)
	maps.Copy(genesis.Alloc, alloc)
	spec.Genesis = &genesis
	return &groupEndChain{t: t, spec: spec}
}

func (c *groupEndChain) l2() *eezconfig.L2Config { return c.spec.Config.L2.(*eezconfig.L2Config) }

func (c *groupEndChain) userTx(key *ecdsa.PrivateKey, nonce uint64, to common.Address, value, gas uint64) types.Transaction {
	tx, err := types.SignTx(&types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{Nonce: nonce, GasLimit: gas, To: &to, Value: *uint256.NewInt(value)},
		ChainID:  *c.spec.Config.ChainID,
		TipCap:   *uint256.NewInt(groupEndTipCap),
		FeeCap:   *uint256.NewInt(groupEndFeeCap),
	}, *types.LatestSignerForChainID(c.spec.Config.ChainID), key)
	require.NoError(c.t, err)
	return tx
}

// nativeTx calls EEZL2 with value and the bare selector: the call reverts
// unless EEZL2's code is replaced, and the transaction opens or ends a group
// by its selector either way.
func (c *groupEndChain) nativeTx(selector [4]byte, value uint64) types.Transaction {
	tx, err := systemtx.NewNative(protocol.NativeTx{
		ChainID:  c.spec.Config.ChainID.Uint64(),
		GasLimit: protocol.NativeTxGasCap,
		To:       protocol.RollupXEEZL2Address,
		Value:    *uint256.NewInt(value),
		Input:    selector[:],
	})
	require.NoError(c.t, err)
	return tx
}

// serial builds block 1 with txs and coinbase without the parallel executor.
func (c *groupEndChain) serial(coinbase common.Address, txs []types.Transaction) (*deriver.ExecChain, *types.BlockWithReceipts) {
	dbg.Exec3Parallel = false
	_, chain := derivertest.NewL2Of(c.t, c.spec)
	parent, err := chain.HeaderByNumber(c.t.Context(), 0)
	require.NoError(c.t, err)
	built, err := chain.Extend(c.t.Context(), parent, deriver.HeaderInputs{
		Number:     1,
		ParentHash: parent.Hash(),
		Time:       parent.Time + c.l2().BlockInterval,
		Coinbase:   coinbase,
		Extra:      []byte{},
		GasLimit:   c.l2().BlockGasLimit,
	}, txs, common.Hash{}, common.Hash{})
	require.NoError(c.t, err)
	require.LessOrEqual(c.t, built.Block.BaseFee().Uint64()+groupEndTipCap, groupEndFeeCap)
	return chain, built
}

// parallel imports block into a fresh chain with the parallel executor.
func (c *groupEndChain) parallel(t *testing.T, workers int, block *types.Block) *deriver.ExecChain {
	dbg.Exec3Parallel = true
	ethconfig.Defaults.Sync.ExecWorkerCount = workers
	m, chain := derivertest.NewL2Of(t, c.spec)
	require.NoError(t, m.InsertChain(&blockgen.ChainPack{Blocks: []*types.Block{block}, Headers: []*types.Header{block.Header()}, TopBlock: block}))
	return chain
}

func storageAt(t *testing.T, chain *deriver.ExecChain, addr common.Address, slot uint64) common.Hash {
	v, err := chain.Storage(t.Context(), 1, addr, common.BigToHash(new(big.Int).SetUint64(slot)))
	require.NoError(t, err)
	return v
}

// storeOf appends to code the store of op applied to target in slot.
func storeOf(code []byte, target common.Address, op vm.OpCode, slot byte) []byte {
	code = append(append(code, byte(vm.PUSH20)), target[:]...)
	return append(code, byte(op), byte(vm.PUSH1), slot, byte(vm.SSTORE))
}

// TestGroupEndBurnAfterFees checks that under rollupX v0.1 the group-end burn
// follows the fees of the transaction ending the group. With EEZL2 as
// coinbase, the tip of the user transaction after a load is burnt before the
// next transaction reads EEZL2's balance, and the native system address is
// emptied and removed, also when that transaction created it; without a group
// end both stay. Parallel import must accept the block serial assembly built
// and read the same state.
func TestGroupEndBurnAfterFees(t *testing.T) {
	eezl2, native := protocol.RollupXEEZL2Address, protocol.NativeSystemAddress
	reader := common.HexToAddress("0x8ead")
	readerCode := append(storeOf(storeOf(nil, eezl2, vm.BALANCE, 0), native, vm.EXTCODEHASH, 1), byte(vm.STOP))
	recipient := common.HexToAddress("0x7ec1")
	rich := types.GenesisAccount{Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(21), nil)}
	user := derivertest.UserKey

	for _, tc := range []struct {
		name       string
		fundNative bool
		txs        func(c *groupEndChain) []types.Transaction
		// tipped is the user transaction whose tip reaches EEZL2 before
		// the reader, -1 for none.
		tipped int
		burnt  bool
	}{
		{"user transaction after a load", true, func(c *groupEndChain) []types.Transaction {
			return []types.Transaction{c.nativeTx(protocol.LoadExecutionTableSelector, 0), c.userTx(user, 0, recipient, 0, 21_000), c.userTx(user, 1, reader, 0, 100_000)}
		}, 1, true},
		{"user transaction after a load pays the native system address", true, func(c *groupEndChain) []types.Transaction {
			return []types.Transaction{c.nativeTx(protocol.LoadExecutionTableSelector, 0), c.userTx(user, 0, native, 1, 21_000), c.userTx(user, 1, reader, 0, 100_000)}
		}, 1, true},
		{"user transaction after a load creates the native system address", false, func(c *groupEndChain) []types.Transaction {
			return []types.Transaction{c.nativeTx(protocol.LoadExecutionTableSelector, 0), c.userTx(user, 0, native, 1, 21_000), c.userTx(user, 1, reader, 0, 100_000)}
		}, 1, true},
		{"delivery", true, func(c *groupEndChain) []types.Transaction {
			return []types.Transaction{c.nativeTx(protocol.ExecuteIncomingCrossChainCallSelector, 0), c.userTx(user, 0, reader, 0, 100_000)}
		}, -1, true},
		{"no group end", true, func(c *groupEndChain) []types.Transaction {
			return []types.Transaction{c.userTx(user, 0, recipient, 0, 21_000), c.userTx(user, 1, reader, 0, 100_000)}
		}, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			alloc := types.GenesisAlloc{derivertest.UserAddress(): rich, reader: {Code: readerCode}}
			if tc.fundNative {
				alloc[native] = types.GenesisAccount{Balance: big.NewInt(common.GWei)}
			}
			c := newGroupEndChain(t, alloc)
			serial, built := c.serial(eezl2, tc.txs(c))
			wantBalance, wantCodeHash := storageAt(t, serial, reader, 0), storageAt(t, serial, reader, 1)
			if tc.tipped >= 0 {
				require.Equal(t, types.ReceiptStatusSuccessful, built.Receipts[tc.tipped].Status)
			}
			if tc.burnt {
				require.Equal(t, common.Hash{}, wantBalance, "EEZL2 burnt at the group end")
				require.Equal(t, common.Hash{}, wantCodeHash, "native system address removed at the group end")
			} else {
				tip := new(uint256.Int).Mul(uint256.NewInt(groupEndTipCap), uint256.NewInt(built.Receipts[tc.tipped].GasUsed))
				require.Equal(t, common.Hash(tip.Bytes32()), wantBalance, "tip kept without a group end")
				require.Equal(t, empty.CodeHash, wantCodeHash, "native system address kept without a group end")
			}

			for _, workers := range []int{1, 2, 4, 8} {
				t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
					parallel := c.parallel(t, workers, built.Block)
					require.Equal(t, wantBalance, storageAt(t, parallel, reader, 0))
					require.Equal(t, wantCodeHash, storageAt(t, parallel, reader, 1))
				})
			}
		})
	}
}

// TestGroupEndBurnRevalidatesReaders checks that a transaction that read what
// the group end wrote before its burn re-executes. A slow first transaction
// lets the group end validate, and the readers after it run, before the
// in-order finalize burns; the readers send from their own accounts and leave
// the coinbase alone, so nothing else orders them after the burn.
func TestGroupEndBurnRevalidatesReaders(t *testing.T) {
	eezl2, native := protocol.RollupXEEZL2Address, protocol.NativeSystemAddress
	burner := common.HexToAddress("0xb0b0")
	nativeBalance, nativeCodeHash, eezl2Balance := common.HexToAddress("0x8ea1"), common.HexToAddress("0x8ea2"), common.HexToAddress("0x8ea3")
	keys := make([]*ecdsa.PrivateKey, 3)
	for i := range keys {
		key, err := crypto.ToECDSA(common.LeftPadBytes([]byte{byte(i + 1)}, 32))
		require.NoError(t, err)
		keys[i] = key
	}
	rich := types.GenesisAccount{Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(21), nil)}

	// payAfterLoad ends a group with a user transaction paying to.
	payAfterLoad := func(to common.Address, value uint64) func(c *groupEndChain) []types.Transaction {
		return func(c *groupEndChain) []types.Transaction {
			return []types.Transaction{c.nativeTx(protocol.LoadExecutionTableSelector, 0), c.userTx(derivertest.UserKey, 0, to, value, 100_000)}
		}
	}
	other := common.HexToAddress("0xc0ffee")

	for _, tc := range []struct {
		name       string
		fundNative bool
		// stopEEZL2 replaces EEZL2's code so a payment to it succeeds.
		stopEEZL2 bool
		coinbase  common.Address
		groupEnd  func(c *groupEndChain) []types.Transaction
		reader    common.Address
	}{
		{"native system address balance", true, false, eezl2, payAfterLoad(native, 1), nativeBalance},
		{"native system address balance, created", false, false, eezl2, payAfterLoad(native, 1), nativeBalance},
		{"native system address code hash, created", false, false, eezl2, payAfterLoad(native, 1), nativeCodeHash},
		{"EEZL2 balance, other coinbase", false, true, other, payAfterLoad(eezl2, common.GWei), eezl2Balance},
		{"EEZL2 balance, delivery", false, true, other, func(c *groupEndChain) []types.Transaction {
			return []types.Transaction{c.nativeTx(protocol.ExecuteIncomingCrossChainCallSelector, common.GWei)}
		}, eezl2Balance},
	} {
		t.Run(tc.name, func(t *testing.T) {
			alloc := types.GenesisAlloc{
				derivertest.UserAddress(): rich,
				burner:                    {Code: []byte{byte(vm.JUMPDEST), byte(vm.PUSH1), 0, byte(vm.JUMP)}},
				nativeBalance:             {Code: append(storeOf(nil, native, vm.BALANCE, 0), byte(vm.STOP))},
				nativeCodeHash:            {Code: append(storeOf(nil, native, vm.EXTCODEHASH, 0), byte(vm.STOP))},
				eezl2Balance:              {Code: append(storeOf(nil, eezl2, vm.BALANCE, 0), byte(vm.STOP))},
			}
			for _, key := range keys {
				alloc[crypto.PubkeyToAddress(key.PublicKey)] = rich
			}
			if tc.fundNative {
				alloc[native] = types.GenesisAccount{Balance: big.NewInt(common.GWei)}
			}
			if tc.stopEEZL2 {
				alloc[eezl2] = types.GenesisAccount{Code: []byte{byte(vm.STOP)}}
			}
			c := newGroupEndChain(t, alloc)
			groupEnd := tc.groupEnd(c)
			txs := append(append([]types.Transaction{c.userTx(keys[0], 0, burner, 0, 10_000_000)}, groupEnd...),
				c.userTx(keys[1], 0, tc.reader, 0, 100_000), c.userTx(keys[2], 0, tc.reader, 0, 100_000))
			serial, built := c.serial(tc.coinbase, txs)
			require.Equal(t, types.ReceiptStatusSuccessful, built.Receipts[len(groupEnd)].Status, "group end")
			require.Equal(t, common.Hash{}, storageAt(t, serial, tc.reader, 0), "burnt or removed at the group end")

			// Whether the group end validates ahead of finalize depends on
			// scheduling.
			for _, workers := range []int{2, 4} {
				for run := range 3 {
					t.Run(fmt.Sprintf("workers=%d/%d", workers, run), func(t *testing.T) {
						require.Equal(t, common.Hash{}, storageAt(t, c.parallel(t, workers, built.Block), tc.reader, 0))
					})
				}
			}
		})
	}
}
