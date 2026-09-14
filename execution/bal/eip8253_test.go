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

package bal_test

import (
	"math"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/bal"
	"github.com/erigontech/erigon/execution/chain"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

func TestEIP8253BlockImportAndBALReplay(t *testing.T) {
	t.Parallel()
	config := chain.AllProtocolChanges.Copy()
	config.AmsterdamTime = common.NewUint64(20)
	config.EIP8253Accounts = chainspec.Mainnet.Config.EIP8253Accounts
	require.Len(t, config.EIP8253Accounts, 28)
	slot, value := common.Hash{}, common.HexToHash("0x42")
	alloc := make(types.GenesisAlloc)
	for _, addr := range config.EIP8253Accounts {
		alloc[addr] = types.GenesisAccount{Balance: big.NewInt(100), Storage: map[common.Hash]common.Hash{slot: value}}
	}
	untargeted := common.HexToAddress("0x8253")
	alloc[untargeted] = types.GenesisAccount{Balance: big.NewInt(100), Storage: map[common.Hash]common.Hash{slot: value}}
	genesis := &types.Genesis{Config: config, Alloc: alloc}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis))
	blocks, err := m.GenerateChain(3, func(i int, b *blockgen.BlockGen) {
		wantNonce := uint64(0)
		if i >= 1 {
			wantNonce = 1
		}
		for _, addr := range config.EIP8253Accounts {
			require.Equal(t, wantNonce, b.TxNonce(addr))
		}
		require.Zero(t, b.TxNonce(untargeted))
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(blocks))
	require.Nil(t, blocks.Blocks[0].BlockAccessList())
	targets := make(map[accounts.Address]struct{}, len(config.EIP8253Accounts))
	for _, addr := range config.EIP8253Accounts {
		targets[accounts.InternAddress(addr)] = struct{}{}
	}
	forkBlock := blocks.Blocks[1]
	require.Equal(t, uint64(20), forkBlock.Time())
	found := 0
	for _, changes := range forkBlock.BlockAccessList() {
		if _, ok := targets[changes.Address]; !ok {
			continue
		}
		found++
		require.Equal(t, []*types.NonceChange{{Index: 0, Value: 1}}, changes.NonceChanges)
		require.Empty(t, changes.BalanceChanges)
		require.Empty(t, changes.CodeChanges)
		require.Empty(t, changes.StorageChanges)
		require.Empty(t, changes.StorageReads)
	}
	require.Equal(t, len(targets), found)
	for _, changes := range blocks.Blocks[2].BlockAccessList() {
		require.NotContains(t, targets, changes.Address)
	}
	for _, block := range blocks.Blocks {
		require.Zero(t, block.GasUsed())
		require.Empty(t, block.Transactions())
	}
	for _, receipts := range blocks.Receipts {
		require.Empty(t, receipts)
	}
	tx, err := m.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	reader := state.NewHistoryReaderV3(tx, math.MaxUint64)
	for addr := range targets {
		account, err := reader.ReadAccountData(addr)
		require.NoError(t, err)
		require.NotNil(t, account)
		require.Equal(t, uint64(1), account.Nonce)
		require.Equal(t, *uint256.NewInt(100), account.Balance)
		require.Equal(t, accounts.EmptyCodeHash, account.CodeHash)
		got, _, err := reader.ReadAccountStorage(addr, accounts.InternKey(slot))
		require.NoError(t, err)
		require.Equal(t, *uint256.NewInt(0x42), got)
	}
	regenerator := bal.NewRegenerator(m.BlockReader, m.Engine, m.Log)
	for _, block := range blocks.Blocks[1:] {
		expected, err := types.EncodeBlockAccessListBytes(block.BlockAccessList())
		require.NoError(t, err)
		actual, err := regenerator.GetBlockAccessListBytes(t.Context(), config, tx, block.Hash(), block.NumberU64())
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	}
}

type eip8253HeaderReader struct {
	rules.ChainHeaderReader
	parent *types.Header
}

func (r eip8253HeaderReader) GetHeader(hash common.Hash, number uint64) *types.Header {
	if r.parent.Hash() == hash && r.parent.Number.Uint64() == number {
		return r.parent
	}
	return nil
}

func TestEIP8253BALReproducesStateRoot(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x8253"))
	untargeted := accounts.InternAddress(common.HexToAddress("0x8254"))
	nonceChanged := accounts.InternAddress(common.HexToAddress("0x8255"))
	codeChanged := accounts.InternAddress(common.HexToAddress("0x8256"))
	slot := accounts.InternKey(common.Hash{})
	config := &chain.Config{
		AmsterdamTime:   common.NewUint64(100),
		EIP8253Accounts: []common.Address{addr.Value(), nonceChanged.Value(), codeChanged.Value()},
	}
	parent := &types.Header{Time: 90}
	header := &types.Header{Number: *uint256.NewInt(1), ParentHash: parent.Hash(), Time: 100}
	blockRules := (&evmtypes.BlockContext{BlockNumber: 1, Time: 100}).Rules(config)
	expectedBAL := types.BlockAccessList{
		{Address: addr, NonceChanges: []*types.NonceChange{{Index: 0, Value: 1}}},
		{Address: nonceChanged},
		{Address: codeChanged},
	}
	var expectedRoot common.Hash
	for _, mode := range []string{"serial", "parallel", "BAL"} {
		t.Run(mode, func(t *testing.T) {
			db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
			tx, domains := temporaltest.NewTestTxSD(t, db)
			domains.EnableParaTrieDB(db)
			reader := state.NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{}))
			writer := state.NewWriter(domains.AsPutDel(tx), nil, 0)
			pre := state.New(reader)
			defer pre.Close()
			for _, account := range []accounts.Address{addr, untargeted, nonceChanged, codeChanged} {
				require.NoError(t, pre.SetBalance(account, *uint256.NewInt(100), tracing.BalanceChangeUnspecified))
				require.NoError(t, pre.SetState(account, slot, *uint256.NewInt(42)))
			}
			require.NoError(t, pre.SetCode(untargeted, []byte{0x00}, tracing.CodeChangeUnspecified))
			require.NoError(t, pre.SetNonce(untargeted, 9, tracing.NonceChangeUnspecified))
			require.NoError(t, pre.SetCode(codeChanged, []byte{0x00}, tracing.CodeChangeUnspecified))
			require.NoError(t, pre.SetNonce(nonceChanged, 9, tracing.NonceChangeUnspecified))
			require.NoError(t, pre.CommitBlock(blockRules, writer))
			before, err := domains.ComputeCommitment(t.Context(), tx, true, 0, 0, "", nil)
			require.NoError(t, err)
			beforeRoot := common.BytesToHash(before)
			if mode == "BAL" {
				writes := bal.ToWriteSet(expectedBAL, math.MaxUint32)
				require.NoError(t, writes.Apply(domains, tx, 1, 1, nil, blockRules, nil, false))
			} else {
				ibs := state.New(reader)
				defer ibs.Close()
				ibs.SetTxContext(1, -1)
				if mode == "parallel" {
					ibs.SetVersionMap(state.NewVersionMap(nil))
				}
				engine := merge.New(ethash.NewFaker())
				defer engine.Close()
				require.NoError(t, protocol.InitializeBlockExecution(engine, eip8253HeaderReader{parent: parent}, header, config, ibs, nil, log.New(), nil))
				if mode == "parallel" {
					writes := ibs.FinalizedWrites(blockRules)
					io := &state.VersionedIO{}
					ibs.MergeTxIOInto(io, writes)
					actualBAL := io.AsBlockAccessList()
					require.Equal(t, expectedBAL, actualBAL)
					require.NoError(t, writes.Apply(domains, tx, 1, 1, nil, blockRules, nil, false))
				} else {
					writer.SetTxNum(1)
					require.NoError(t, ibs.CommitBlock(blockRules, writer))
				}
			}
			after, err := domains.ComputeCommitment(t.Context(), tx, true, 1, 1, "", nil)
			require.NoError(t, err)
			afterRoot := common.BytesToHash(after)
			require.NotEqual(t, beforeRoot, afterRoot)
			if mode == "serial" {
				expectedRoot = afterRoot
			} else {
				require.Equal(t, expectedRoot, afterRoot)
			}
			for _, account := range []accounts.Address{addr, untargeted, nonceChanged, codeChanged} {
				data, err := reader.ReadAccountData(account)
				require.NoError(t, err)
				require.NotNil(t, data)
				require.Equal(t, *uint256.NewInt(100), data.Balance)
				value, _, err := reader.ReadAccountStorage(account, slot)
				require.NoError(t, err)
				require.Equal(t, *uint256.NewInt(42), value)
				wantNonce := uint64(9)
				if account == addr {
					wantNonce = 1
				} else if account == codeChanged {
					wantNonce = 0
				}
				require.Equal(t, wantNonce, data.Nonce)
				if account == untargeted || account == codeChanged {
					code, err := reader.ReadAccountCode(account)
					require.NoError(t, err)
					require.Equal(t, []byte{0x00}, code)
				} else {
					require.Equal(t, accounts.EmptyCodeHash, data.CodeHash)
				}
			}
		})
	}
}

func TestEIP8253CreationCollision(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		salt *uint256.Int
	}{
		{"CREATE", nil},
		{"CREATE2", uint256.NewInt(1)},
	} {
		for _, active := range []bool{false, true} {
			name := tc.name + "/without transition"
			if active {
				name = tc.name + "/fork block"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				sender := accounts.InternAddress(common.HexToAddress("0x1234"))
				initcode := []byte{byte(vm.STOP)}
				target := types.CreateAddress(sender.Value(), 0)
				if tc.salt != nil {
					target = types.CreateAddress2(sender.Value(), tc.salt.Bytes32(), accounts.InternCodeHash(crypto.Keccak256Hash(initcode)))
				}
				addr := accounts.InternAddress(target)
				config := chain.AllProtocolChanges.Copy()
				config.AmsterdamTime = common.NewUint64(100)
				if active {
					config.EIP8253Accounts = []common.Address{target}
				}
				parent := &types.Header{Time: 90}
				header := &types.Header{Number: *uint256.NewInt(1), ParentHash: parent.Hash(), Time: 100, GasLimit: 30_000_000}
				engine := merge.New(ethash.NewFaker())
				defer engine.Close()
				ibs := state.New(state.NewNoopReader())
				defer ibs.Close()
				require.NoError(t, ibs.SetBalance(sender, *uint256.NewInt(1_000_000), tracing.BalanceChangeUnspecified))
				require.NoError(t, ibs.SetBalance(addr, *uint256.NewInt(100), tracing.BalanceChangeUnspecified))
				slot := accounts.InternKey(common.Hash{})
				require.NoError(t, ibs.SetState(addr, slot, *uint256.NewInt(42)))
				ibs.SetTxContext(1, -1)
				require.NoError(t, protocol.InitializeBlockExecution(engine, eip8253HeaderReader{parent: parent}, header, config, ibs, nil, log.New(), nil))
				ibs.SetTxContext(1, 0)
				blockContext := protocol.NewEVMBlockContext(header, protocol.GetHashFn(header, nil), engine, accounts.NilAddress, config)
				evm := vm.NewEVM(blockContext, evmtypes.TxContext{}, ibs, config, vm.Config{})
				gas := mdgas.MdGas{Execution: 1_000_000, State: 1_000_000}
				_, _, _, _, err := evm.Create(sender, initcode, gas, uint256.Int{}, tc.salt, false)
				if active {
					require.ErrorIs(t, err, vm.ErrContractAddressCollision)
					value, err := ibs.GetState(addr, slot)
					require.NoError(t, err)
					require.Equal(t, *uint256.NewInt(42), value)
				} else {
					require.NoError(t, err)
				}
			})
		}
	}
}
