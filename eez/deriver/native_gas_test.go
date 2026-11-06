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

package deriver_test

import (
	"fmt"
	"maps"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/eez"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/internal/stateless"
	"github.com/erigontech/erigon/eez/networks"
	"github.com/erigontech/erigon/eez/protocol"
	eezrules "github.com/erigontech/erigon/eez/rules"
	"github.com/erigontech/erigon/eez/systemtx"
	"github.com/erigontech/erigon/execution/blockreplay"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	execprotocol "github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

// TestExtendNativeGasLeft: a native transaction after more gas than its
// block can spare for the cap is built with the gas the block has left.
func TestExtendNativeGasLeft(t *testing.T) {
	spec := networks.Chiado699169
	genesis := *spec.Genesis
	genesis.Alloc = maps.Clone(genesis.Alloc)
	genesis.Alloc[derivertest.UserAddress()] = types.GenesisAccount{Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(21), nil)}
	spec.Genesis = &genesis
	_, chain := derivertest.NewL2Of(t, spec)
	l2 := genesis.Config.L2.(*config.L2Config)
	signer := types.LatestSignerForChainID(genesis.Config.ChainID)
	var nonce uint64
	userTx := func(to *common.Address, gas uint64, data []byte) types.Transaction {
		tx, err := types.SignTx(&types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{Nonce: nonce, GasLimit: gas, To: to, Data: data},
			ChainID:  *genesis.Config.ChainID,
			FeeCap:   *uint256.NewInt(100 * common.GWei),
		}, *signer, derivertest.UserKey)
		require.NoError(t, err)
		nonce++
		return tx
	}
	extend := func(parent *types.Header, txs ...types.Transaction) *types.BlockWithReceipts {
		in := deriver.HeaderInputs{
			Number:     parent.Number.Uint64() + 1,
			ParentHash: parent.Hash(),
			Time:       parent.Time + l2.BlockInterval,
			Extra:      []byte{},
			GasLimit:   l2.BlockGasLimit,
		}
		built, err := chain.Extend(t.Context(), parent, in, txs, common.Hash{}, common.Hash{})
		require.NoError(t, err)
		return built
	}

	parent, err := chain.HeaderByNumber(t.Context(), 0)
	require.NoError(t, err)
	// Its runtime code, JUMPDEST PUSH1 0 JUMP, burns all the gas it gets.
	burner := types.CreateAddress(crypto.PubkeyToAddress(derivertest.UserKey.PublicKey), 0)
	deployed := extend(parent, userTx(nil, 100_000, common.FromHex("0x635b6000566000526004601cf3")))
	require.Equal(t, types.ReceiptStatusSuccessful, deployed.Receipts[0].Status)

	const burnt = 14_000_000
	delivery, err := systemtx.NewNative(protocol.NativeTx{
		ChainID:  genesis.Config.ChainID.Uint64(),
		GasLimit: protocol.NativeTxGasCap,
		To:       protocol.RollupXEEZL2Address,
		Input:    protocol.ExecuteIncomingCrossChainCallSelector[:],
	})
	require.NoError(t, err)
	built := extend(deployed.Block.Header(), userTx(&burner, burnt, nil), delivery)
	require.Equal(t, uint64(burnt), built.Receipts[0].GasUsed)
	require.Less(t, l2.BlockGasLimit-burnt, protocol.NativeTxGasCap)
	require.Equal(t, l2.BlockGasLimit-burnt, built.Block.Transactions()[1].GetGasLimit())
}

// headerless is the EEZ engine without its header checks, which hold EEZ
// blocks to Prague through Osaka.
type headerless struct{ *eezrules.Engine }

func (headerless) VerifyHeader(rules.ChainHeaderReader, *types.Header, bool) error { return nil }

// pinnedNativeGas binds every native transaction to gas, whatever its position.
type pinnedNativeGas struct {
	headerless
	gas uint64
}

func (e pinnedNativeGas) BindTxGas(txn types.Transaction, _ uint64) (types.Transaction, error) {
	native, ok := txn.(*systemtx.NativeTransaction)
	if !ok {
		return txn, nil
	}
	body := native.Body()
	body.GasLimit = e.gas
	return systemtx.NewNative(body)
}

// stateGasBlock builds block 1 of a v0.1 chain: a transaction writing the
// given number of new storage slots, then a native transaction encoding the cap.
type stateGasBlock struct {
	genesis types.Genesis
	osaka   *chain.Config
	txs     []types.Transaction
}

func newStateGasBlock(t *testing.T, slots byte, amsterdam bool) *stateGasBlock {
	t.Helper()
	prevParallel, prevWorkers := dbg.Exec3Parallel, ethconfig.Defaults.Sync.ExecWorkerCount
	t.Cleanup(func() { dbg.Exec3Parallel, ethconfig.Defaults.Sync.ExecWorkerCount = prevParallel, prevWorkers })
	eez.Register()
	eezl2 := protocol.RollupXEEZL2Address
	filler := common.HexToAddress("0x00000000000000000000000000000000000f111e")
	genesis := *networks.Chiado699169.Genesis
	genesis.Alloc = maps.Clone(genesis.Alloc)
	genesis.Alloc[derivertest.UserAddress()] = types.GenesisAccount{Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(21), nil)}
	// JUMPDEST PUSH1 0 JUMP: EEZL2 burns all the gas it gets.
	genesis.Alloc[eezl2] = types.GenesisAccount{Code: []byte{byte(vm.JUMPDEST), byte(vm.PUSH1), 0, byte(vm.JUMP)}}
	// Stores i in slot i, for i from slots down to 1.
	genesis.Alloc[filler] = types.GenesisAccount{Code: []byte{
		byte(vm.PUSH2), 0, slots,
		byte(vm.JUMPDEST), byte(vm.DUP1), byte(vm.DUP1), byte(vm.SSTORE),
		byte(vm.PUSH1), 1, byte(vm.SWAP1), byte(vm.SUB),
		byte(vm.DUP1), byte(vm.PUSH1), 3, byte(vm.JUMPI),
		byte(vm.STOP),
	}}
	b := &stateGasBlock{genesis: genesis, osaka: genesis.Config}
	if amsterdam {
		cc := *genesis.Config
		cc.AmsterdamTime = common.NewUint64(0)
		b.genesis.Config = &cc
	}
	user, err := types.SignTx(&types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{GasLimit: protocol.NativeTxGasCap, To: &filler},
		ChainID:  *genesis.Config.ChainID,
		FeeCap:   *uint256.NewInt(100 * common.GWei),
	}, *types.LatestSignerForChainID(genesis.Config.ChainID), derivertest.UserKey)
	require.NoError(t, err)
	native, err := systemtx.NewNative(protocol.NativeTx{
		ChainID:  genesis.Config.ChainID.Uint64(),
		GasLimit: protocol.NativeTxGasCap,
		To:       eezl2,
		Input:    protocol.ExecuteIncomingCrossChainCallSelector[:],
	})
	require.NoError(t, err)
	b.txs = []types.Transaction{user, native}
	return b
}

// chain starts a chain on the genesis with the engine wrap makes of the EEZ
// engine, executing serially or with workers parallel workers.
func (b *stateGasBlock) chain(t *testing.T, workers int, wrap func(*eezrules.Engine) rules.Engine) (*execmoduletester.ExecModuleTester, *deriver.ExecChain) {
	t.Helper()
	dbg.Exec3Parallel = workers > 0
	ethconfig.Defaults.Sync.ExecWorkerCount = max(workers, 1)
	engine, err := eezrules.New(b.osaka)
	require.NoError(t, err)
	genesis := b.genesis
	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(&genesis),
		execmoduletester.WithChainConfig(genesis.Config),
		execmoduletester.WithEngine(wrap(engine)),
		execmoduletester.WithKey(derivertest.UserKey),
		execmoduletester.WithoutSystemContracts(),
	)
	return m, deriver.NewExecChain(m.ExecModule, m.DB, m.BlockReader, m.ReceiptsReader, m.ChainConfig)
}

// build derives block 1 serially with the engine wrap makes.
func (b *stateGasBlock) build(t *testing.T, wrap func(*eezrules.Engine) rules.Engine) (*execmoduletester.ExecModuleTester, *types.BlockWithReceipts) {
	t.Helper()
	m, chain := b.chain(t, 0, wrap)
	parent, err := chain.HeaderByNumber(t.Context(), 0)
	require.NoError(t, err)
	l2 := b.genesis.Config.L2.(*config.L2Config)
	built, err := chain.Extend(t.Context(), parent, deriver.HeaderInputs{
		Number:     1,
		ParentHash: parent.Hash(),
		Time:       parent.Time + l2.BlockInterval,
		Extra:      []byte{},
		GasLimit:   l2.BlockGasLimit,
	}, b.txs, common.Hash{}, common.Hash{})
	require.NoError(t, err)
	require.Equal(t, types.ReceiptStatusSuccessful, built.Receipts[0].Status)
	return m, built
}

// insert imports block into a fresh chain of the EEZ engine.
func (b *stateGasBlock) insert(t *testing.T, workers int, block *types.Block) error {
	t.Helper()
	m, _ := b.chain(t, workers, func(e *eezrules.Engine) rules.Engine { return headerless{e} })
	return m.InsertChain(&blockgen.ChainPack{Blocks: []*types.Block{block}, Headers: []*types.Header{block.Header()}, TopBlock: block})
}

// TestExtendNativeGasAmsterdam checks that from Amsterdam derivation gives a
// native transaction the gas its block admits, the limit less the larger of
// its regular and state gas, not what its receipts leave.
func TestExtendNativeGasAmsterdam(t *testing.T) {
	const slots = 150
	b := newStateGasBlock(t, slots, true)
	_, built := b.build(t, func(e *eezrules.Engine) rules.Engine { return headerless{e} })
	left := built.Block.GasLimit() - slots*params.StateGasPerStorageSet
	require.Less(t, built.Block.GasLimit()-built.Receipts[0].CumulativeGasUsed, left)
	require.Equal(t, left, built.Block.Transactions()[1].GetGasLimit())
}

// TestInsertNativeGas checks that serial and parallel execution accept a
// native transaction with the gas its block admits and reject one with other
// gas: from Amsterdam, what its receipts leave.
func TestInsertNativeGas(t *testing.T) {
	for _, amsterdam := range []bool{false, true} {
		t.Run(fmt.Sprintf("amsterdam=%v", amsterdam), func(t *testing.T) {
			b := newStateGasBlock(t, 150, amsterdam)
			_, valid := b.build(t, func(e *eezrules.Engine) rules.Engine { return headerless{e} })
			gas := valid.Block.Transactions()[1].GetGasLimit()
			wrong := gas - 1
			if amsterdam {
				wrong = valid.Block.GasLimit() - valid.Receipts[0].CumulativeGasUsed
			}
			require.NotEqual(t, gas, wrong)
			_, invalid := b.build(t, func(e *eezrules.Engine) rules.Engine { return pinnedNativeGas{headerless{e}, wrong} })
			require.Equal(t, wrong, invalid.Block.Transactions()[1].GetGasLimit())
			for _, workers := range []int{0, 1, 2, 4} {
				t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
					require.NoError(t, b.insert(t, workers, valid.Block))
					require.ErrorContains(t, b.insert(t, workers, invalid.Block), fmt.Sprintf("%s: %d, want %d", execprotocol.ErrGasLimitPosition, wrong, gas))
				})
			}
		})
	}
}

// TestReplayNativeGas checks that a replay on recorded pre-state and the
// stateless executor reject a native transaction with other gas than its
// block admits.
func TestReplayNativeGas(t *testing.T) {
	// The witness reads the parent's commitment from history.
	schema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() { statecfg.Schema = schema })
	for _, amsterdam := range []bool{false, true} {
		t.Run(fmt.Sprintf("amsterdam=%v", amsterdam), func(t *testing.T) {
			b := newStateGasBlock(t, 150, amsterdam)
			m, valid := b.build(t, func(e *eezrules.Engine) rules.Engine { return headerless{e} })
			wrong := valid.Block.Transactions()[1].GetGasLimit() - 1
			_, invalid := b.build(t, func(e *eezrules.Engine) rules.Engine { return pinnedNativeGas{headerless{e}, wrong} })
			require.Equal(t, wrong, invalid.Block.Transactions()[1].GetGasLimit())

			t.Run("recorded pre-state", func(t *testing.T) {
				var fx *blockreplay.Fixture
				require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) (err error) {
					fx, err = blockreplay.Capture(t.Context(), tx, m.BlockReader, m.ChainConfig, m.Engine, 1, log.New())
					return err
				}))
				_, err := blockreplay.Replay(fx, m.ChainConfig, m.Engine, 0, log.New())
				require.NoError(t, err)
				fx.BlockRLP, err = rlp.EncodeToBytes(invalid.Block)
				require.NoError(t, err)
				_, err = blockreplay.Replay(fx, m.ChainConfig, m.Engine, 0, log.New())
				require.ErrorIs(t, err, execprotocol.ErrGasLimitPosition)
			})
			t.Run("stateless", func(t *testing.T) {
				require.NoError(t, m.DB.Update(t.Context(), func(tx kv.RwTx) error {
					return rawdb.WriteDBCommitmentHistoryEnabled(tx, true)
				}))
				api := jsonrpc.NewPrivateDebugAPI(
					jsonrpc.NewBaseApi(nil, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs}),
					m.DB, nil, &rpccfg.DebugApiConfig{})
				number := rpc.BlockNumber(1)
				res, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHash{BlockNumber: &number}, nil)
				require.NoError(t, err)
				raw := func(bs []hexutil.Bytes) [][]byte {
					out := make([][]byte, len(bs))
					for i, b := range bs {
						out[i] = b
					}
					return out
				}
				w := &stateless.Witness{State: raw(res.State), Codes: raw(res.Codes), Headers: raw(res.Headers)}
				_, err = stateless.Execute(m.ChainConfig, m.Engine, valid.Block, w)
				require.NoError(t, err)
				_, err = stateless.Execute(m.ChainConfig, m.Engine, invalid.Block, w)
				require.ErrorIs(t, err, execprotocol.ErrGasLimitPosition)
			})
		})
	}
}
