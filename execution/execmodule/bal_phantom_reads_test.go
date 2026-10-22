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
	"context"
	"encoding/binary"
	"math/big"
	"slices"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
)

type auraReadEngine struct {
	rules.Engine
	reader accounts.Address
}

func (e *auraReadEngine) read(syscall rules.SystemCall, firstSlot uint64) error {
	var data [32]byte
	binary.BigEndian.PutUint64(data[24:], firstSlot)
	_, err := syscall(e.reader, data[:])
	return err
}

func (e *auraReadEngine) IsServiceTransaction(_ accounts.Address, syscall rules.SystemCall) bool {
	return e.read(syscall, 0) == nil && e.read(syscall, 14_000) == nil
}

func (e *auraReadEngine) Finalize(config *chain.Config, header *types.Header, ibs *state.IntraBlockState,
	uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal,
	reader rules.ChainReader, syscall rules.SystemCall, skipReceipts bool, logger log.Logger,
) (types.FlatRequests, error) {
	requests, err := e.Engine.Finalize(config, header, ibs, uncles, receipts, withdrawals, reader, syscall, skipReceipts, logger)
	if err != nil {
		return nil, err
	}
	for _, slot := range []uint64{28_000, 42_000} {
		if err := e.read(syscall, slot); err != nil {
			return nil, err
		}
	}
	return requests, nil
}

func (e *auraReadEngine) FinalizeAndAssemble(config *chain.Config, header *types.Header, ibs *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal,
	reader rules.ChainReader, syscall rules.SystemCall, _ rules.Call, logger log.Logger,
) (*types.Block, types.FlatRequests, error) {
	header.RequestsHash = nil
	requests, err := e.Finalize(config, header, ibs, uncles, receipts, withdrawals, reader, syscall, false, logger)
	if err != nil {
		return nil, nil, err
	}
	header.RequestsHash = requests.Hash()
	return types.NewBlockForAsembling(header, txs, uncles, receipts, withdrawals, nil), requests, nil
}

func TestBALUnmeteredAuraReads(t *testing.T) {
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	burner, reader := common.HexToAddress("0x1234"), common.HexToAddress("0x5678")
	code := program.New().Push(0).Op(vm.CALLDATALOAD)
	for slot := range 14_000 {
		code.Op(vm.DUP1).Push(slot).Op(vm.ADD, vm.SLOAD, vm.POP)
	}
	config := chain.AllProtocolChanges.Copy()
	config.Aura = &chain.AuRaConfig{}
	genesis := &types.Genesis{
		Config: config, GasLimit: 128_000_000,
		Alloc: types.GenesisAlloc{
			sender: {Balance: big.NewInt(common.Ether)},
			burner: {Balance: new(big.Int), Code: []byte{0xfe}},
			reader: {Balance: new(big.Int), Code: code.Op(vm.STOP).Bytes()},
		},
	}
	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(key),
		execmoduletester.WithExperimentalBAL(),
		execmoduletester.WithEngine(&auraReadEngine{Engine: merge.New(ethash.NewFaker()), reader: accounts.InternAddress(reader)}),
	)
	pack, err := m.GenerateChain(1, func(_ int, gen *blockgen.BlockGen) {
		for nonce := range uint64(9) {
			gas, price := uint64(16_000_000), uint256.NewInt(m.Genesis.BaseFee().Uint64()*2)
			if nonce == 7 {
				gas = 8_000_000
			} else if nonce == 8 {
				gas, price = 1_000_000, new(uint256.Int)
			}
			tx, err := types.SignTx(types.NewTransaction(nonce, burner, new(uint256.Int), gas, price, nil),
				*types.LatestSignerForChainID(config.ChainID), key)
			require.NoError(t, err)
			gen.AddTxWithChain(nil, m.Engine, tx)
		}
	})
	require.NoError(t, err)
	bal := pack.TopBlock.BlockAccessList()
	idx := slices.IndexFunc(bal, func(ac types.AccountChanges) bool { return ac.Address == reader })
	require.NotEqual(t, -1, idx)
	require.Equal(t, 56_000, len(bal[idx].StorageReads))
	require.NoError(t, bal.ValidateMaxItems(pack.TopBlock.GasLimit()))
	require.NoError(t, m.InsertChain(pack))
}

type blockedWarmupDB struct {
	kv.RoDB
	started chan context.Context
}

func (db *blockedWarmupDB) BeginRo(ctx context.Context) (kv.Tx, error) {
	select {
	case db.started <- ctx:
	default:
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestBALPhantomReadAttackCancelsWarmup(t *testing.T) {
	oldReadAhead := dbg.ReadAhead
	dbg.SetReadAhead(true)
	defer dbg.SetReadAhead(oldReadAhead)
	m, block := phantomReadFixture(t, "attack", 2, "canonical")
	defer m.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	db := &blockedWarmupDB{started: make(chan context.Context, 1)}
	m.ReadAheader.AddBlockAccessList(block.Hash(), block.BlockAccessListSidecar())
	require.NoError(t, m.DB.View(ctx, func(tx kv.Tx) error {
		m.ReadAheader.AddHeaderAndBody(ctx, db, tx, block.Header(), block.Body())
		return nil
	}))
	warmupCtx := <-db.started
	status, err := m.InsertBlocks(t.Context(), []*types.Block{block})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	done := make(chan struct{})
	var result execmodule.ValidationResult
	go func() {
		defer close(done)
		result, err = m.ValidateChain(t.Context(), block.Header())
	}()
	select {
	case <-warmupCtx.Done():
	case <-time.After(time.Second):
		t.Error("rejected block left BAL warmup running")
	}
	cancel()
	<-done
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusBadBlock, result.ValidationStatus)
	require.Contains(t, result.ValidationError, "block access list mismatch")
	m.ReadAheader.WaitForWarmup(t.Context())
}

func TestBALPhantomReadAttack(t *testing.T) {
	for _, name := range []string{"valid", "reads", "attack"} {
		t.Run(name, func(t *testing.T) {
			m, block := phantomReadFixture(t, name, 2, "canonical")
			defer m.Close()
			status, err := m.InsertBlocks(t.Context(), []*types.Block{block})
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, status)
			result, err := m.ValidateChain(t.Context(), block.Header())
			require.NoError(t, err)
			if name == "attack" {
				require.Equal(t, execmodule.ExecutionStatusBadBlock, result.ValidationStatus)
				require.Contains(t, result.ValidationError, "block access list mismatch")
			} else {
				require.Equal(t, execmodule.ExecutionStatusSuccess, result.ValidationStatus, result.ValidationError)
			}
		})
	}
}

func TestBALPhantomReadAttackWithSystemCalls(t *testing.T) {
	for _, scenario := range []string{"custom", "aura"} {
		t.Run(scenario, func(t *testing.T) {
			m, block := phantomReadFixture(t, "attack", 2, scenario)
			defer m.Close()
			err := m.InsertValidateAndUfc1By1(t.Context(), []*types.Block{block})
			require.ErrorContains(t, err, "block access list mismatch")
		})
	}
}

func phantomReadFixture(tb testing.TB, workload string, workers int, scenario string) (*execmoduletester.ExecModuleTester, *types.Block) {
	tb.Helper()
	txCount, phantomSlots := 32, 15_000
	if scenario == "custom" {
		txCount, phantomSlots = 64, 30_000
	} else if scenario == "aura" {
		txCount, phantomSlots = 128, 60_000
	}
	const txGas = uint64(1_000_000)
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(tb, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	burner := common.HexToAddress("0x1234")
	code := program.New()
	const readsPerTx = 400
	if workload == "reads" {
		code.Push(0).Op(vm.CALLDATALOAD)
		for slot := range readsPerTx {
			code.Op(vm.DUP1).Push(slot).Op(vm.ADD, vm.SLOAD, vm.POP)
		}
		code.Op(vm.POP)
	}
	loop := len(code.Bytes())
	code.Op(vm.JUMPDEST).Push(loop).Op(vm.JUMP)
	genesis := &types.Genesis{
		Config:   chain.AllProtocolChanges.Copy(),
		GasLimit: uint64(txCount) * txGas,
		Alloc: types.GenesisAlloc{
			sender: {Balance: new(big.Int).Mul(big.NewInt(100), big.NewInt(common.Ether))},
			burner: {Balance: new(big.Int), Code: code.Bytes()},
		},
	}
	if scenario == "aura" {
		genesis.Config.Aura = &chain.AuRaConfig{}
	}
	opts := []execmoduletester.Option{
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(key),
		execmoduletester.WithExperimentalBAL(),
		execmoduletester.WithExecWorkerCount(workers),
	}
	if scenario == "custom" {
		for _, addr := range []accounts.Address{
			genesis.Config.GetWithdrawalRequestContract(),
			genesis.Config.GetConsolidationRequestContract(),
			genesis.Config.GetBuilderDepositContract(),
			genesis.Config.GetBuilderExitContract(),
		} {
			genesis.Alloc[addr.Value()] = types.GenesisAccount{Balance: new(big.Int), Code: []byte{byte(vm.STOP)}}
		}
		opts = append(opts, execmoduletester.WithoutSystemContracts())
	}
	m := execmoduletester.New(nil, opts...)
	pack, err := m.GenerateChain(1, func(_ int, gen *blockgen.BlockGen) {
		for nonce := range uint64(txCount) {
			var data []byte
			if workload == "reads" {
				data = make([]byte, 32)
				binary.BigEndian.PutUint64(data[24:], nonce*readsPerTx)
			}
			tx, err := types.SignTx(types.NewTransaction(nonce, burner, uint256.NewInt(0), txGas,
				uint256.NewInt(m.Genesis.BaseFee().Uint64()*2), data),
				*types.LatestSignerForChainID(genesis.Config.ChainID), key)
			require.NoError(tb, err)
			gen.AddTx(tx)
		}
	})
	require.NoError(tb, err)
	block := pack.TopBlock
	require.Equal(tb, block.GasLimit(), block.GasUsed())
	if workload == "reads" {
		idx := slices.IndexFunc(block.BlockAccessList(), func(ac types.AccountChanges) bool { return ac.Address == burner })
		require.NotEqual(tb, -1, idx)
		require.Equal(tb, readsPerTx*txCount, len(block.BlockAccessList()[idx].StorageReads))
	}
	if workload == "attack" {
		bal := block.BlockAccessList().Copy()
		idx := slices.IndexFunc(bal, func(ac types.AccountChanges) bool { return ac.Address == burner })
		require.NotEqual(tb, -1, idx)
		for slot := range uint64(phantomSlots) {
			var key common.Hash
			binary.BigEndian.PutUint64(key[24:], slot)
			bal[idx].StorageReads = append(bal[idx].StorageReads, key)
		}
		require.NoError(tb, bal.Validate())
		require.NoError(tb, bal.ValidateMaxItems(block.GasLimit()))
		header := block.Header()
		header.BlockAccessListHash = new(bal.Hash())
		block = types.NewBlockFromNetwork(header, block.Body(), types.NewBlockAccessListSidecar(bal))
	}
	return m, block
}

func TestBALSystemReadsAfterFullBlock(t *testing.T) {
	for _, name := range []string{"default contracts", "custom contracts"} {
		t.Run(name, func(t *testing.T) {
			config := chain.AllProtocolChanges.Copy()
			if name == "custom contracts" {
				config.WithdrawalRequestContract = new(common.HexToAddress("0x1001"))
				config.ConsolidationRequestContract = new(common.HexToAddress("0x1002"))
				config.BuilderDepositContract = new(common.HexToAddress("0x1003"))
				config.BuilderExitContract = new(common.HexToAddress("0x1004"))
			}
			m, pack := generateFullBALBlock(t, config)
			require.NoError(t, m.InsertChain(pack))
		})
	}
}

func TestBALSystemCallExternalReadAfterFullBlock(t *testing.T) {
	config := chain.AllProtocolChanges.Copy()
	contracts := []struct {
		name string
		addr accounts.Address
	}{
		{"withdrawal", config.GetWithdrawalRequestContract()},
		{"consolidation", config.GetConsolidationRequestContract()},
		{"builder deposit", config.GetBuilderDepositContract()},
		{"builder exit", config.GetBuilderExitContract()},
	}
	for _, contract := range contracts {
		t.Run(contract.name, func(t *testing.T) {
			key, err := crypto.GenerateKey()
			require.NoError(t, err)
			sender := crypto.PubkeyToAddress(key.PublicKey)
			burner := common.HexToAddress("0x1234")
			reader := common.HexToAddress("0x5678")
			genesis := &types.Genesis{
				Config:   config,
				GasLimit: 100_000,
				Alloc: types.GenesisAlloc{
					sender: {Balance: big.NewInt(common.Ether)},
					burner: {Balance: new(big.Int), Code: []byte{0xfe}},
					reader: {Balance: new(big.Int), Code: program.New().Push(0).Op(vm.SLOAD, vm.POP, vm.STOP).Bytes()},
				},
			}
			for _, other := range contracts {
				genesis.Alloc[other.addr.Value()] = types.GenesisAccount{Balance: new(big.Int), Code: []byte{byte(vm.STOP)}}
			}
			genesis.Alloc[contract.addr.Value()] = types.GenesisAccount{
				Balance: new(big.Int),
				Code:    program.New().StaticCall(nil, reader, 0, 0, 0, 0).Op(vm.POP, vm.STOP).Bytes(),
			}
			m := execmoduletester.New(t,
				execmoduletester.WithGenesisSpec(genesis),
				execmoduletester.WithKey(key),
				execmoduletester.WithExperimentalBAL(),
				execmoduletester.WithoutSystemContracts(),
			)
			pack, err := m.GenerateChain(1, func(_ int, gen *blockgen.BlockGen) {
				tx, err := types.SignTx(types.NewTransaction(0, burner, uint256.NewInt(0), genesis.GasLimit,
					uint256.NewInt(m.Genesis.BaseFee().Uint64()*2), nil),
					*types.LatestSignerForChainID(config.ChainID), key)
				require.NoError(t, err)
				gen.AddTx(tx)
			})
			require.NoError(t, err)
			require.Equal(t, pack.TopBlock.GasLimit(), pack.TopBlock.GasUsed())
			bal := pack.TopBlock.BlockAccessList()
			require.NoError(t, bal.ValidateMaxItems(pack.TopBlock.GasLimit()))
			idx := slices.IndexFunc(bal, func(ac types.AccountChanges) bool { return ac.Address == reader })
			require.NotEqual(t, -1, idx)
			require.Equal(t, []common.Hash{{}}, bal[idx].StorageReads)
			require.NoError(t, m.InsertChain(pack))
		})
	}
}

func TestBALPhantomReadsAfterFullBlock(t *testing.T) {
	for _, systemContract := range []bool{false, true} {
		name := "ordinary contract"
		if systemContract {
			name = "system contract"
		}
		t.Run(name, func(t *testing.T) {
			m, pack := generateFullBALBlock(t, chain.AllProtocolChanges)
			addr := common.HexToAddress("0x1234")
			if systemContract {
				addr = m.ChainConfig.GetWithdrawalRequestContract().Value()
			}
			bal := pack.TopBlock.BlockAccessList().Copy()
			idx := slices.IndexFunc(bal, func(ac types.AccountChanges) bool { return ac.Address == addr })
			require.NotEqual(t, -1, idx)
			bal[idx].StorageReads = append(bal[idx].StorageReads, common.HexToHash("0xffff"))
			require.NoError(t, bal.Validate())
			require.NoError(t, bal.ValidateMaxItems(pack.TopBlock.GasLimit()))

			header := pack.TopBlock.Header()
			header.BlockAccessListHash = new(bal.Hash())
			block := types.NewBlockFromNetwork(header, pack.TopBlock.Body(), types.NewBlockAccessListSidecar(bal))
			err := m.InsertValidateAndUfc1By1(t.Context(), []*types.Block{block})
			require.ErrorContains(t, err, "block access list mismatch")
		})
	}
}

func TestBALReadsAfterStateGasConsumption(t *testing.T) {
	const readGasLimit = uint64(50_000)
	const slotsPerContract = 15
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	readers := []common.Address{common.HexToAddress("0x2222"), common.HexToAddress("0x3333")}
	code := make([]byte, 0, slotsPerContract*4)
	for slot := range slotsPerContract {
		code = append(code, byte(vm.PUSH1), byte(slot), byte(vm.SLOAD), byte(vm.POP))
	}
	genesis := &types.Genesis{
		Config:   chain.AllProtocolChanges,
		GasLimit: params.StateGasNewAccount + readGasLimit,
		Alloc: types.GenesisAlloc{
			sender: {Balance: big.NewInt(common.Ether)},
		},
	}
	for _, addr := range readers {
		genesis.Alloc[addr] = types.GenesisAccount{Balance: new(big.Int), Code: code}
	}
	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(key),
		execmoduletester.WithExperimentalBAL(),
	)
	pack, err := m.GenerateChain(1, func(_ int, gen *blockgen.BlockGen) {
		gasPrice := uint256.NewInt(m.Genesis.BaseFee().Uint64() * 2)
		signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
		create, err := types.SignTx(types.NewContractCreation(0, uint256.NewInt(0), genesis.GasLimit,
			gasPrice, []byte{byte(vm.STOP)}), *signer, key)
		require.NoError(t, err)
		gen.AddTx(create)
		for i, addr := range readers {
			read, err := types.SignTx(types.NewTransaction(uint64(i+1), addr, uint256.NewInt(0),
				readGasLimit, gasPrice, nil), *signer, key)
			require.NoError(t, err)
			gen.AddTx(read)
		}
	})
	require.NoError(t, err)
	for _, receipt := range pack.Receipts[0] {
		require.Equal(t, types.ReceiptStatusSuccessful, receipt.Status)
	}
	require.EqualValues(t, params.StateGasNewAccount, pack.TopBlock.GasUsed())
	bal := pack.TopBlock.BlockAccessList()
	require.NoError(t, bal.ValidateMaxItems(pack.TopBlock.GasLimit()))
	for _, addr := range readers {
		idx := slices.IndexFunc(bal, func(ac types.AccountChanges) bool { return ac.Address == addr })
		require.NotEqual(t, -1, idx)
		require.Len(t, bal[idx].StorageReads, slotsPerContract)
	}
	require.Greater(t, uint64(len(readers)*slotsPerContract*types.BalItemCost), readGasLimit)
	require.NoError(t, m.InsertChain(pack))
}

func generateFullBALBlock(t *testing.T, config *chain.Config) (*execmoduletester.ExecModuleTester, *blockgen.ChainPack) {
	t.Helper()
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	burner := common.HexToAddress("0x1234")
	genesis := &types.Genesis{
		Config:   config,
		GasLimit: 100_000,
		Alloc: types.GenesisAlloc{
			sender: {Balance: big.NewInt(common.Ether)},
			burner: {Balance: new(big.Int), Code: []byte{0xfe}},
		},
	}
	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(key),
		execmoduletester.WithExperimentalBAL(),
	)
	pack, err := m.GenerateChain(1, func(_ int, gen *blockgen.BlockGen) {
		tx, err := types.SignTx(types.NewTransaction(0, burner, uint256.NewInt(0), genesis.GasLimit,
			uint256.NewInt(m.Genesis.BaseFee().Uint64()*2), nil),
			*types.LatestSignerForChainID(m.ChainConfig.ChainID), key)
		require.NoError(t, err)
		gen.AddTx(tx)
	})
	require.NoError(t, err)
	require.Equal(t, pack.TopBlock.GasLimit(), pack.TopBlock.GasUsed())
	bal := pack.TopBlock.BlockAccessList()
	require.NoError(t, bal.ValidateMaxItems(pack.TopBlock.GasLimit()))
	require.True(t, slices.ContainsFunc(bal, func(ac types.AccountChanges) bool { return len(ac.StorageReads) > 0 }))
	return m, pack
}
