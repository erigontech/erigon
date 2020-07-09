// Copyright 2018 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package miner

import (
	"context"
	"crypto/ecdsa"
	"math/big"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/consensus/clique"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/event"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
)

const (
	// testCode is the testing contract binary code which will initialises some
	// variables in constructor
	testCode = "0x60806040527fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0060005534801561003457600080fd5b5060fc806100436000396000f3fe6080604052348015600f57600080fd5b506004361060325760003560e01c80630c4dae8814603757806398a213cf146053575b600080fd5b603d607e565b6040518082815260200191505060405180910390f35b607c60048036036020811015606757600080fd5b81019080803590602001909291905050506084565b005b60005481565b806000819055507fe9e44f9f7da8c559de847a3232b57364adc0354f15a2cd8dc636d54396f9587a6000546040518082815260200191505060405180910390a15056fea265627a7a723058208ae31d9424f2d0bc2a3da1a5dd659db2d71ec322a17db8f87e19e209e3a1ff4a64736f6c634300050a0032"

	// testGas is the gas required for contract deployment.
	testGas = 144109
)

type testCase struct {
	testTxPoolConfig  core.TxPoolConfig
	ethashChainConfig *params.ChainConfig
	cliqueChainConfig *params.ChainConfig

	testBankKey     *ecdsa.PrivateKey
	testBankAddress common.Address
	testBankFunds   *big.Int

	testUserKey     *ecdsa.PrivateKey
	testUserAddress common.Address
	testUserFunds   *uint256.Int

	// Test transactions
	pendingTxs []*types.Transaction
	newTxs     []*types.Transaction

	testConfig *Config
}

func getTestCase() (*testCase, error) {
	testBankKey, err := crypto.GenerateKey()
	if err != nil {
		return nil, err
	}

	testUserKey, err := crypto.GenerateKey()
	if err != nil {
		return nil, err
	}

	t := &testCase{
		testBankKey:     testBankKey,
		testBankAddress: crypto.PubkeyToAddress(testBankKey.PublicKey),
		testBankFunds:   big.NewInt(1000000000000000000),

		testUserKey:     testUserKey,
		testUserAddress: crypto.PubkeyToAddress(testUserKey.PublicKey),
		testUserFunds:   uint256.NewInt().SetUint64(1000),

		testConfig: &Config{
			Recommit: time.Second,
			GasFloor: params.GenesisGasLimit,
			GasCeil:  params.GenesisGasLimit,
		},
	}

	t.testTxPoolConfig = core.DefaultTxPoolConfig
	t.testTxPoolConfig.Journal = ""

	t.ethashChainConfig = params.TestChainConfig
	t.cliqueChainConfig = params.TestChainConfig
	t.cliqueChainConfig.Clique = &params.CliqueConfig{
		Period: 10,
		Epoch:  30000,
	}

	tx1, err := types.SignTx(types.NewTransaction(0, t.testUserAddress, t.testUserFunds, params.TxGas, nil, nil), types.HomesteadSigner{}, testBankKey)
	if err != nil {
		return nil, err
	}
	t.pendingTxs = append(t.pendingTxs, tx1)

	tx2, err := types.SignTx(types.NewTransaction(1, t.testUserAddress, t.testUserFunds, params.TxGas, nil, nil), types.HomesteadSigner{}, testBankKey)
	if err != nil {
		return nil, err
	}
	t.newTxs = append(t.newTxs, tx2)

	rand.Seed(time.Now().UnixNano())

	return t, nil
}

// testWorkerBackend implements worker.Backend interfaces and wraps all information needed during the testing.
type testWorkerBackend struct {
	db         *ethdb.ObjectDatabase
	txPool     *core.TxPool
	chain      *core.BlockChain
	testTxFeed event.Feed
	genesis    *core.Genesis
	uncleBlock *types.Block
}

func newTestWorkerBackend(t *testing.T, testCase *testCase, chainConfig *params.ChainConfig, engine consensus.Engine, db *ethdb.ObjectDatabase, n int) (*testWorkerBackend, func()) {
	var (
		gspec = core.Genesis{
			Config: chainConfig,
			Alloc: core.GenesisAlloc{
				testCase.testBankAddress: {Balance: testCase.testBankFunds},
			},
		}
	)

	switch engine.(type) {
	case *clique.Clique:
		gspec.ExtraData = make([]byte, 32+common.AddressLength+crypto.SignatureLength)
		copy(gspec.ExtraData[32:], testCase.testBankAddress[:])
	case *ethash.Ethash:
		// nothing to do
	default:
		t.Fatalf("unexpected consensus engine type: %T", engine)
	}

	genesis := gspec.MustCommit(db)

	chain, _ := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil, nil, nil)
	txpool := core.NewTxPool(testCase.testTxPoolConfig, chainConfig, chain)

	// Generate a small n-block chain and an uncle block for it
	var dbSide *ethdb.ObjectDatabase
	var parentSide *types.Block
	if n > 0 {
		if n-1 > 0 {
			blocks, _, err := core.GenerateChain(chainConfig, genesis, engine, db, n-1, func(i int, gen *core.BlockGen) {
				gen.SetCoinbase(testCase.testBankAddress)
			})
			if err != nil {
				t.Fatalf("generate blocks: %v", err)
			}
			if _, err = chain.InsertChain(context.Background(), blocks); err != nil {
				t.Fatalf("failed to insert origin chain: %v", err)
			}
		}

		dbSide = db.MemCopy()
		defer dbSide.Close()
		parentSide = chain.CurrentBlock()

		ctx := chain.WithContext(context.Background(), big.NewInt(parentSide.Number().Int64()+1))
		blocks, _, err := core.GenerateChain(chainConfig, parentSide, engine, db, 1, func(i int, gen *core.BlockGen) {
			gen.SetCoinbase(testCase.testBankAddress)
		})
		if err != nil {
			t.Fatalf("generate blocks: %v", err)
		}
		if _, err = chain.InsertChain(ctx, blocks); err != nil {
			t.Fatalf("failed to insert origin chain: %v", err)
		}
	}

	if parentSide == nil {
		parentSide = genesis
	}
	if dbSide == nil {
		dbSide = db
	}

	sideBlocks, _, err := core.GenerateChain(chainConfig, parentSide, engine, dbSide, 1, func(i int, gen *core.BlockGen) {
		gen.SetCoinbase(testCase.testUserAddress)
	})
	if err != nil {
		t.Fatalf("generage sideBlocks: %v", err)
	}

	return &testWorkerBackend{
			db:         db,
			chain:      chain,
			txPool:     txpool,
			genesis:    &gspec,
			uncleBlock: sideBlocks[0],
		}, func() {
			chain.Stop()
			chain.ChainDb().Close()
			db.Close()
		}
}

func (b *testWorkerBackend) BlockChain() *core.BlockChain { return b.chain }
func (b *testWorkerBackend) TxPool() *core.TxPool         { return b.txPool }

func newTestWorker(t *testCase, chainConfig *params.ChainConfig, engine consensus.Engine, backend Backend, h hooks, waitInit bool) *worker {
	w := newWorker(t.testConfig, chainConfig, engine, backend, new(event.TypeMux), h, true)
	w.setEtherbase(t.testBankAddress)
	if waitInit {
		w.init(vm.NewDestsCache(100))

		// Ensure worker has finished initialization
		timer := time.NewTicker(10 * time.Millisecond)
		defer timer.Stop()
		for range timer.C {
			b := w.pendingBlock()
			if b != nil && b.NumberU64() >= 1 {
				break
			}
		}
	}
	return w
}

func newTestBackend(t *testing.T, testCase *testCase, chainConfig *params.ChainConfig, engine consensus.Engine, db *ethdb.ObjectDatabase, blocks int) (*testWorkerBackend, func()) {
	backend, clear := newTestWorkerBackend(t, testCase, chainConfig, engine, db, blocks)

	errs := backend.txPool.AddLocals(testCase.pendingTxs)
	for _, err := range errs {
		if err != nil {
			t.Fatal(errs)
		}
	}

	return backend, clear
}

func TestGenerateBlockAndImportEthash(t *testing.T) {
	t.Skip("should be fixed. a new test from geth 1.9.7")
	testCase, err := getTestCase()
	if err != nil {
		t.Error(err)
	}
	testGenerateBlockAndImport(t, testCase, false)
}

func TestGenerateBlockAndImportClique(t *testing.T) {
	t.Skip("should be restored. skipped for turbo-geth. tag: Clique")
	testCase, err := getTestCase()
	if err != nil {
		t.Error(err)
	}
	testGenerateBlockAndImport(t, testCase, true)
}

func testGenerateBlockAndImport(t *testing.T, testCase *testCase, isClique bool) {
	var (
		engine      consensus.Engine
		chainConfig *params.ChainConfig
		db          = ethdb.NewMemDatabase()
	)
	defer db.Close()
	if isClique {
		chainConfig = params.AllCliqueProtocolChanges
		chainConfig.Clique = &params.CliqueConfig{Period: 1, Epoch: 30000}
		engine = clique.New(chainConfig.Clique, db)
	} else {
		chainConfig = params.AllEthashProtocolChanges
		engine = ethash.NewFaker()
	}

	b, clear := newTestBackend(t, testCase, chainConfig, engine, db, 0)
	defer clear()
	w := newTestWorker(testCase, chainConfig, engine, b, hooks{}, false)
	defer w.close()

	db2 := ethdb.NewMemDatabase()
	defer db2.Close()
	b.genesis.MustCommit(db2)
	chain, _ := core.NewBlockChain(db2, nil, b.chain.Config(), engine, vm.Config{}, nil, nil, nil)
	defer chain.Stop()

	// Ignore empty commit here for less noise
	w.skipSealHook = func(task *task) bool {
		return len(task.receipts) == 0
	}

	// Wait for mined blocks.
	sub := w.mux.Subscribe(core.NewMinedBlockEvent{})
	defer sub.Unsubscribe()

	// Start mining!
	w.start(vm.NewDestsCache(100))

	for i := 0; i < 5; i++ {
		if err := b.txPool.AddLocal(b.newRandomTx(testCase, true)); err != nil {
			t.Fatal(err)
		}
		if err := b.txPool.AddLocal(b.newRandomTx(testCase, false)); err != nil {
			t.Fatal(err)
		}
		w.postSideBlock(core.ChainSideEvent{Block: b.newRandomUncle()})
		w.postSideBlock(core.ChainSideEvent{Block: b.newRandomUncle()})

		select {
		case ev := <-sub.Chan():
			block := ev.Data.(core.NewMinedBlockEvent).Block
			if _, err := chain.InsertChain(context.Background(), []*types.Block{block}); err != nil {
				t.Fatalf("failed to insert new mined block %d: %v", block.NumberU64(), err)
			}
		case <-time.After(3 * time.Second): // Worker needs 1s to include new changes.
			t.Fatalf("timeout")
		}
	}
}

func (b *testWorkerBackend) newRandomUncle() *types.Block {
	var parent *types.Block
	cur := b.chain.CurrentBlock()
	if cur.NumberU64() == 0 {
		parent = b.chain.Genesis()
	} else {
		parent = b.chain.GetBlockByHash(b.chain.CurrentBlock().ParentHash())
	}
	blocks, _, err := core.GenerateChain(b.chain.Config(), parent, b.chain.Engine(), b.db, 1, func(i int, gen *core.BlockGen) {
		var addr = make([]byte, common.AddressLength)
		//nolint:gosec
		rand.Read(addr)
		gen.SetCoinbase(common.BytesToAddress(addr))
	})
	if err != nil {
		panic(err)
	}
	return blocks[0]
}

func (b *testWorkerBackend) newRandomTx(testCase *testCase, creation bool) *types.Transaction {
	var tx *types.Transaction
	if creation {
		tx, _ = types.SignTx(types.NewContractCreation(b.txPool.Nonce(testCase.testBankAddress), uint256.NewInt(), testGas, nil, common.FromHex(testCode)), types.HomesteadSigner{}, testCase.testBankKey)
	} else {
		tx, _ = types.SignTx(types.NewTransaction(b.txPool.Nonce(testCase.testBankAddress), testCase.testUserAddress, uint256.NewInt().SetUint64(1000), params.TxGas, nil, nil), types.HomesteadSigner{}, testCase.testBankKey)
	}
	return tx
}

func TestPendingStateAndBlockEthash(t *testing.T) {
	t.Skip("should be restored. works only on small values of difficulty. tag: Mining")
	testCase, err := getTestCase()
	if err != nil {
		t.Error(err)
	}
	testPendingStateAndBlock(t, testCase, testCase.ethashChainConfig, ethash.NewFaker())
}
func TestPendingStateAndBlockClique(t *testing.T) {
	t.Skip("should be restored. skipped for turbo-geth. tag: Clique")
	testCase, err := getTestCase()
	if err != nil {
		t.Error(err)
	}
	testPendingStateAndBlock(t, testCase, testCase.cliqueChainConfig, clique.New(testCase.cliqueChainConfig.Clique, ethdb.NewMemDatabase()))
}

func testPendingStateAndBlock(t *testing.T, testCase *testCase, chainConfig *params.ChainConfig, engine consensus.Engine) {
	defer engine.Close()

	b, clear := newTestBackend(t, testCase, chainConfig, engine, ethdb.NewMemDatabase(), 0)
	defer clear()
	w := newTestWorker(testCase, chainConfig, engine, b, hooks{}, true)
	defer w.close()

	// Ensure snapshot has been updated.
	time.Sleep(100 * time.Millisecond)
	block, state, _ := w.pending()
	if block == nil {
		t.Fatalf("block number mismatch: have nil, want %d", 1)
	}
	if block.NumberU64() != 1 {
		t.Errorf("block number mismatch: have %d, want %d", block.NumberU64(), 1)
	}
	if balance := state.GetBalance(testCase.testUserAddress); balance.Uint64() != 1000 {
		t.Errorf("account balance mismatch: have %d, want %d", balance, 1000)
	}
	b.txPool.AddLocals(testCase.newTxs)

	// Ensure the new tx events has been processed
	time.Sleep(100 * time.Millisecond)
	block, state, _ = w.pending()
	if balance := state.GetBalance(testCase.testUserAddress); balance.Uint64() != 2000 {
		t.Errorf("account balance mismatch: have %d, want %d", balance, 2000)
	}
}

func TestEmptyWorkEthash(t *testing.T) {
	t.Skip("should be restored. Unstable. tag: Mining")
	testCase, err := getTestCase()
	if err != nil {
		t.Error(err)
	}
	testEmptyWork(t, testCase, testCase.ethashChainConfig, ethash.NewFaker())
}
func TestEmptyWorkClique(t *testing.T) {
	t.Skip("should be restored. skipped for turbo-geth. tag: Clique")
	testCase, err := getTestCase()
	if err != nil {
		t.Error(err)
	}
	testEmptyWork(t, testCase, testCase.cliqueChainConfig, clique.New(testCase.cliqueChainConfig.Clique, ethdb.NewMemDatabase()))
}

func testEmptyWork(t *testing.T, testCase *testCase, chainConfig *params.ChainConfig, engine consensus.Engine) {
	defer engine.Close()

	taskCh := make(chan struct{})
	h := hooks{
		newTaskHook: func(task *task) {
			gotBalance := task.state.GetBalance(testCase.testUserAddress).Uint64()
			gotBankBalance := task.state.GetBalance(testCase.testBankAddress).Uint64()

			log.Warn("mining: newTaskHook",
				"balance", gotBalance,
				"number", task.block.NumberU64(),
				"hash", task.block.Hash().String(),
				"parentHash", task.block.ParentHash().String(),
			)

			emptyMiningNewTaskHook(t, testCase, task.block, taskCh, gotBalance, gotBankBalance)
		},
		fullTaskHook: func() {
			time.Sleep(100 * time.Millisecond)
		},
	}

	backend, clear := newTestBackend(t, testCase, chainConfig, engine, ethdb.NewMemDatabase(), 0)
	defer clear()
	w := newTestWorker(testCase, chainConfig, engine, backend, h, true)
	defer w.close()

	w.start(vm.NewDestsCache(100))
	for i := 0; i < 3; i++ {
		select {
		case <-taskCh:
			// nothing to do
		case <-time.NewTimer(2 * time.Second).C:
			t.Error("new task timeout")
		}
	}
}

func checkEmptyMining(t *testing.T, testCase *testCase, gotBalance uint64, gotBankBalance uint64, index int) {
	var balance uint64
	if index > 0 {
		balance = 1000
	}

	if gotBalance != balance {
		t.Errorf("account balance mismatch: have %d, want %d. index %d. %v %v",
			gotBalance,
			balance, index,
			testCase.testBankFunds.Uint64(), gotBankBalance)
	}
}

func emptyMiningNewTaskHook(t *testing.T, testCase *testCase, block *types.Block, taskCh chan<- struct{}, gotBalance uint64, gotBankBalance uint64) {
	checkEmptyMining(t, testCase, gotBalance, gotBankBalance, int(block.NumberU64()))
	taskCh <- struct{}{}
}

func TestStreamUncleBlock(t *testing.T) {
	ethash := ethash.NewFaker()
	defer ethash.Close()

	testCase, err := getTestCase()
	if err != nil {
		t.Error(err)
	}

	b, clear := newTestBackend(t, testCase, testCase.ethashChainConfig, ethash, ethdb.NewMemDatabase(), 1)
	defer clear()

	var taskCh = make(chan struct{}, 1)
	taskIndex := 0

	h := hooks{
		newTaskHook: func(task *task) {
			if task.block.NumberU64() == 2 {
				if taskIndex == 2 {
					have := task.block.Header().UncleHash
					want := types.CalcUncleHash([]*types.Header{b.uncleBlock.Header()})
					if have != want {
						t.Errorf("uncle hash mismatch: have %s, want %s", have.Hex(), want.Hex())
					}
				}
				taskCh <- struct{}{}
				taskIndex++
			}
		},
		skipSealHook: func(task *task) bool {
			return true
		},
		fullTaskHook: func() {
			time.Sleep(100 * time.Millisecond)
		},
	}

	w := newTestWorker(testCase, testCase.ethashChainConfig, ethash, b, h, true)
	defer w.close()

	w.start(vm.NewDestsCache(100))

	// Ignore the first two works
	for i := 0; i < 2; i += 1 {
		select {
		case <-taskCh:
		case <-time.NewTimer(time.Second).C:
			t.Error("new task timeout")
		}
	}

	w.postSideBlock(core.ChainSideEvent{Block: b.uncleBlock})

	select {
	case <-taskCh:
	case <-time.NewTimer(time.Second).C:
		t.Error("new task timeout")
	}
}

func TestRegenerateMiningBlockEthash(t *testing.T) {
	t.Skip("should be restored. works only on small values of difficulty. tag: Mining")
	testCase, err := getTestCase()
	if err != nil {
		t.Error(err)
	}
	testRegenerateMiningBlock(t, testCase, testCase.ethashChainConfig, ethash.NewFaker())
}
func TestRegenerateMiningBlockClique(t *testing.T) {
	t.Skip("should be restored. skipped for turbo-geth. tag: Clique")
	testCase, err := getTestCase()
	if err != nil {
		t.Error(err)
	}
	testRegenerateMiningBlock(t, testCase, testCase.cliqueChainConfig, clique.New(testCase.cliqueChainConfig.Clique, ethdb.NewMemDatabase()))
}

func testRegenerateMiningBlock(t *testing.T, testCase *testCase, chainConfig *params.ChainConfig, engine consensus.Engine) {
	defer engine.Close()

	var taskCh = make(chan struct{})
	taskIndex := 0
	h := hooks{
		newTaskHook: func(task *task) {
			if task.block.NumberU64() == 1 {
				if taskIndex == 2 {
					receiptLen, balance := 2, uint256.NewInt().SetUint64(2000)
					if len(task.receipts) != receiptLen {
						t.Errorf("receipt number mismatch: have %d, want %d", len(task.receipts), receiptLen)
					}
					if task.state.GetBalance(testCase.testUserAddress).Cmp(balance) != 0 {
						t.Errorf("account balance mismatch: have %d, want %d", task.state.GetBalance(testCase.testUserAddress), balance)
					}
				}
				taskCh <- struct{}{}
				taskIndex++
			}
		},
		skipSealHook: func(task *task) bool {
			return true
		},
		fullTaskHook: func() {
			time.Sleep(100 * time.Millisecond)
		},
	}

	b, clear := newTestBackend(t, testCase, chainConfig, engine, ethdb.NewMemDatabase(), 0)
	defer clear()

	w := newTestWorker(testCase, chainConfig, engine, b, h, true)
	defer w.close()

	w.start(vm.NewDestsCache(100))
	// Ignore the first two works
	for i := 0; i < 2; i += 1 {
		select {
		case <-taskCh:
		case <-time.NewTimer(time.Second).C:
			t.Error("new task timeout on first 2 works")
		}
	}
	b.txPool.AddLocals(testCase.newTxs)
	time.Sleep(time.Second)

	select {
	case <-taskCh:
	case <-time.NewTimer(time.Second).C:
		t.Error("new task timeout")
	}
}

func TestAdjustIntervalEthash(t *testing.T) {
	testCase, err := getTestCase()
	if err != nil {
		t.Error(err)
	}
	testAdjustInterval(t, testCase, testCase.ethashChainConfig, ethash.NewFaker())
}

func TestAdjustIntervalClique(t *testing.T) {
	t.Skip("should be restored. skipped for turbo-geth. tag: Clique")
	testCase, err := getTestCase()
	if err != nil {
		t.Error(err)
	}
	testAdjustInterval(t, testCase, testCase.cliqueChainConfig, clique.New(testCase.cliqueChainConfig.Clique, ethdb.NewMemDatabase()))
}

func testAdjustInterval(t *testing.T, testCase *testCase, chainConfig *params.ChainConfig, engine consensus.Engine) {
	defer engine.Close()

	var (
		progress = make(chan struct{}, 10)
		result   = make([]float64, 0, 10)
		index    = 0
		start    uint32
	)
	h := hooks{
		skipSealHook: func(task *task) bool {
			return true
		},
		fullTaskHook: func() {
			time.Sleep(100 * time.Millisecond)
		},
		resubmitHook: func(minInterval time.Duration, recommitInterval time.Duration) {
			// Short circuit if interval checking hasn't started.
			if atomic.LoadUint32(&start) == 0 {
				return
			}

			var wantMinInterval, wantRecommitInterval time.Duration

			switch index {
			case 0:
				wantMinInterval, wantRecommitInterval = 3*time.Second, 3*time.Second
			case 1:
				origin := float64(3 * time.Second.Nanoseconds())
				estimate := origin*(1-intervalAdjustRatio) + intervalAdjustRatio*(origin/0.8+intervalAdjustBias)
				wantMinInterval, wantRecommitInterval = 3*time.Second, time.Duration(estimate)*time.Nanosecond
			case 2:
				estimate := result[index-1]
				min := float64(3 * time.Second.Nanoseconds())
				estimate = estimate*(1-intervalAdjustRatio) + intervalAdjustRatio*(min-intervalAdjustBias)
				wantMinInterval, wantRecommitInterval = 3*time.Second, time.Duration(estimate)*time.Nanosecond
			case 3:
				wantMinInterval, wantRecommitInterval = time.Second, time.Second
			}

			// Check interval
			if minInterval != wantMinInterval {
				t.Errorf("resubmit min interval mismatch: have %v, want %v ", minInterval, wantMinInterval)
			}
			if recommitInterval != wantRecommitInterval {
				t.Errorf("resubmit interval mismatch: have %v, want %v", recommitInterval, wantRecommitInterval)
			}
			result = append(result, float64(recommitInterval.Nanoseconds()))
			index++
			progress <- struct{}{}
		},
	}

	backend, clear := newTestBackend(t, testCase, chainConfig, engine, ethdb.NewMemDatabase(), 0)
	defer clear()
	w := newTestWorker(testCase, chainConfig, engine, backend, h, true)
	defer w.close()

	w.start(vm.NewDestsCache(100))

	time.Sleep(time.Second) // Ensure two tasks have been summitted due to start opt
	atomic.StoreUint32(&start, 1)

	w.setRecommitInterval(3 * time.Second)
	select {
	case <-progress:
	case <-time.NewTimer(time.Second).C:
		t.Error("interval reset timeout")
	}

	w.resubmitAdjustCh <- &intervalAdjust{inc: true, ratio: 0.8}
	select {
	case <-progress:
	case <-time.NewTimer(time.Second).C:
		t.Error("interval reset timeout")
	}

	w.resubmitAdjustCh <- &intervalAdjust{inc: false}
	select {
	case <-progress:
	case <-time.NewTimer(time.Second).C:
		t.Error("interval reset timeout")
	}

	w.setRecommitInterval(500 * time.Millisecond)
	select {
	case <-progress:
	case <-time.NewTimer(time.Second).C:
		t.Error("interval reset timeout")
	}
}
