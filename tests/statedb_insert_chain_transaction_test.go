package tests

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/consensus"

	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/tests/contracts"
)

func TestInsertIncorrectStateRootDifferentAccounts(t *testing.T) {
	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := common.Address{1}

	engine, db, blocks, receipts, clear, err := genBlocks(data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt().SetUint64(1000)),
			fromKey,
		},
		1: {
			getBlockTx(from, to, uint256.NewInt().SetUint64(2000)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear()

	// BLOCK 1
	incorrectHeader := blocks[0].Header()
	incorrectHeader.Root = blocks[1].Header().Root

	if blocks[0].Header().Root == incorrectHeader.Root {
		t.Fatal("roots are the same")
	}

	incorrectBlock := types.NewBlock(incorrectHeader, blocks[0].Transactions(), blocks[0].Uncles(), receipts[0])
	if _, err = stagedsync.InsertBlockInStages(db, data.genesisSpec.Config, &vm.Config{}, engine, incorrectBlock, true /* checkRoot */); err == nil {
		t.Fatal("should fail")
	}

	// insert a correct block
	engine, db, blocks, _, clear, err = genBlocks(data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(data.addresses[1], to, uint256.NewInt().SetUint64(5000)),
			data.keys[1],
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear()

	if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, data.genesisSpec.Config, &vm.Config{}, engine, blocks, true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	st := state.New(state.NewDbStateReader(db))
	if !st.Exist(to) {
		t.Error("expected account to exist")
	}

	if balance := st.GetBalance(from); balance.Uint64() != 1000000000 {
		t.Fatalf("got %v, expected %v", balance, 1000000000)
	}
	if balance := st.GetBalance(data.addresses[1]); balance.Uint64() != 999995000 {
		t.Fatalf("got %v, expected %v", balance, 999995000)
	}
	if balance := st.GetBalance(to); balance.Uint64() != 5000 {
		t.Fatalf("got %v, expected %v", balance, 5000)
	}
}

func TestInsertIncorrectStateRootSameAccount(t *testing.T) {
	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := common.Address{1}

	engine, db, blocks, receipts, clear, err := genBlocks(data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt().SetUint64(1000)),
			fromKey,
		},
		1: {
			getBlockTx(from, to, uint256.NewInt().SetUint64(2000)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear()

	// BLOCK 1
	incorrectHeader := blocks[0].Header()
	incorrectHeader.Root = blocks[1].Header().Root

	if blocks[0].Header().Root == incorrectHeader.Root {
		t.Fatal("roots are the same")
	}

	incorrectBlock := types.NewBlock(incorrectHeader, blocks[0].Transactions(), blocks[0].Uncles(), receipts[0])
	if _, err = stagedsync.InsertBlockInStages(db, data.genesisSpec.Config, &vm.Config{}, engine, incorrectBlock, true /* checkRoot */); err == nil {
		t.Fatal("should fail")
	}

	// insert a correct block
	engine, db, blocks, _, clear, err = genBlocks(data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt().SetUint64(5000)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear()

	if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, data.genesisSpec.Config, &vm.Config{}, engine, blocks, true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	st := state.New(state.NewDbStateReader(db))
	if !st.Exist(to) {
		t.Error("expected account to exist")
	}

	if balance := st.GetBalance(from); balance.Uint64() != 999995000 {
		t.Fatalf("got %v, expected %v", balance, 999995000)
	}
	if balance := st.GetBalance(to); balance.Uint64() != 5000 {
		t.Fatalf("got %v, expected %v", balance, 5000)
	}
}

func TestInsertIncorrectStateRootSameAccountSameAmount(t *testing.T) {
	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := common.Address{1}

	engine, db, blocks, receipts, clear, err := genBlocks(data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt().SetUint64(1000)),
			fromKey,
		},
		1: {
			getBlockTx(from, to, uint256.NewInt().SetUint64(2000)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear()

	// BLOCK 1
	incorrectHeader := blocks[0].Header()
	incorrectHeader.Root = blocks[1].Header().Root

	incorrectBlock := types.NewBlock(incorrectHeader, blocks[0].Transactions(), blocks[0].Uncles(), receipts[0])

	if _, err = stagedsync.InsertBlockInStages(db, data.genesisSpec.Config, &vm.Config{}, engine, incorrectBlock, true /* checkRoot */); err == nil {
		t.Fatal("should fail")
	}

	// insert a correct block
	engine, db, blocks, _, clear, err = genBlocks(data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt().SetUint64(1000)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear()

	if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, data.genesisSpec.Config, &vm.Config{}, engine, blocks, true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	st := state.New(state.NewDbStateReader(db))
	if !st.Exist(to) {
		t.Error("expected account to exist")
	}

	if balance := st.GetBalance(from); balance.Uint64() != 999999000 {
		t.Fatalf("got %v, expected %v", balance, 999999000)
	}
	if balance := st.GetBalance(to); balance.Uint64() != 1000 {
		t.Fatalf("got %v, expected %v", balance, 1000)
	}
}

func TestInsertIncorrectStateRootAllFundsRoot(t *testing.T) {
	data := getGenesis(big.NewInt(3000))
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := common.Address{1}

	engine, db, blocks, receipts, clear, err := genBlocks(data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt().SetUint64(1000)),
			fromKey,
		},
		1: {
			getBlockTx(from, to, uint256.NewInt().SetUint64(2000)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear()

	// BLOCK 1
	incorrectHeader := blocks[0].Header()
	incorrectHeader.Root = blocks[1].Header().Root

	incorrectBlock := types.NewBlock(incorrectHeader, blocks[0].Transactions(), blocks[0].Uncles(), receipts[0])

	if _, err = stagedsync.InsertBlockInStages(db, data.genesisSpec.Config, &vm.Config{}, engine, incorrectBlock, true /* checkRoot */); err == nil {
		t.Fatal("should fail")
	}

	// insert a correct block
	engine, db, blocks, _, clear, err = genBlocks(data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt().SetUint64(1000)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear()

	if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, data.genesisSpec.Config, &vm.Config{}, engine, blocks, true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	st := state.New(state.NewDbStateReader(db))
	if !st.Exist(to) {
		t.Error("expected account to exist")
	}

	if balance := st.GetBalance(from); balance.Uint64() != 2000 {
		t.Fatalf("got %v, expected %v", balance, 2000)
	}
	if balance := st.GetBalance(to); balance.Uint64() != 1000 {
		t.Fatalf("got %v, expected %v", balance, 1000)
	}
}

func TestInsertIncorrectStateRootAllFunds(t *testing.T) {
	data := getGenesis(big.NewInt(3000))
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := common.Address{1}

	engine, db, blocks, receipts, clear, err := genBlocks(data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt().SetUint64(3000)),
			fromKey,
		},
		1: {
			getBlockTx(data.addresses[1], to, uint256.NewInt().SetUint64(2000)),
			data.keys[1],
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear()

	// BLOCK 1
	incorrectHeader := blocks[0].Header()
	incorrectHeader.Root = blocks[1].Header().Root
	incorrectBlock := types.NewBlock(incorrectHeader, blocks[0].Transactions(), blocks[0].Uncles(), receipts[0])

	if _, err = stagedsync.InsertBlockInStages(db, data.genesisSpec.Config, &vm.Config{}, engine, incorrectBlock, true /* checkRoot */); err == nil {
		t.Fatal("should fail")
	}

	// insert a correct block
	engine, db, blocks, _, clear, err = genBlocks(data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt().SetUint64(1000)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear()

	if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, data.genesisSpec.Config, &vm.Config{}, engine, blocks, true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	st := state.New(state.NewDbStateReader(db))
	if !st.Exist(to) {
		t.Error("expected account to exist")
	}

	if balance := st.GetBalance(from); balance.Uint64() != 2000 {
		t.Fatalf("got %v, expected %v", balance, 2000)
	}
	if balance := st.GetBalance(to); balance.Uint64() != 1000 {
		t.Fatalf("got %v, expected %v", balance, 1000)
	}
}

func TestAccountDeployIncorrectRoot(t *testing.T) {
	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := common.Address{1}

	var contractAddress common.Address
	eipContract := new(contracts.Testcontract)

	engine, db, blocks, receipts, clear, err := genBlocks(data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt().SetUint64(10)),
			fromKey,
		},
		1: {
			getBlockDeployTestContractTx(data.transactOpts[0], &contractAddress, eipContract),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear()

	// BLOCK 1
	if _, err = stagedsync.InsertBlockInStages(db, data.genesisSpec.Config, &vm.Config{}, engine, blocks[0], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	st := state.New(state.NewDbStateReader(db))
	if !st.Exist(from) {
		t.Error("expected account to exist")
	}

	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist at the block 0", contractAddress.Hash().String())
	}

	incorrectHeader := blocks[1].Header()
	incorrectHeader.Root = blocks[0].Header().Root
	incorrectBlock := types.NewBlock(incorrectHeader, blocks[1].Transactions(), blocks[1].Uncles(), receipts[1])

	// BLOCK 2 - INCORRECT
	if _, err = stagedsync.InsertBlockInStages(db, data.genesisSpec.Config, &vm.Config{}, engine, incorrectBlock, true /* checkRoot */); err == nil {
		t.Fatal("should fail")
	}

	st = state.New(state.NewDbStateReader(db))
	if !st.Exist(from) {
		t.Error("expected account to exist")
	}

	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist at the block 1", contractAddress.Hash().String())
	}

	// BLOCK 2 - CORRECT
	if _, err = stagedsync.InsertBlockInStages(db, data.genesisSpec.Config, &vm.Config{}, engine, blocks[1], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	st = state.New(state.NewDbStateReader(db))
	if !st.Exist(from) {
		t.Error("expected account to exist")
	}

	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist at the block 1", contractAddress.Hash().String())
	}
}

func TestAccountCreateIncorrectRoot(t *testing.T) {
	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := common.Address{1}

	var contractAddress common.Address
	eipContract := new(contracts.Testcontract)

	engine, db, blocks, receipts, clear, err := genBlocks(data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt().SetUint64(10)),
			fromKey,
		},
		1: {
			getBlockDeployTestContractTx(data.transactOpts[0], &contractAddress, eipContract),
			fromKey,
		},
		2: {
			getBlockTestContractTx(data.transactOpts[0], eipContract.Create, big.NewInt(2)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear()

	// BLOCK 1
	if _, err = stagedsync.InsertBlockInStages(db, data.genesisSpec.Config, &vm.Config{}, engine, blocks[0], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	st := state.New(state.NewDbStateReader(db))
	if !st.Exist(from) {
		t.Error("expected account to exist")
	}

	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist at the block 0", contractAddress.Hash().String())
	}

	// BLOCK 2
	if _, err = stagedsync.InsertBlockInStages(db, data.genesisSpec.Config, &vm.Config{}, engine, blocks[1], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	st = state.New(state.NewDbStateReader(db))
	if !st.Exist(from) {
		t.Error("expected account to exist")
	}

	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 2", contractAddress.Hash().String())
	}

	// BLOCK 3 - INCORRECT
	incorrectHeader := blocks[2].Header()
	incorrectHeader.Root = blocks[1].Header().Root
	incorrectBlock := types.NewBlock(incorrectHeader, blocks[2].Transactions(), blocks[2].Uncles(), receipts[2])

	if _, err = stagedsync.InsertBlockInStages(db, data.genesisSpec.Config, &vm.Config{}, engine, incorrectBlock, true /* checkRoot */); err == nil {
		t.Fatal("should fail")
	}

	// BLOCK 3
	if _, err = stagedsync.InsertBlockInStages(db, data.genesisSpec.Config, &vm.Config{}, engine, blocks[2], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}
}

func TestAccountUpdateIncorrectRoot(t *testing.T) {
	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := common.Address{1}

	var contractAddress common.Address
	eipContract := new(contracts.Testcontract)

	engine, db, blocks, receipts, clear, err := genBlocks(data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt().SetUint64(10)),
			fromKey,
		},
		1: {
			getBlockDeployTestContractTx(data.transactOpts[0], &contractAddress, eipContract),
			fromKey,
		},
		2: {
			getBlockTestContractTx(data.transactOpts[0], eipContract.Create, big.NewInt(2)),
			fromKey,
		},
		3: {
			getBlockTestContractTx(data.transactOpts[0], eipContract.Update, big.NewInt(0)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear()

	// BLOCK 1
	if _, err = stagedsync.InsertBlockInStages(db, data.genesisSpec.Config, &vm.Config{}, engine, blocks[0], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	st := state.New(state.NewDbStateReader(db))
	if !st.Exist(from) {
		t.Error("expected account to exist")
	}

	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist at the block 0", contractAddress.Hash().String())
	}

	// BLOCK 2
	if _, err = stagedsync.InsertBlockInStages(db, data.genesisSpec.Config, &vm.Config{}, engine, blocks[1], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	st = state.New(state.NewDbStateReader(db))
	if !st.Exist(from) {
		t.Error("expected account to exist")
	}

	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 2", contractAddress.Hash().String())
	}

	// BLOCK 3
	if _, err = stagedsync.InsertBlockInStages(db, data.genesisSpec.Config, &vm.Config{}, engine, blocks[2], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	// BLOCK 4 - INCORRECT
	incorrectHeader := blocks[3].Header()
	incorrectHeader.Root = blocks[1].Header().Root
	incorrectBlock := types.NewBlock(incorrectHeader, blocks[3].Transactions(), blocks[3].Uncles(), receipts[3])

	if _, err = stagedsync.InsertBlockInStages(db, data.genesisSpec.Config, &vm.Config{}, engine, incorrectBlock, true /* checkRoot */); err == nil {
		t.Fatal("should fail")
	}

	// BLOCK 4
	if _, err = stagedsync.InsertBlockInStages(db, data.genesisSpec.Config, &vm.Config{}, engine, blocks[3], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}
}

func TestAccountDeleteIncorrectRoot(t *testing.T) {
	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := common.Address{1}

	var contractAddress common.Address
	eipContract := new(contracts.Testcontract)

	engine, db, blocks, receipts, clear, err := genBlocks(data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt().SetUint64(10)),
			fromKey,
		},
		1: {
			getBlockDeployTestContractTx(data.transactOpts[0], &contractAddress, eipContract),
			fromKey,
		},
		2: {
			getBlockTestContractTx(data.transactOpts[0], eipContract.Create, big.NewInt(2)),
			fromKey,
		},
		3: {
			getBlockTestContractTx(data.transactOpts[0], eipContract.Remove),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear()

	// BLOCK 1
	if _, err = stagedsync.InsertBlockInStages(db, data.genesisSpec.Config, &vm.Config{}, engine, blocks[0], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	st := state.New(state.NewDbStateReader(db))
	if !st.Exist(from) {
		t.Error("expected account to exist")
	}

	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist at the block 0", contractAddress.Hash().String())
	}

	// BLOCK 2
	if _, err = stagedsync.InsertBlockInStages(db, data.genesisSpec.Config, &vm.Config{}, engine, blocks[1], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	st = state.New(state.NewDbStateReader(db))
	if !st.Exist(from) {
		t.Error("expected account to exist")
	}

	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 1", contractAddress.Hash().String())
	}

	// BLOCK 3
	if _, err = stagedsync.InsertBlockInStages(db, data.genesisSpec.Config, &vm.Config{}, engine, blocks[2], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	// BLOCK 4 - INCORRECT
	incorrectHeader := blocks[3].Header()
	incorrectHeader.Root = blocks[1].Header().Root
	incorrectBlock := types.NewBlock(incorrectHeader, blocks[3].Transactions(), blocks[3].Uncles(), receipts[3])

	if _, err = stagedsync.InsertBlockInStages(db, data.genesisSpec.Config, &vm.Config{}, engine, incorrectBlock, true /* checkRoot */); err == nil {
		t.Fatal("should fail")
	}

	// BLOCK 4
	if _, err = stagedsync.InsertBlockInStages(db, data.genesisSpec.Config, &vm.Config{}, engine, blocks[3], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}
}

type initialData struct {
	keys         []*ecdsa.PrivateKey
	addresses    []common.Address
	transactOpts []*bind.TransactOpts
	genesisSpec  *core.Genesis
}

func getGenesis(funds ...*big.Int) initialData {
	accountFunds := big.NewInt(1000000000)
	if len(funds) > 0 {
		accountFunds = funds[0]
	}

	keys := make([]*ecdsa.PrivateKey, 3)
	keys[0], _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	keys[1], _ = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
	keys[2], _ = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")

	addresses := make([]common.Address, 0, len(keys))
	transactOpts := make([]*bind.TransactOpts, 0, len(keys))
	allocs := core.GenesisAlloc{}
	for _, key := range keys {
		addr := crypto.PubkeyToAddress(key.PublicKey)
		addresses = append(addresses, addr)
		transactOpts = append(transactOpts, bind.NewKeyedTransactor(key))

		allocs[addr] = core.GenesisAccount{Balance: accountFunds}
	}

	return initialData{
		keys:         keys,
		addresses:    addresses,
		transactOpts: transactOpts,
		genesisSpec: &core.Genesis{
			Config: &params.ChainConfig{
				ChainID:             big.NewInt(1),
				HomesteadBlock:      new(big.Int),
				EIP150Block:         new(big.Int),
				EIP155Block:         new(big.Int),
				EIP158Block:         big.NewInt(1),
				ByzantiumBlock:      big.NewInt(1),
				ConstantinopleBlock: big.NewInt(1),
			},
			Alloc: allocs,
		},
	}
}

type tx struct {
	txFn blockTx
	key  *ecdsa.PrivateKey
}

func genBlocks(gspec *core.Genesis, txs map[int]tx) (consensus.Engine, *ethdb.ObjectDatabase, []*types.Block, []types.Receipts, func(), error) {
	engine := ethash.NewFaker()
	db := ethdb.NewMemDatabase()
	genesis := gspec.MustCommit(db)
	genesisDb := db.MemCopy()

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackend.Close()

	blocks, receipts, err := core.GenerateChain(gspec.Config, genesis, engine, genesisDb, len(txs), func(i int, block *core.BlockGen) {
		var tx *types.Transaction
		var isContractCall bool
		signer := types.HomesteadSigner{}

		if txToSend, ok := txs[i]; ok {
			tx, isContractCall = txToSend.txFn(block, contractBackend)
			var err error
			tx, err = types.SignTx(tx, signer, txToSend.key)
			if err != nil {
				return
			}
		}

		if tx != nil {
			if !isContractCall {
				err := contractBackend.SendTransaction(context.Background(), tx)
				if err != nil {
					return
				}
			}

			block.AddTx(tx)
		}

		contractBackend.Commit()
	}, false /* intermediateHashes */)
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("generate chain: %w", err)
	}

	clear := func() {
		db.Close()
		genesisDb.Close()
	}
	return engine, db, blocks, receipts, clear, err
}

type blockTx func(_ *core.BlockGen, backend bind.ContractBackend) (*types.Transaction, bool)

func getBlockTx(from common.Address, to common.Address, amount *uint256.Int) blockTx {
	return func(block *core.BlockGen, _ bind.ContractBackend) (*types.Transaction, bool) {
		return types.NewTransaction(block.TxNonce(from), to, amount, 21000, new(uint256.Int), nil), false
	}
}

func getBlockDeployTestContractTx(transactOpts *bind.TransactOpts, contractAddress *common.Address, eipContract *contracts.Testcontract) blockTx {
	return func(_ *core.BlockGen, backend bind.ContractBackend) (*types.Transaction, bool) {
		contractAddressRes, tx, eipContractRes, err := contracts.DeployTestcontract(transactOpts, backend)
		if err != nil {
			panic(err)
		}

		*contractAddress = contractAddressRes
		*eipContract = *eipContractRes

		return tx, true
	}
}

func getBlockTestContractTx(transactOpts *bind.TransactOpts, contractCall interface{}, newBalance ...*big.Int) blockTx {
	return func(_ *core.BlockGen, backend bind.ContractBackend) (*types.Transaction, bool) {
		var (
			tx  *types.Transaction
			err error
		)

		switch fn := contractCall.(type) {
		case func(opts *bind.TransactOpts) (*types.Transaction, error):
			tx, err = fn(transactOpts)
		case func(opts *bind.TransactOpts, newBalance *big.Int) (*types.Transaction, error):
			if len(newBalance) != 1 {
				panic("*big.Int type new balance is expected")
			}
			tx, err = fn(transactOpts, newBalance[0])
		default:
			panic("non expected function type")
		}

		if err != nil {
			panic(err)
		}

		return tx, true
	}
}
