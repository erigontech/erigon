package tests

import (
	"context"
	"crypto/ecdsa"
	"math/big"
	"testing"

	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

func TestInsertIncorrectStateRootDifferentAccounts(t *testing.T) {
	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := common.Address{1}

	blockchain, blocks, receipts, err := genBlocks(data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, big.NewInt(1000), 21000, new(big.Int), nil),
			fromKey,
		},
		1: {
			getBlockTx(from, to, big.NewInt(2000), 21000, new(big.Int), nil),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// BLOCK 1
	incorrectHeader := blocks[0].Header()
	incorrectHeader.Root = blocks[1].Header().Root

	if blocks[0].Header().Root == incorrectHeader.Root {
		t.Fatal("roots are the same")
	}

	incorrectBlock := types.NewBlock(incorrectHeader, blocks[0].Transactions(), blocks[0].Uncles(), receipts[0])
	if _, err := blockchain.InsertChain(types.Blocks{incorrectBlock}); err == nil {
		t.Fatal("should fail")
	}

	// insert a correct block
	blockchain, blocks, receipts, err = genBlocks(data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(data.addresses[1], to, big.NewInt(5000), 21000, new(big.Int), nil),
			data.keys[1],
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := blockchain.InsertChain(blocks); err != nil {
		t.Fatal(err)
	}

	st, _, _ := blockchain.State()
	if !st.Exist(to) {
		t.Error("expected account to exist")
	}

	if balance := st.GetBalance(from); balance.Cmp(big.NewInt(1000000000)) != 0 {
		t.Fatalf("got %v, expected %v", balance, big.NewInt(1000000000))
	}
	if balance := st.GetBalance(to); balance.Cmp(big.NewInt(5000)) != 0 {
		t.Fatalf("got %v, expected %v", balance, big.NewInt(5000))
	}
}

func TestInsertIncorrectStateRootSameAccount(t *testing.T) {
	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := common.Address{1}

	blockchain, blocks, receipts, err := genBlocks(data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, big.NewInt(1000), 21000, new(big.Int), nil),
			fromKey,
		},
		1: {
			getBlockTx(from, to, big.NewInt(2000), 21000, new(big.Int), nil),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// BLOCK 1
	incorrectHeader := blocks[0].Header()
	incorrectHeader.Root = blocks[1].Header().Root

	if blocks[0].Header().Root == incorrectHeader.Root {
		t.Fatal("roots are the same")
	}

	incorrectBlock := types.NewBlock(incorrectHeader, blocks[0].Transactions(), blocks[0].Uncles(), receipts[0])
	if _, err := blockchain.InsertChain(types.Blocks{incorrectBlock}); err == nil {
		t.Fatal("should fail")
	}

	// insert a correct block
	blockchain, blocks, receipts, err = genBlocks(data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, big.NewInt(5000), 21000, new(big.Int), nil),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := blockchain.InsertChain(blocks); err != nil {
		t.Fatal(err)
	}

	st, _, _ := blockchain.State()
	if !st.Exist(to) {
		t.Error("expected account to exist")
	}

	if balance := st.GetBalance(from); balance.Cmp(big.NewInt(999995000)) != 0 {
		t.Fatalf("got %v, expected %v", balance, big.NewInt(999995000))
	}
	if balance := st.GetBalance(to); balance.Cmp(big.NewInt(5000)) != 0 {
		t.Fatalf("got %v, expected %v", balance, big.NewInt(5000))
	}
}

func TestInsertIncorrectStateRootSameAccountSameAmount(t *testing.T) {
	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := common.Address{1}

	blockchain, blocks, receipts, err := genBlocks(data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, big.NewInt(1000), 21000, new(big.Int), nil),
			fromKey,
		},
		1: {
			getBlockTx(from, to, big.NewInt(2000), 21000, new(big.Int), nil),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// BLOCK 1
	incorrectHeader := blocks[0].Header()
	incorrectHeader.Root = blocks[1].Header().Root

	incorrectBlock := types.NewBlock(incorrectHeader, blocks[0].Transactions(), blocks[0].Uncles(), receipts[0])

	if _, err := blockchain.InsertChain(types.Blocks{incorrectBlock}); err == nil {
		t.Fatal("should fail")
	}

	// insert a correct block
	blockchain, blocks, receipts, err = genBlocks(data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, big.NewInt(1000), 21000, new(big.Int), nil),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := blockchain.InsertChain(blocks); err != nil {
		t.Fatal(err)
	}

	st, _, _ := blockchain.State()
	if !st.Exist(to) {
		t.Error("expected account to exist")
	}

	if balance := st.GetBalance(from); balance.Cmp(big.NewInt(999999000)) != 0 {
		t.Fatalf("got %v, expected %v", balance, big.NewInt(999999000))
	}
	if balance := st.GetBalance(to); balance.Cmp(big.NewInt(1000)) != 0 {
		t.Fatalf("got %v, expected %v", balance, big.NewInt(1000))
	}
}

func TestInsertIncorrectStateRootAllFundsRoot(t *testing.T) {
	data := getGenesis(big.NewInt(3000))
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := common.Address{1}

	blockchain, blocks, receipts, err := genBlocks(data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, big.NewInt(1000), 21000, new(big.Int), nil),
			fromKey,
		},
		1: {
			getBlockTx(from, to, big.NewInt(2000), 21000, new(big.Int), nil),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// BLOCK 1
	incorrectHeader := blocks[0].Header()
	incorrectHeader.Root = blocks[1].Header().Root

	incorrectBlock := types.NewBlock(incorrectHeader, blocks[0].Transactions(), blocks[0].Uncles(), receipts[0])

	if _, err := blockchain.InsertChain(types.Blocks{incorrectBlock}); err == nil {
		t.Fatal("should fail")
	}

	// insert a correct block
	blockchain, blocks, receipts, err = genBlocks(data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, big.NewInt(1000), 21000, new(big.Int), nil),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := blockchain.InsertChain(blocks); err != nil {
		t.Fatal(err)
	}

	st, _, _ := blockchain.State()
	if !st.Exist(to) {
		t.Error("expected account to exist")
	}

	if balance := st.GetBalance(from); balance.Cmp(big.NewInt(2000)) != 0 {
		t.Fatalf("got %v, expected %v", balance, big.NewInt(2000))
	}
	if balance := st.GetBalance(to); balance.Cmp(big.NewInt(1000)) != 0 {
		t.Fatalf("got %v, expected %v", balance, big.NewInt(1000))
	}
}

func TestInsertIncorrectStateRootAllFunds(t *testing.T) {
	data := getGenesis(big.NewInt(3000))
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := common.Address{1}

	blockchain, blocks, receipts, err := genBlocks(data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, big.NewInt(3000), 21000, new(big.Int), nil),
			fromKey,
		},
		1: {
			getBlockTx(data.addresses[1], to, big.NewInt(2000), 21000, new(big.Int), nil),
			data.keys[1],
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// BLOCK 1
	incorrectHeader := blocks[0].Header()
	incorrectHeader.Root = blocks[1].Header().Root

	incorrectBlock := types.NewBlock(incorrectHeader, blocks[0].Transactions(), blocks[0].Uncles(), receipts[0])

	if _, err := blockchain.InsertChain(types.Blocks{incorrectBlock}); err == nil {
		t.Fatal("should fail")
	}

	// insert a correct block
	blockchain, blocks, receipts, err = genBlocks(data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, big.NewInt(1000), 21000, new(big.Int), nil),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := blockchain.InsertChain(blocks); err != nil {
		t.Fatal(err)
	}

	st, _, _ := blockchain.State()
	if !st.Exist(to) {
		t.Error("expected account to exist")
	}

	if balance := st.GetBalance(from); balance.Cmp(big.NewInt(2000)) != 0 {
		t.Fatalf("got %v, expected %v", balance, big.NewInt(2000))
	}
	if balance := st.GetBalance(to); balance.Cmp(big.NewInt(1000)) != 0 {
		t.Fatalf("got %v, expected %v", balance, big.NewInt(1000))
	}
}

type initialData struct {
	keys        []*ecdsa.PrivateKey
	addresses   []common.Address
	genesisSpec *core.Genesis
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

	var addresses []common.Address
	allocs := core.GenesisAlloc{}
	for _, key := range keys {
		addr := crypto.PubkeyToAddress(key.PublicKey)
		addresses = append(addresses, addr)

		allocs[addr] = core.GenesisAccount{Balance: accountFunds}
	}

	return initialData{
		keys:      keys,
		addresses: addresses,
		genesisSpec: &core.Genesis{
			Config: &params.ChainConfig{
				ChainID:             big.NewInt(1),
				HomesteadBlock:      new(big.Int),
				EIP155Block:         new(big.Int),
				EIP158Block:         big.NewInt(1),
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

func genBlocks(gspec *core.Genesis, txs map[int]tx) (*core.BlockChain, []*types.Block, []types.Receipts, error) {
	engine := ethash.NewFaker()
	db := ethdb.NewMemDatabase()
	genesis := gspec.MustCommit(db)
	genesisDb := db.MemCopy()

	blockchain, err := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil)
	if err != nil {
		return nil, nil, nil, err
	}
	blockchain.EnableReceipts(true)

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	ctx := blockchain.WithContext(context.Background(), big.NewInt(genesis.Number().Int64()+1))

	blocks, receipts := core.GenerateChain(ctx, gspec.Config, genesis, engine, genesisDb, 2, func(i int, block *core.BlockGen) {
		var tx *types.Transaction
		signer := types.HomesteadSigner{}
		ctx = gspec.Config.WithEIPsFlags(ctx, block.Number())

		if txToSend, ok := txs[i]; ok {
			tx, err = types.SignTx(txToSend.txFn(block), signer, txToSend.key)
			if err != nil {
				return
			}
		}

		if tx != nil {
			err = contractBackend.SendTransaction(ctx, tx)
			if err != nil {
				return
			}

			block.AddTx(tx)
		}

		contractBackend.Commit()
	})

	return blockchain, blocks, receipts, err
}

type blockTx func(block *core.BlockGen) *types.Transaction

func getBlockTx(from common.Address, to common.Address, amount *big.Int, gasLimit uint64, gasPrice *big.Int, data []byte) blockTx {
	return func(block *core.BlockGen) *types.Transaction {
		return types.NewTransaction(block.TxNonce(from), to, amount, gasLimit, gasPrice, data)
	}
}
