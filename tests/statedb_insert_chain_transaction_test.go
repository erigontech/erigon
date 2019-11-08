package tests

import (
	"context"
	"fmt"
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
	// Configure and generate a sample block chain
	var (
		db       = ethdb.NewMemDatabase()
		key, _   = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		key1, _  = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
		key2, _  = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
		address  = crypto.PubkeyToAddress(key.PublicKey)
		address1 = crypto.PubkeyToAddress(key1.PublicKey)
		address2 = crypto.PubkeyToAddress(key2.PublicKey)
		theAddr  = common.Address{1}
		funds    = big.NewInt(1000000000)
		gspec    = &core.Genesis{
			Config: &params.ChainConfig{
				ChainID:             big.NewInt(1),
				HomesteadBlock:      new(big.Int),
				EIP155Block:         new(big.Int),
				EIP158Block:         big.NewInt(1),
				ConstantinopleBlock: big.NewInt(1),
			},
			Alloc: core.GenesisAlloc{
				address:  {Balance: funds},
				address1: {Balance: funds},
				address2: {Balance: funds},
			},
		}
		genesis   = gspec.MustCommit(db)
		genesisDb = db.MemCopy()
		// this code generates a log
		signer = types.HomesteadSigner{}
	)

	engine := ethash.NewFaker()
	blockchain, err := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}

	blockchain.EnableReceipts(true)

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)

	ctx := blockchain.WithContext(context.Background(), big.NewInt(genesis.Number().Int64()+1))
	blocks, receipts := core.GenerateChain(ctx, gspec.Config, genesis, engine, genesisDb, 2, func(i int, block *core.BlockGen) {
		var (
			tx  *types.Transaction
			err error
		)

		ctx = gspec.Config.WithEIPsFlags(ctx, block.Number())

		switch i {
		case 0:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, big.NewInt(1000), 21000, new(big.Int), nil), signer, key)
		case 1:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, big.NewInt(2000), 21000, new(big.Int), nil), signer, key)
		}

		if err != nil {
			t.Fatal(err)
		}

		if tx != nil {
			err = contractBackend.SendTransaction(ctx, tx)
			if err != nil {
				t.Fatal(err)
			}

			block.AddTx(tx)
		}

		contractBackend.Commit()
	})

	fmt.Printf("\n\n\n\n\n")

	// BLOCK 1
	incorrectHeader := blocks[0].Header()
	incorrectHeader.Root = blocks[1].Header().Root

	if blocks[0].Header().Root == incorrectHeader.Root {
		t.Fatal("roots are the same")
	}

	incorrectBlock := types.NewBlock(incorrectHeader, blocks[0].Transactions(), blocks[0].Uncles(), receipts[0])

	fmt.Println(blocks[0].Header().Number.Uint64(), blocks[0].Header().Root.String(), blocks[1].Header().Root.String())
	fmt.Println(incorrectBlock.Header().Number.Uint64(), incorrectBlock.Header().Root.String(), blocks[1].Header().Root.String())

	fmt.Println("111", blockchain.CurrentBlock().Number().String())

	if _, err := blockchain.InsertChain(types.Blocks{incorrectBlock}); err == nil {
		fmt.Println("111", blockchain.CurrentBlock().Number().String())
		t.Fatal("should fail")
	} else {
		fmt.Println("expected error", err)
	}

	//
	fmt.Printf("\n\n\n\n\n")
	engineNew := ethash.NewFaker()
	contractBackendNew := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	blocksNew, _ := core.GenerateChain(ctx, gspec.Config, genesis, engineNew, db.MemCopy(), 1, func(i int, block *core.BlockGen) {
		var (
			tx  *types.Transaction
			err error
		)

		ctx = gspec.Config.WithEIPsFlags(ctx, block.Number())

		switch i {
		case 0:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address1), theAddr, big.NewInt(5000), 21000, new(big.Int), nil), signer, key1)
		}

		if err != nil {
			t.Fatal(err)
		}

		if tx != nil {
			err = contractBackendNew.SendTransaction(ctx, tx)
			if err != nil {
				t.Fatal(err)
			}

			block.AddTx(tx)
		}

		contractBackendNew.Commit()
	})

	fmt.Printf("\n\n\n\n\n")

	if _, err := blockchain.InsertChain(blocksNew); err != nil {
		t.Fatal(err)
	}

	st, _, _ := blockchain.State()
	if !st.Exist(theAddr) {
		t.Error("expected account to exist")
	}

	if balance := st.GetBalance(theAddr); balance.Cmp(big.NewInt(5000)) != 0 {
		t.Fatalf("got %v, expected %v", balance, big.NewInt(5000))
	}
}

func TestInsertIncorrectStateRootSameAccount(t *testing.T) {
	// Configure and generate a sample block chain
	var (
		db       = ethdb.NewMemDatabase()
		key, _   = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		key1, _  = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
		key2, _  = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
		address  = crypto.PubkeyToAddress(key.PublicKey)
		address1 = crypto.PubkeyToAddress(key1.PublicKey)
		address2 = crypto.PubkeyToAddress(key2.PublicKey)
		theAddr  = common.Address{1}
		funds    = big.NewInt(1000000000)
		gspec    = &core.Genesis{
			Config: &params.ChainConfig{
				ChainID:             big.NewInt(1),
				HomesteadBlock:      new(big.Int),
				EIP155Block:         new(big.Int),
				EIP158Block:         big.NewInt(1),
				ConstantinopleBlock: big.NewInt(1),
			},
			Alloc: core.GenesisAlloc{
				address:  {Balance: funds},
				address1: {Balance: funds},
				address2: {Balance: funds},
			},
		}
		genesis   = gspec.MustCommit(db)
		genesisDb = db.MemCopy()
		// this code generates a log
		signer = types.HomesteadSigner{}
	)

	engine := ethash.NewFaker()
	blockchain, err := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}

	blockchain.EnableReceipts(true)

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)

	ctx := blockchain.WithContext(context.Background(), big.NewInt(genesis.Number().Int64()+1))
	blocks, receipts := core.GenerateChain(ctx, gspec.Config, genesis, engine, genesisDb, 2, func(i int, block *core.BlockGen) {
		var (
			tx  *types.Transaction
			err error
		)

		ctx = gspec.Config.WithEIPsFlags(ctx, block.Number())

		switch i {
		case 0:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, big.NewInt(1000), 21000, new(big.Int), nil), signer, key)
		case 1:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, big.NewInt(2000), 21000, new(big.Int), nil), signer, key)
		}

		if err != nil {
			t.Fatal(err)
		}

		if tx != nil {
			err = contractBackend.SendTransaction(ctx, tx)
			if err != nil {
				t.Fatal(err)
			}

			block.AddTx(tx)
		}

		contractBackend.Commit()
	})

	fmt.Printf("\n\n\n\n\n")

	// BLOCK 1
	incorrectHeader := blocks[0].Header()
	incorrectHeader.Root = blocks[1].Header().Root

	if blocks[0].Header().Root == incorrectHeader.Root {
		t.Fatal("roots are the same")
	}

	incorrectBlock := types.NewBlock(incorrectHeader, blocks[0].Transactions(), blocks[0].Uncles(), receipts[0])

	fmt.Println(blocks[0].Header().Number.Uint64(), blocks[0].Header().Root.String(), blocks[1].Header().Root.String())
	fmt.Println(incorrectBlock.Header().Number.Uint64(), incorrectBlock.Header().Root.String(), blocks[1].Header().Root.String())

	fmt.Println("111", blockchain.CurrentBlock().Number().String())

	if _, err := blockchain.InsertChain(types.Blocks{incorrectBlock}); err == nil {
		fmt.Println("111", blockchain.CurrentBlock().Number().String())
		t.Fatal("should fail")
	} else {
		fmt.Println("expected error", err)
	}

	//
	fmt.Printf("\n\n\n\n\n")
	engineNew := ethash.NewFaker()
	contractBackendNew := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	blocksNew, _ := core.GenerateChain(ctx, gspec.Config, genesis, engineNew, db.MemCopy(), 1, func(i int, block *core.BlockGen) {
		var (
			tx  *types.Transaction
			err error
		)

		ctx = gspec.Config.WithEIPsFlags(ctx, block.Number())

		switch i {
		case 0:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, big.NewInt(5000), 21000, new(big.Int), nil), signer, key)
		}

		if err != nil {
			t.Fatal(err)
		}

		if tx != nil {
			err = contractBackendNew.SendTransaction(ctx, tx)
			if err != nil {
				t.Fatal(err)
			}

			block.AddTx(tx)
		}

		contractBackendNew.Commit()
	})

	fmt.Printf("\n\n\n\n\n")

	if _, err := blockchain.InsertChain(blocksNew); err != nil {
		t.Fatal(err)
	}

	st, _, _ := blockchain.State()
	if !st.Exist(theAddr) {
		t.Error("expected account to exist")
	}

	if balance := st.GetBalance(theAddr); balance.Cmp(big.NewInt(5000)) != 0 {
		t.Fatalf("got %v, expected %v", balance, big.NewInt(5000))
	}
}

func TestInsertIncorrectStateRootSameAccountSameAmount(t *testing.T) {
	// Configure and generate a sample block chain
	var (
		db       = ethdb.NewMemDatabase()
		key, _   = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		key1, _  = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
		key2, _  = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
		address  = crypto.PubkeyToAddress(key.PublicKey)
		address1 = crypto.PubkeyToAddress(key1.PublicKey)
		address2 = crypto.PubkeyToAddress(key2.PublicKey)
		theAddr  = common.Address{1}
		funds    = big.NewInt(1000000000)
		gspec    = &core.Genesis{
			Config: &params.ChainConfig{
				ChainID:             big.NewInt(1),
				HomesteadBlock:      new(big.Int),
				EIP155Block:         new(big.Int),
				EIP158Block:         big.NewInt(1),
				ConstantinopleBlock: big.NewInt(1),
			},
			Alloc: core.GenesisAlloc{
				address:  {Balance: funds},
				address1: {Balance: funds},
				address2: {Balance: funds},
			},
		}
		genesis   = gspec.MustCommit(db)
		genesisDb = db.MemCopy()
		// this code generates a log
		signer = types.HomesteadSigner{}
	)

	engine := ethash.NewFaker()
	blockchain, err := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}

	blockchain.EnableReceipts(true)

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)

	ctx := blockchain.WithContext(context.Background(), big.NewInt(genesis.Number().Int64()+1))
	blocks, receipts := core.GenerateChain(ctx, gspec.Config, genesis, engine, genesisDb, 2, func(i int, block *core.BlockGen) {
		var (
			tx  *types.Transaction
			err error
		)

		ctx = gspec.Config.WithEIPsFlags(ctx, block.Number())

		switch i {
		case 0:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, big.NewInt(1000), 21000, new(big.Int), nil), signer, key)
		case 1:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, big.NewInt(2000), 21000, new(big.Int), nil), signer, key)
		}

		if err != nil {
			t.Fatal(err)
		}

		if tx != nil {
			err = contractBackend.SendTransaction(ctx, tx)
			if err != nil {
				t.Fatal(err)
			}

			block.AddTx(tx)
		}

		contractBackend.Commit()
	})

	fmt.Printf("\n\n\n\n\n")

	// BLOCK 1
	incorrectHeader := blocks[0].Header()
	incorrectHeader.Root = blocks[1].Header().Root

	if blocks[0].Header().Root == incorrectHeader.Root {
		t.Fatal("roots are the same")
	}

	incorrectBlock := types.NewBlock(incorrectHeader, blocks[0].Transactions(), blocks[0].Uncles(), receipts[0])

	fmt.Println(blocks[0].Header().Number.Uint64(), blocks[0].Header().Root.String(), blocks[1].Header().Root.String())
	fmt.Println(incorrectBlock.Header().Number.Uint64(), incorrectBlock.Header().Root.String(), blocks[1].Header().Root.String())

	fmt.Println("111", blockchain.CurrentBlock().Number().String())

	if _, err := blockchain.InsertChain(types.Blocks{incorrectBlock}); err == nil {
		fmt.Println("111", blockchain.CurrentBlock().Number().String())
		t.Fatal("should fail")
	} else {
		fmt.Println("expected error", err)
	}

	//
	fmt.Printf("\n\n\n\n\n")
	engineNew := ethash.NewFaker()
	contractBackendNew := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	blocksNew, _ := core.GenerateChain(ctx, gspec.Config, genesis, engineNew, db.MemCopy(), 1, func(i int, block *core.BlockGen) {
		var (
			tx  *types.Transaction
			err error
		)

		ctx = gspec.Config.WithEIPsFlags(ctx, block.Number())

		switch i {
		case 0:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, big.NewInt(1000), 21000, new(big.Int), nil), signer, key)
		}

		if err != nil {
			t.Fatal(err)
		}

		if tx != nil {
			err = contractBackendNew.SendTransaction(ctx, tx)
			if err != nil {
				t.Fatal(err)
			}

			block.AddTx(tx)
		}

		contractBackendNew.Commit()
	})

	fmt.Printf("\n\n\n\n\n")

	if _, err := blockchain.InsertChain(blocksNew); err != nil {
		t.Fatal(err)
	}

	st, _, _ := blockchain.State()
	if !st.Exist(theAddr) {
		t.Error("expected account to exist")
	}

	if balance := st.GetBalance(theAddr); balance.Cmp(big.NewInt(1000)) != 0 {
		t.Fatalf("got %v, expected %v", balance, big.NewInt(1000))
	}
}
