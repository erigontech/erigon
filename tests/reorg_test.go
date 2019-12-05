package tests

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/tests/contracts"
	"math/big"
	"testing"
)

func TestStorageReorg(t *testing.T) {
	t.Skip()
	// Configure and generate a sample block chain
	var (
		db       = ethdb.NewMemDatabase()
		key, _   = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		key1, _  = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
		key2, _  = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
		address  = crypto.PubkeyToAddress(key.PublicKey)
		address1 = crypto.PubkeyToAddress(key1.PublicKey)
		address2 = crypto.PubkeyToAddress(key2.PublicKey)
		funds    = big.NewInt(1000000000)
		gspec    = &core.Genesis{
			Config: &params.ChainConfig{
				ChainID:             big.NewInt(1),
				HomesteadBlock:      new(big.Int),
				EIP155Block:         new(big.Int),
				EIP158Block:         big.NewInt(1),
				EIP2027Block:        big.NewInt(4),
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
	)

	engine := ethash.NewFaker()
	blockchain, err := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}

	blockchain.EnableReceipts(true)

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOpts := bind.NewKeyedTransactor(key)
	transactOpts1 := bind.NewKeyedTransactor(key1)
	transactOpts2 := bind.NewKeyedTransactor(key2)

	var eipContract *contracts.Eip2027
	blockNum := 6
	ctx := blockchain.WithContext(context.Background(), big.NewInt(int64(genesis.NumberU64())+1))
	f:=func(offset int64) func(i int, block *core.BlockGen){
		return func(i int, block *core.BlockGen) {
			//Some manipulations with contract to generate blocks with state history
			var (
				tx       *types.Transaction
				innerErr error
			)

			switch i {
			case 0:
				_, tx, eipContract, innerErr = contracts.DeployEip2027(transactOpts, contractBackend)
				assertNil(t, innerErr)
				block.AddTx(tx)

			case 1:
				tx, innerErr = eipContract.Create(transactOpts1, big.NewInt(1))
				assertNil(t, innerErr)
				block.AddTx(tx)

				tx, innerErr = eipContract.Create(transactOpts2, big.NewInt(2))
				assertNil(t, innerErr)
				block.AddTx(tx)

				tx, innerErr = eipContract.Create(transactOpts, big.NewInt(3))
				assertNil(t, innerErr)
				block.AddTx(tx)
			case 2:
				tx, innerErr = eipContract.Update(transactOpts1, big.NewInt(0))
				assertNil(t, innerErr)
				block.AddTx(tx)

				tx, innerErr = eipContract.Update(transactOpts2, big.NewInt(0))
				assertNil(t, innerErr)
				block.AddTx(tx)

				tx, innerErr = eipContract.Update(transactOpts, big.NewInt(0))
				assertNil(t, innerErr)
				block.AddTx(tx)

			case 3:
				tx, innerErr = eipContract.Update(transactOpts1, big.NewInt(7))
				assertNil(t, innerErr)
				block.AddTx(tx)

				tx, innerErr = eipContract.Update(transactOpts2, big.NewInt(7))
				assertNil(t, innerErr)
				block.AddTx(tx)

				tx, innerErr = eipContract.Update(transactOpts, big.NewInt(7))
				assertNil(t, innerErr)
				block.AddTx(tx)

			case 4:
				tx, innerErr = eipContract.Update(transactOpts1, big.NewInt(5))
				assertNil(t, innerErr)
				block.AddTx(tx)

				tx, innerErr = eipContract.Update(transactOpts2, big.NewInt(5))
				assertNil(t, innerErr)
				block.AddTx(tx)

				tx, innerErr = eipContract.Update(transactOpts, big.NewInt(5))
				assertNil(t, innerErr)
				block.AddTx(tx)

			case 5:
				tx, innerErr = eipContract.Remove(transactOpts1)
				assertNil(t, innerErr)
				block.AddTx(tx)

				tx, innerErr = eipContract.Remove(transactOpts2)
				assertNil(t, innerErr)
				block.AddTx(tx)

				tx, innerErr = eipContract.Remove(transactOpts)
				assertNil(t, innerErr)
				block.AddTx(tx)

			}

			if err != nil {
				t.Fatal(innerErr)
			}
			block.OffsetTime(offset)
			//contractBackend.Commit()
		}
	}

	f2:=func(offset int64) func(i int, block *core.BlockGen){
		return func(i int, block *core.BlockGen) {
			fmt.Println("Block", i)
			//Some manipulations with contract to generate blocks with state history
			var (
				tx       *types.Transaction
				innerErr error
			)

			switch i {
			case 0:
				tx, innerErr = eipContract.Update(transactOpts1, big.NewInt(7))
				assertNil(t, innerErr)
				block.AddTx(tx)

				tx, innerErr = eipContract.Update(transactOpts2, big.NewInt(7))
				assertNil(t, innerErr)
				block.AddTx(tx)

				tx, innerErr = eipContract.Update(transactOpts, big.NewInt(7))
				assertNil(t, innerErr)
				block.AddTx(tx)

			case 1:
				tx, innerErr = eipContract.Remove(transactOpts1)
				assertNil(t, innerErr)
				block.AddTx(tx)

				tx, innerErr = eipContract.Remove(transactOpts2)
				assertNil(t, innerErr)
				block.AddTx(tx)

				tx, innerErr = eipContract.Remove(transactOpts)
				assertNil(t, innerErr)
				block.AddTx(tx)

			}

			if err != nil {
				t.Fatal(innerErr)
			}
			block.OffsetTime(offset)
			//contractBackend.Commit()
		}
	}
	db1:=genesisDb.MemCopy()
	blocks, _ := core.GenerateChain(ctx, gspec.Config, genesis, engine, db1, blockNum, f(2))
	blocks2, _ := core.GenerateChain(ctx, gspec.Config, blocks[4], engine, db1.MemCopy(), 2, f2(5))

	_, err = blockchain.InsertChain(blocks)
	if err != nil {
		t.Fatal(err)
	}

	res, err := getStat(db)
	assertNil(t, err)
	spew.Dump(res)


	_, err = blockchain.InsertChain(blocks2)
	if err != nil {
		t.Fatal(err)
	}

	res, err = getStat(db)
	assertNil(t, err)
	spew.Dump(res)

	//
	//res, err := getStat(db)
	//assertNil(t, err)
	//
	//expected := stateStats{
	//	NotFoundAccountsInHistory:     5,
	//	ErrAccountsInHistory:          0,
	//	ErrDecodedAccountsInHistory:   0,
	//	NumOfChangesInAccountsHistory: 26,
	//	AccountSuffixRecordsByTimestamp: map[uint64]uint32{
	//		0: 3,
	//		1: 3,
	//		2: 5,
	//		3: 5,
	//		4: 5,
	//		5: 5,
	//		6: 5,
	//	},
	//	StorageSuffixRecordsByTimestamp: map[uint64]uint32{
	//		1: 1,
	//		2: 3,
	//		3: 3,
	//		4: 3,
	//		5: 3,
	//		6: 3,
	//	},
	//	AccountsInState: 5,
	//}
	//if !reflect.DeepEqual(expected, res) {
	//	spew.Dump(getStat(db))
	//	t.Fatal("not equals")
	//}
	//
	//err = core.Prune(db, 0, uint64(blockNum-1))
	//assertNil(t, err)
	//res, err = getStat(db)
	//assertNil(t, err)
	//
	//expected = stateStats{
	//	NotFoundAccountsInHistory:     0,
	//	ErrAccountsInHistory:          0,
	//	ErrDecodedAccountsInHistory:   0,
	//	NumOfChangesInAccountsHistory: 5,
	//	AccountSuffixRecordsByTimestamp: map[uint64]uint32{
	//		6: 5,
	//	},
	//	StorageSuffixRecordsByTimestamp: map[uint64]uint32{
	//		6: 3,
	//	},
	//	AccountsInState: 5,
	//}
	//
	//if !reflect.DeepEqual(expected, res) {
	//	spew.Dump(getStat(db))
	//	t.Fatal("not equals")
	//}

}

func TestAccountReorg(t *testing.T) {
	t.Skip()
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
		signer = types.HomesteadSigner{}

		funds    = big.NewInt(1000000000)
		gspec    = &core.Genesis{
			Config: &params.ChainConfig{
				ChainID:             big.NewInt(1),
				HomesteadBlock:      new(big.Int),
				EIP155Block:         new(big.Int),
				EIP158Block:         big.NewInt(1),
				EIP2027Block:        big.NewInt(4),
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
	)

	engine := ethash.NewFaker()
	blockchain, err := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}

	blockchain.EnableReceipts(true)


	blockNum := 6
	ctx := blockchain.WithContext(context.Background(), big.NewInt(int64(genesis.NumberU64())+1))
	f:=func(offset []int64) func(i int, block *core.BlockGen){
		return func(i int, block *core.BlockGen) {
			//Some transactions to generate blocks and history
			var (
				tx     *types.Transaction
				genErr error
			)
			var addr common.Address
			var k *ecdsa.PrivateKey
			switch i % 3 {
			case 0:
				addr = address
				k = key
			case 1:
				addr = address1
				k = key1
			case 2:
				addr = address2
				k = key2
			}
			tx, genErr = types.SignTx(types.NewTransaction(block.TxNonce(addr), theAddr, big.NewInt(1000), 21000, new(big.Int), nil), signer, k)
			if genErr != nil {
				t.Fatal(genErr)
			}

			block.AddTx(tx)
		}
	}
	return
	var first, second []int64
	blocks, _ := core.GenerateChain(ctx, gspec.Config, genesis, engine, genesisDb.MemCopy(), blockNum, f(first))
	blocks2, _ := core.GenerateChain(ctx, gspec.Config, genesis, engine, genesisDb.MemCopy(), blockNum-1, f(second))

	_, err = blockchain.InsertChain(blocks)
	if err != nil {
		t.Fatal(err)
	}

	res, err := getStat(db)
	assertNil(t, err)
	spew.Dump(res)

	t.Log(blockchain.CurrentBlock().Number())


	_, err = blockchain.InsertChain(blocks2)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(blockchain.CurrentBlock().Number())

	res, err = getStat(db)
	assertNil(t, err)
	spew.Dump(res)
}