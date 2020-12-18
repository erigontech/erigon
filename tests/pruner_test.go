package tests

import (
	"context"
	"crypto/ecdsa"
	"math/big"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/holiman/uint256"

	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/tests/contracts"
)

// It generates several blocks with money transfer, checks that it's correct
// than prune two times with database state and history checks
func TestBasisAccountPruning(t *testing.T) {
	// TODO: recover
	t.Skip()

	// Configure and generate a sample block chain
	db := ethdb.NewMemDatabase()
	defer db.Close()
	var (
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
				EIP150Block:         new(big.Int),
				EIP155Block:         new(big.Int),
				EIP158Block:         big.NewInt(1),
				ByzantiumBlock:      big.NewInt(1),
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
	defer genesisDb.Close()

	numBlocks := 10
	engine := ethash.NewFaker()
	txCacher := core.NewTxSenderCacher(runtime.NumCPU())
	blockchain, err := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil, txCacher)
	if err != nil {
		t.Fatal(err)
	}

	blocks, _, err := core.GenerateChain(gspec.Config, genesis, engine, genesisDb, numBlocks, func(i int, block *core.BlockGen) {
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
		tx, genErr = types.SignTx(types.NewTransaction(block.TxNonce(addr), theAddr, uint256.NewInt().SetUint64(1000), 21000, new(uint256.Int), nil), signer, k)
		if genErr != nil {
			t.Fatal(genErr)
		}
		block.AddTx(tx)
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	// Insert blocks
	_, err = blockchain.InsertChain(context.Background(), blocks)
	if err != nil {
		t.Fatal(err)
	}

	// Get stats
	res, err := getStat(db)
	if err != nil {
		t.Fatal(err)
	}
	expected := stateStats{
		NumOfChangesInAccountsHistory: 33,
		ErrAccountsInHistory:          0,
		AccountSuffixRecordsByTimestamp: map[uint64]uint32{
			0:  3,
			1:  3,
			2:  3,
			3:  3,
			4:  3,
			5:  3,
			6:  3,
			7:  3,
			8:  3,
			9:  3,
			10: 3,
		},
		StorageSuffixRecordsByTimestamp: map[uint64]uint32{},
		AccountsInState:                 5,
	}
	if !reflect.DeepEqual(expected, res) {
		spew.Dump(res)
		t.Fatal("Not equal")
	}

	//Prune database history up to HEAD-1
	err = core.Prune(db, 0, uint64(numBlocks)-1)
	if err != nil {
		t.Fatal(err)
	}

	//Check stats after history
	res, err = getStat(db)
	if err != nil {
		t.Fatal(err)
	}
	expected = stateStats{
		ErrAccountsInHistory:          0,
		NumOfChangesInAccountsHistory: 3,
		AccountSuffixRecordsByTimestamp: map[uint64]uint32{
			10: 3,
		},
		StorageSuffixRecordsByTimestamp: map[uint64]uint32{},
		AccountsInState:                 5,
	}
	if !reflect.DeepEqual(expected, res) {
		spew.Dump(res)
		t.Fatal("Not equal")
	}

	//Prune database history up to HEAD
	err = core.Prune(db, uint64(numBlocks)-1, uint64(numBlocks))
	if err != nil {
		t.Fatal(err)
	}
	res, err = getStat(db)
	if err != nil {
		t.Fatal(err)
	}
	expected = stateStats{
		NumOfChangesInAccountsHistory:   0,
		ErrAccountsInHistory:            0,
		AccountSuffixRecordsByTimestamp: map[uint64]uint32{},
		StorageSuffixRecordsByTimestamp: map[uint64]uint32{},
		AccountsInState:                 5,
	}
	if !reflect.DeepEqual(expected, res) {
		spew.Dump(res)
		t.Fatal("Not equal")
	}

}

// It generates several blocks with money transfer, with noHistory flag enabled, checks that history not saved, but changeset exesists for every block
// than prune two times with database state and history checks
func TestBasisAccountPruningNoHistory(t *testing.T) {
	// TODO: recover
	t.Skip()

	// Configure and generate a sample block chain
	db := ethdb.NewMemDatabase()
	defer db.Close()
	var (
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
				EIP150Block:         new(big.Int),
				EIP155Block:         new(big.Int),
				EIP158Block:         big.NewInt(1),
				ByzantiumBlock:      big.NewInt(1),
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
	defer genesisDb.Close()

	numBlocks := 10
	engine := ethash.NewFaker()
	cacheConfig := core.CacheConfig{
		Pruning:             false,
		NoHistory:           true,
		ArchiveSyncInterval: 1,
	}
	txCacher := core.NewTxSenderCacher(runtime.NumCPU())
	blockchain, err := core.NewBlockChain(db, &cacheConfig, gspec.Config, engine, vm.Config{}, nil, txCacher)
	if err != nil {
		t.Fatal(err)
	}
	defer blockchain.Stop()

	blocks, _, err := core.GenerateChain(gspec.Config, genesis, engine, genesisDb, numBlocks, func(i int, block *core.BlockGen) {
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
		tx, genErr = types.SignTx(types.NewTransaction(block.TxNonce(addr), theAddr, uint256.NewInt().SetUint64(1000), 21000, new(uint256.Int), nil), signer, k)
		if genErr != nil {
			t.Fatal(genErr)
		}
		block.AddTx(tx)
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	_, err = blockchain.InsertChain(context.Background(), blocks)
	if err != nil {
		t.Fatal(err)
	}

	res, err := getStat(db)
	if err != nil {
		t.Fatal(err)
	}
	expected := stateStats{
		ErrAccountsInHistory:          30, //not in history
		NumOfChangesInAccountsHistory: 3,  // exists in history
		AccountSuffixRecordsByTimestamp: map[uint64]uint32{
			0:  3,
			1:  3,
			2:  3,
			3:  3,
			4:  3,
			5:  3,
			6:  3,
			7:  3,
			8:  3,
			9:  3,
			10: 3,
		},
		StorageSuffixRecordsByTimestamp: map[uint64]uint32{},
		AccountsInState:                 5,
	}
	if !reflect.DeepEqual(expected, res) {
		spew.Dump(res)
		t.Fatal("Not equal")
	}

	err = core.Prune(db, 0, uint64(numBlocks)-1)
	if err != nil {
		t.Fatal(err)
	}

	res, err = getStat(db)
	if err != nil {
		t.Fatal(err)
	}
	expected = stateStats{
		ErrAccountsInHistory:          3, //not in history after prune
		NumOfChangesInAccountsHistory: 0,
		AccountSuffixRecordsByTimestamp: map[uint64]uint32{
			10: 3,
		},
		StorageSuffixRecordsByTimestamp: map[uint64]uint32{},
		AccountsInState:                 5,
	}
	if !reflect.DeepEqual(expected, res) {
		spew.Dump(res)
		t.Fatal("Not equal")
	}

	//Prune database history up to HEAD
	err = core.Prune(db, uint64(numBlocks)-1, uint64(numBlocks))
	if err != nil {
		t.Fatal(err)
	}
	res, err = getStat(db)
	if err != nil {
		t.Fatal(err)
	}
	expected = stateStats{
		ErrAccountsInHistory:            0,
		NumOfChangesInAccountsHistory:   0,
		AccountSuffixRecordsByTimestamp: map[uint64]uint32{},
		StorageSuffixRecordsByTimestamp: map[uint64]uint32{},
		AccountsInState:                 5,
	}
	if !reflect.DeepEqual(expected, res) {
		spew.Dump(res)
		t.Fatal("Not equal")
	}

}

// It deploys simple contract and makes several state changes, checks that state and history is correct,
// than prune to numBlock-1 with database state and history checks
func TestStoragePruning(t *testing.T) {
	// TODO: recover
	t.Skip()

	// Configure and generate a sample block chain
	db := ethdb.NewMemDatabase()
	defer db.Close()
	var (
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
				EIP150Block:         new(big.Int),
				EIP155Block:         new(big.Int),
				EIP158Block:         big.NewInt(1),
				ByzantiumBlock:      big.NewInt(1),
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
	defer genesisDb.Close()

	engine := ethash.NewFaker()
	txCacher := core.NewTxSenderCacher(runtime.NumCPU())
	blockchain, err := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil, txCacher)
	if err != nil {
		t.Fatal(err)
	}
	defer blockchain.Stop()

	blockchain.EnableReceipts(true)

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackend.Close()
	transactOpts := bind.NewKeyedTransactor(key)
	transactOpts1 := bind.NewKeyedTransactor(key1)
	transactOpts2 := bind.NewKeyedTransactor(key2)

	var eipContract *contracts.Testcontract
	blockNum := 6
	blocks, _, err := core.GenerateChain(gspec.Config, genesis, engine, genesisDb, blockNum, func(i int, block *core.BlockGen) {
		//Some manipulations with contract to generate blocks with state history
		var (
			tx       *types.Transaction
			innerErr error
		)

		switch i {
		case 0:
			_, tx, eipContract, innerErr = contracts.DeployTestcontract(transactOpts, contractBackend)
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

		contractBackend.Commit()
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	_, err = blockchain.InsertChain(context.Background(), blocks)
	if err != nil {
		t.Fatal(err)
	}

	res, err := getStat(db)
	assertNil(t, err)

	expected := stateStats{
		ErrAccountsInHistory:          0,
		NumOfChangesInAccountsHistory: 31,
		AccountSuffixRecordsByTimestamp: map[uint64]uint32{
			0: 3,
			1: 3,
			2: 5,
			3: 5,
			4: 5,
			5: 5,
			6: 5,
		},
		StorageSuffixRecordsByTimestamp: map[uint64]uint32{
			1: 1,
			2: 3,
			3: 3,
			4: 3,
			5: 3,
			6: 3,
		},
		AccountsInState: 5,
	}
	if !reflect.DeepEqual(expected, res) {
		spew.Dump(getStat(db))
		t.Fatal("not equals")
	}

	err = core.Prune(db, 0, uint64(blockNum-1))
	assertNil(t, err)
	res, err = getStat(db)
	assertNil(t, err)

	expected = stateStats{
		ErrAccountsInHistory:          0,
		NumOfChangesInAccountsHistory: 5,
		AccountSuffixRecordsByTimestamp: map[uint64]uint32{
			6: 5,
		},
		StorageSuffixRecordsByTimestamp: map[uint64]uint32{
			6: 3,
		},
		AccountsInState: 5,
	}

	if !reflect.DeepEqual(expected, res) {
		spew.Dump(getStat(db))
		t.Fatal("not equals")
	}

}

//Simple E2E test that starts pruning an inserts blocks
func TestBasisAccountPruningStrategy(t *testing.T) {
	// TODO: recover
	t.Skip()

	// Configure and generate a sample block chain
	db := ethdb.NewMemDatabase()
	defer db.Close()
	var (
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
				EIP150Block:         new(big.Int),
				EIP155Block:         new(big.Int),
				EIP158Block:         big.NewInt(1),
				ByzantiumBlock:      big.NewInt(1),
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
	defer genesisDb.Close()

	numBlocks := 25
	engine := ethash.NewFaker()
	txCacher := core.NewTxSenderCacher(runtime.NumCPU())
	blockchain, err := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil, txCacher)
	if err != nil {
		t.Fatal(err)
	}
	defer blockchain.Stop()

	blocks, _, err := core.GenerateChain(gspec.Config, genesis, engine, genesisDb, numBlocks, func(i int, block *core.BlockGen) {
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
		tx, genErr = types.SignTx(types.NewTransaction(block.TxNonce(addr), theAddr, uint256.NewInt().SetUint64(1000), 21000, new(uint256.Int), nil), signer, k)
		if genErr != nil {
			t.Fatal(genErr)
		}
		block.AddTx(tx)
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	pruner, err := core.NewBasicPruner(db, blockchain, &core.CacheConfig{BlocksBeforePruning: 1, BlocksToPrune: 10, PruneTimeout: time.Second})
	assertNil(t, err)
	err = pruner.Start()
	assertNil(t, err)

	for i := range blocks {
		_, err = blockchain.InsertChain(context.Background(), types.Blocks{blocks[i]})
		if err != nil {
			t.Fatal(err)
		}
		if i%5 == 0 {
			time.Sleep(300 * time.Millisecond)
		}
	}
	timeout := time.After(time.Second * 5)
	for {
		select {
		case <-timeout:
			t.Fatal("Prunnig timeout")
		default:
			//wait pruning correct state
			res, err := getStat(db)
			if err != nil {
				t.Fatal(err)
			}

			expected := stateStats{
				ErrAccountsInHistory:          0,
				NumOfChangesInAccountsHistory: 3,
				AccountSuffixRecordsByTimestamp: map[uint64]uint32{
					25: 3,
				},
				StorageSuffixRecordsByTimestamp: map[uint64]uint32{},
				AccountsInState:                 5,
			}
			if reflect.DeepEqual(expected, res) {
				return
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}

type stateStats struct {
	NumOfChangesInAccountsHistory   uint64
	ErrAccountsInHistory            uint64
	AccountSuffixRecordsByTimestamp map[uint64]uint32
	StorageSuffixRecordsByTimestamp map[uint64]uint32
	AccountsInState                 uint64
}

func getStat(db ethdb.Database) (stateStats, error) {
	stat := stateStats{
		AccountSuffixRecordsByTimestamp: make(map[uint64]uint32),
		StorageSuffixRecordsByTimestamp: make(map[uint64]uint32),
	}
	dec := changeset.Mapper[dbutils.AccountChangeSetBucket].Decode
	err := db.Walk(dbutils.AccountChangeSetBucket, []byte{}, 0, func(key, v []byte) (b bool, e error) {
		timestamp, parsedK, _ := dec(key, v)
		if _, ok := stat.AccountSuffixRecordsByTimestamp[timestamp]; ok {
			panic("multiple account suffix records")
		}
		stat.AccountSuffixRecordsByTimestamp[timestamp] = uint32(changeset.Len(v))

		compKey, _ := dbutils.CompositeKeySuffix(common.CopyBytes(parsedK), timestamp)
		_, err := db.Get(dbutils.AccountsHistoryBucket, compKey)
		if err != nil {
			stat.ErrAccountsInHistory++
			return false, err
		}
		stat.NumOfChangesInAccountsHistory++

		return true, nil
	})
	if err != nil {
		return stateStats{}, err
	}

	dec = changeset.Mapper[dbutils.StorageChangeSetBucket].Decode
	err = db.Walk(dbutils.StorageChangeSetBucket, []byte{}, 0, func(key, v []byte) (b bool, e error) {
		timestamp, _, _ := dec(key, v)
		if _, ok := stat.StorageSuffixRecordsByTimestamp[timestamp]; ok {
			panic("multiple storage suffix records")
		}
		stat.StorageSuffixRecordsByTimestamp[timestamp] = uint32(changeset.Len(v))

		return true, nil
	})
	if err != nil {
		return stateStats{}, err
	}

	err = db.Walk(dbutils.CurrentStateBucket, []byte{}, 0, func(key, v []byte) (b bool, e error) {
		if len(key) > 32 {
			return true, nil
		}
		stat.AccountsInState++
		return true, nil
	})
	if err != nil {
		return stateStats{}, err
	}
	return stat, nil
}

func assertNil(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
