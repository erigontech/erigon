// Copyright 2019 The go-ethereum Authors
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

package state_test

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/state/contracts"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Create revival problem
func TestCreate2Revive(t *testing.T) {
	// Configure and generate a sample block chain
	var (
		db      = ethdb.NewMemDatabase()
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &core.Genesis{
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
				address: {Balance: funds},
			},
		}
		genesis   = gspec.MustCommit(db)
		genesisDb = db.MemCopy()
		signer    = types.HomesteadSigner{}
	)

	engine := ethash.NewFaker()
	blockchain, err := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}

	blockchain.EnableReceipts(true)

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOpts := bind.NewKeyedTransactor(key)
	transactOpts.GasLimit = 1000000

	var contractAddress common.Address
	var revive *contracts.Revive
	// Change this address whenever you make any changes in the code of the revive contract in
	// contracts/revive.sol
	var create2address = common.HexToAddress("e70fd65144383e1189bd710b1e23b61e26315ff4")

	// There are 4 blocks
	// In the first block, we deploy the "factory" contract Revive, which can create children contracts via CREATE2 opcode
	// In the second block, we create the first child contract
	// In the third block, we cause the first child contract to selfdestruct
	// In the forth block, we create the second child contract, and we expect it to have a "clean slate" of storage,
	// i.e. without any storage items that "inherited" from the first child contract by mistake
	ctx := blockchain.WithContext(context.Background(), big.NewInt(genesis.Number().Int64()+1))
	blocks, _ := core.GenerateChain(ctx, gspec.Config, genesis, engine, genesisDb, 4, func(i int, block *core.BlockGen) {
		var tx *types.Transaction

		switch i {
		case 0:
			contractAddress, tx, revive, err = contracts.DeployRevive(transactOpts, contractBackend)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		case 1:
			tx, err = revive.Deploy(transactOpts, big.NewInt(0))
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		case 2:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), create2address, big.NewInt(0), 1000000, new(big.Int), nil), signer, key)
			if err != nil {
				t.Fatal(err)
			}
			err = contractBackend.SendTransaction(ctx, tx)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		case 3:
			tx, err = revive.Deploy(transactOpts, big.NewInt(0))
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		}
		contractBackend.Commit()
	})

	st, _, _ := blockchain.State()
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}
	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist before block 0", contractAddress.String())
	}

	// BLOCK 1
	if _, err = blockchain.InsertChain(context.Background(), types.Blocks{blocks[0]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 1", contractAddress.String())
	}

	// BLOCK 2
	if _, err = blockchain.InsertChain(context.Background(), types.Blocks{blocks[1]}); err != nil {
		t.Fatal(err)
	}
	var it *contracts.ReviveDeployEventIterator
	it, err = revive.FilterDeployEvent(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !it.Next() {
		t.Error("Expected DeployEvent")
	}
	if it.Event.D != create2address {
		t.Errorf("Wrong create2address: %x, expected %x", it.Event.D, create2address)
	}
	st, _, _ = blockchain.State()
	if !st.Exist(create2address) {
		t.Error("expected create2address to exist at the block 2", create2address.String())
	}
	// We expect number 0x42 in the position [2], because it is the block number 2
	check2 := st.GetState(create2address, common.BigToHash(big.NewInt(2)))
	if check2 != common.HexToHash("0x42") {
		t.Errorf("expected 0x42 in position 2, got: %x", check2)
	}

	// BLOCK 3
	if _, err = blockchain.InsertChain(context.Background(), types.Blocks{blocks[2]}); err != nil {
		t.Fatal(err)
	}
	st, _, _ = blockchain.State()
	if st.Exist(create2address) {
		t.Error("expected create2address to be self-destructed at the block 3", create2address.String())
	}

	// BLOCK 4
	if _, err = blockchain.InsertChain(context.Background(), types.Blocks{blocks[3]}); err != nil {
		t.Fatal(err)
	}
	it, err = revive.FilterDeployEvent(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !it.Next() {
		t.Error("Expected DeployEvent")
	}
	if it.Event.D != create2address {
		t.Errorf("Wrong create2address: %x, expected %x", it.Event.D, create2address)
	}
	st, _, _ = blockchain.State()
	if !st.Exist(create2address) {
		t.Error("expected create2address to exist at the block 2", create2address.String())
	}
	// We expect number 0x42 in the position [4], because it is the block number 4
	check4 := st.GetState(create2address, common.BigToHash(big.NewInt(4)))
	if check4 != common.HexToHash("0x42") {
		t.Errorf("expected 0x42 in position 4, got: %x", check4)
	}
	// We expect number 0x0 in the position [2], because it is the block number 4
	check2 = st.GetState(create2address, common.BigToHash(big.NewInt(2)))
	if check2 != common.HexToHash("0x0") {
		t.Errorf("expected 0x0 in position 2, got: %x", check2)
	}
}

func TestReorgOverSelfDestruct(t *testing.T) {
	// Configure and generate a sample block chain
	var (
		db      = ethdb.NewMemDatabase()
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &core.Genesis{
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
				address: {Balance: funds},
			},
		}
		genesis = gspec.MustCommit(db)
	)

	engine := ethash.NewFaker()
	blockchain, err := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}

	blockchain.EnableReceipts(true)

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOpts := bind.NewKeyedTransactor(key)
	transactOpts.GasLimit = 1000000

	var contractAddress common.Address
	var selfDestruct *contracts.Selfdestruct

	ctx := blockchain.WithContext(context.Background(), big.NewInt(genesis.Number().Int64()+1))
	// Here we generate 3 blocks, two of which (the one with "Change" invocation and "Destruct" invocation will be reverted during the reorg)
	blocks, _ := core.GenerateChain(ctx, gspec.Config, genesis, engine, db.MemCopy(), 3, func(i int, block *core.BlockGen) {
		var tx *types.Transaction

		switch i {
		case 0:
			contractAddress, tx, selfDestruct, err = contracts.DeploySelfdestruct(transactOpts, contractBackend)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		case 1:
			tx, err = selfDestruct.Change(transactOpts)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		case 2:
			tx, err = selfDestruct.Destruct(transactOpts)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		}
		contractBackend.Commit()
	})

	// Create a longer chain, with 4 blocks (with higher total difficulty) that reverts the change of stroage self-destruction of the contract
	contractBackendLonger := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOptsLonger := bind.NewKeyedTransactor(key)
	transactOptsLonger.GasLimit = 1000000

	longerBlocks, _ := core.GenerateChain(ctx, gspec.Config, genesis, engine, db.MemCopy(), 4, func(i int, block *core.BlockGen) {
		var tx *types.Transaction

		switch i {
		case 0:
			_, tx, _, err = contracts.DeploySelfdestruct(transactOptsLonger, contractBackendLonger)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		}
		contractBackendLonger.Commit()
	})

	st, _, _ := blockchain.State()
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}
	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist before block 0", contractAddress.String())
	}

	// BLOCK 1
	if _, err = blockchain.InsertChain(context.Background(), types.Blocks{blocks[0]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 1", contractAddress.String())
	}

	// Remember value of field "x" (storage item 0) after the first block, to check after rewinding
	correctValueX := st.GetState(contractAddress, common.Hash{})

	// BLOCKS 2 + 3
	if _, err = blockchain.InsertChain(context.Background(), types.Blocks{blocks[1], blocks[2]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist at the block 3", contractAddress.String())
	}

	fmt.Println("-------Reorg")
	// REORG of block 2 and 3, and insert new (empty) BLOCK 2, 3, and 4
	if _, err = blockchain.InsertChain(context.Background(), types.Blocks{longerBlocks[1], longerBlocks[2], longerBlocks[3]}); err != nil {
		t.Fatal(err)
	}
	st, _, _ = blockchain.State()
	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 4", contractAddress.String())
	}

	// Reload blockchain from the database
	blockchain, err = core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	st, _, _ = blockchain.State()
	valueX := st.GetState(contractAddress, common.Hash{})
	if valueX != correctValueX {
		t.Fatalf("storage value has changed after reorg: %x, expected %x", valueX, correctValueX)
	}
}

func TestReorgOverStateChange(t *testing.T) {
	// Configure and generate a sample block chain
	var (
		db      = ethdb.NewMemDatabase()
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &core.Genesis{
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
				address: {Balance: funds},
			},
		}
		genesis = gspec.MustCommit(db)
	)

	engine := ethash.NewFaker()
	blockchain, err := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}

	blockchain.EnableReceipts(true)

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOpts := bind.NewKeyedTransactor(key)
	transactOpts.GasLimit = 1000000

	var contractAddress common.Address
	var selfDestruct *contracts.Selfdestruct

	ctx := blockchain.WithContext(context.Background(), big.NewInt(genesis.Number().Int64()+1))
	// Here we generate 3 blocks, two of which (the one with "Change" invocation and "Destruct" invocation will be reverted during the reorg)
	blocks, _ := core.GenerateChain(ctx, gspec.Config, genesis, engine, db.MemCopy(), 2, func(i int, block *core.BlockGen) {
		var tx *types.Transaction

		switch i {
		case 0:
			contractAddress, tx, selfDestruct, err = contracts.DeploySelfdestruct(transactOpts, contractBackend)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		case 1:
			tx, err = selfDestruct.Change(transactOpts)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		}
		contractBackend.Commit()
		fmt.Println("commited i=", i)
	})

	// Create a longer chain, with 4 blocks (with higher total difficulty) that reverts the change of stroage self-destruction of the contract
	contractBackendLonger := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOptsLonger := bind.NewKeyedTransactor(key)
	transactOptsLonger.GasLimit = 1000000
	longerBlocks, _ := core.GenerateChain(ctx, gspec.Config, genesis, engine, db.MemCopy(), 3, func(i int, block *core.BlockGen) {
		var tx *types.Transaction

		switch i {
		case 0:
			_, tx, _, err = contracts.DeploySelfdestruct(transactOptsLonger, contractBackendLonger)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		}
		contractBackendLonger.Commit()
	})

	st, _, _ := blockchain.State()
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}
	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist before block 0", contractAddress.String())
	}

	// BLOCK 1
	if _, err = blockchain.InsertChain(context.Background(), types.Blocks{blocks[0]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 1", contractAddress.String())
	}

	// Remember value of field "x" (storage item 0) after the first block, to check after rewinding
	correctValueX := st.GetState(contractAddress, common.Hash{})

	fmt.Println("Insert block 2")
	// BLOCK 2
	if _, err = blockchain.InsertChain(context.Background(), types.Blocks{blocks[1]}); err != nil {
		t.Fatal(err)
	}

	fmt.Println("Insert long blocks 2,3")
	// REORG of block 2 and 3, and insert new (empty) BLOCK 2, 3, and 4
	if _, err = blockchain.InsertChain(context.Background(), types.Blocks{longerBlocks[1], longerBlocks[2]}); err != nil {
		t.Fatal(err)
	}
	st, _, _ = blockchain.State()
	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 4", contractAddress.String())
	}

	// Reload blockchain from the database
	blockchain, err = core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	st, _, _ = blockchain.State()
	valueX := st.GetState(contractAddress, common.Hash{})
	if valueX != correctValueX {
		t.Fatalf("storage value has changed after reorg: %x, expected %x", valueX, correctValueX)
	}
}

func TestDatabaseStateChangeDBSizeDebug(t *testing.T) {
	t.Skip()
	if !debug.IsThinHistory() {
		t.Skip()
	}
	// Configure and generate a sample block chain
	numOfContracts := 10
	txPerBlock := 10
	numOfBlocks := 10
	var addresses []common.Address
	var transactOpts []*bind.TransactOpts
	for i := 0; i < numOfContracts; i++ {
		key, err := crypto.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		addresses = append(addresses, crypto.PubkeyToAddress(key.PublicKey))
		transactOpt := bind.NewKeyedTransactor(key)
		transactOpt.GasLimit = 1000000
		transactOpts = append(transactOpts, transactOpt)

	}
	funds := big.NewInt(1000000000)
	alloc := core.GenesisAlloc{}
	for _, v := range addresses {
		alloc[v] = core.GenesisAccount{Balance: funds}
	}
	var (
		db    = ethdb.NewRWDecorator(ethdb.NewMemDatabase())
		gspec = &core.Genesis{
			Config: &params.ChainConfig{
				ChainID:             big.NewInt(1),
				HomesteadBlock:      new(big.Int),
				EIP150Block:         new(big.Int),
				EIP155Block:         new(big.Int),
				EIP158Block:         big.NewInt(1),
				ByzantiumBlock:      big.NewInt(1),
				ConstantinopleBlock: big.NewInt(1),
			},
			Alloc: alloc,
		}
		genesis = gspec.MustCommit(db)
	)

	engine := ethash.NewFaker()
	blockchain, err := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}

	blockchain.EnableReceipts(true)

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)

	var selfDestruct = make([]*contracts.Selfdestruct, numOfContracts)

	ctx := blockchain.WithContext(context.Background(), big.NewInt(genesis.Number().Int64()+1))

	// Here we generate 3 blocks, two of which (the one with "Change" invocation and "Destruct" invocation will be reverted during the reorg)
	blocks, _ := core.GenerateChain(ctx, gspec.Config, genesis, engine, db.MemCopy(), numOfBlocks, func(i int, block *core.BlockGen) {
		var tx *types.Transaction

		switch i {
		case 0:
			for i := 0; i < numOfContracts; i++ {
				_, tx, selfDestruct[i], err = contracts.DeploySelfdestruct(transactOpts[i], contractBackend)
				if err != nil {
					t.Fatal(err)
				}
				block.AddTx(tx)
			}
		case numOfBlocks - 1:
			for i := 0; i < numOfContracts; i++ {
				for j := 0; j < txPerBlock; j++ {
					tx, err = selfDestruct[i].Destruct(transactOpts[i])
					if err != nil {
						t.Fatal(err)
					}
					block.AddTx(tx)
				}
			}
		default:
			for i := 0; i < numOfContracts; i++ {
				for j := 0; j < txPerBlock; j++ {
					tx, err = selfDestruct[i].Change(transactOpts[i])
					if err != nil {
						t.Fatal(err)
					}
					block.AddTx(tx)
				}
			}

		}
		contractBackend.Commit()
	})

	if _, err = blockchain.InsertChain(context.Background(), blocks); err != nil {
		t.Fatal(err)
	}

	stats := BucketsStats{}

	fmt.Println("==========================ACCOUNT===========================")
	err = blockchain.ChainDb().Walk(dbutils.AccountsBucket, []byte{}, 0, func(k []byte, v []byte) (b bool, e error) {
		acc := &accounts.Account{}
		innerErr := acc.DecodeForStorage(v)
		if innerErr != nil {
			t.Fatal(innerErr)
		}
		stats.Accounts += uint64(len(v))
		return true, nil
	})
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("==========================ACCOUNTHISTORY===========================")
	err = blockchain.ChainDb().Walk(dbutils.AccountsHistoryBucket, []byte{}, 0, func(k []byte, v []byte) (b bool, e error) {
		stats.HAT += uint64(len(v))
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("==========================STORAGE===========================")
	err = blockchain.ChainDb().Walk(dbutils.StorageBucket, []byte{}, 0, func(k []byte, v []byte) (b bool, e error) {
		stats.Storage += uint64(len(v))
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("==========================StorageHISTORY===========================")
	err = blockchain.ChainDb().Walk(dbutils.StorageHistoryBucket, []byte{}, 0, func(k []byte, v []byte) (b bool, e error) {
		stats.HST += uint64(len(v))
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("==========================CHANGESET===========================")
	err = blockchain.ChainDb().Walk(dbutils.AccountChangeSetBucket, []byte{}, 0, func(k []byte, v []byte) (b bool, e error) {
		stats.ChangeSetHAT += uint64(len(v))
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = blockchain.ChainDb().Walk(dbutils.StorageChangeSetBucket, []byte{}, 0, func(k []byte, v []byte) (b bool, e error) {
		stats.ChangeSetHST += uint64(len(v))
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	spew.Dump(stats)
	spew.Dump(db.DBCounterStats)
	spew.Dump(stats.Size())
}

type BucketsStats struct {
	Accounts     uint64
	Storage      uint64
	ChangeSetHAT uint64
	ChangeSetHST uint64
	HAT          uint64
	HST          uint64
}

func (b BucketsStats) Size() uint64 {
	return b.ChangeSetHST + b.ChangeSetHAT + b.HST + b.Storage + b.HAT + b.Accounts
}

func TestCreateOnExistingStorage(t *testing.T) {
	// Configure and generate a sample block chain
	var (
		db      = ethdb.NewMemDatabase()
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		// Address of the contract that will be deployed
		contractAddr = common.HexToAddress("0x3a220f351252089d385b29beca14e27f204c296a")
		funds        = big.NewInt(1000000000)
		gspec        = &core.Genesis{
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
				address: {Balance: funds},
				// Pre-existing storage item in an account without code
				contractAddr: {Balance: funds, Storage: map[common.Hash]common.Hash{{}: common.HexToHash("0x42")}},
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
	transactOpts.GasLimit = 1000000

	var contractAddress common.Address
	// There is one block, and it ends up deploying Revive contract (could be any other contract, it does not really matter)
	// On the address contractAddr, where there is a storage item in the genesis, but no contract code
	// We expect the pre-existing storage items to be removed by the deployment
	ctx := blockchain.WithContext(context.Background(), big.NewInt(genesis.Number().Int64()+1))
	blocks, _ := core.GenerateChain(ctx, gspec.Config, genesis, engine, genesisDb, 4, func(i int, block *core.BlockGen) {
		var tx *types.Transaction

		switch i {
		case 0:
			contractAddress, tx, _, err = contracts.DeployRevive(transactOpts, contractBackend)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		}
		contractBackend.Commit()
	})

	st, _, _ := blockchain.State()
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}
	if contractAddress != contractAddr {
		t.Errorf("expected contract address to be %x, got: %x", contractAddr, contractAddress)
	}

	// BLOCK 1
	if _, err = blockchain.InsertChain(context.Background(), types.Blocks{blocks[0]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 1", contractAddress.String())
	}
	// We expect number 0x42 in the position [2], because it is the block number 2
	check0 := st.GetState(contractAddress, common.BigToHash(big.NewInt(0)))
	if check0 != common.HexToHash("0x0") {
		t.Errorf("expected 0x00 in position 0, got: %x", check0)
	}
}

func TestReproduceCrash(t *testing.T) {
	// This example was taken from Ropsten contract that used to cause a crash
	// it is created in the block 598915 and then there are 3 transactions modifying
	// its storage in the same block:
	// 1. Setting storageKey 1 to a non-zero value
	// 2. Setting storageKey 2 to a non-zero value
	// 3. Setting both storageKey1 and storageKey2 to zero values
	value0 := common.Hash{}
	contract := common.HexToAddress("0x71dd1027069078091B3ca48093B00E4735B20624")
	storageKey1 := common.HexToHash("0x0e4c0e7175f9d22279a4f63ff74f7fa28b7a954a6454debaa62ce43dd9132541")
	value1 := common.HexToHash("0x016345785d8a0000")
	storageKey2 := common.HexToHash("0x0e4c0e7175f9d22279a4f63ff74f7fa28b7a954a6454debaa62ce43dd9132542")
	value2 := common.HexToHash("0x58c00a51")
	db := ethdb.NewMemDatabase()
	tds, err := state.NewTrieDbState(common.Hash{}, db, 0)
	if err != nil {
		t.Errorf("could not create TrieDbState: %v", err)
	}
	tsw := tds.TrieStateWriter()
	intraBlockState := state.New(tds)
	ctx := context.Background()
	// Start the 1st transaction
	tds.StartNewBuffer()
	intraBlockState.CreateAccount(contract, true)
	if err = intraBlockState.FinalizeTx(ctx, tsw); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}
	// Start the 2nd transaction
	tds.StartNewBuffer()
	intraBlockState.SetState(contract, storageKey1, value1)
	if err = intraBlockState.FinalizeTx(ctx, tsw); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}
	// Start the 3rd transaction
	tds.StartNewBuffer()
	intraBlockState.AddBalance(contract, big.NewInt(1000000000))
	intraBlockState.SetState(contract, storageKey2, value2)
	if err = intraBlockState.FinalizeTx(ctx, tsw); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}
	// Start the 4th transaction - clearing both storage cells
	tds.StartNewBuffer()
	intraBlockState.SubBalance(contract, big.NewInt(1000000000))
	intraBlockState.SetState(contract, storageKey1, value0)
	intraBlockState.SetState(contract, storageKey2, value0)
	if err = intraBlockState.FinalizeTx(ctx, tsw); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}
	if _, err = tds.ComputeTrieRoots(); err != nil {
		t.Errorf("ComputeTrieRoots failed: %v", err)
	}
	// We expect the list of prunable entries to be empty
	prunables := tds.TriePruningDebugDump()
	if len(prunables) > 0 {
		t.Errorf("Expected empty list of prunables, got:\n %s", prunables)
	}
}
func TestEip2200Gas(t *testing.T) {
	// Configure and generate a sample block chain
	var (
		db      = ethdb.NewMemDatabase()
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &core.Genesis{
			Config: &params.ChainConfig{
				ChainID:             big.NewInt(1),
				HomesteadBlock:      new(big.Int),
				EIP150Block:         new(big.Int),
				EIP155Block:         new(big.Int),
				EIP158Block:         big.NewInt(1),
				ByzantiumBlock:      big.NewInt(1),
				PetersburgBlock:     big.NewInt(1),
				ConstantinopleBlock: big.NewInt(1),
				IstanbulBlock:       big.NewInt(1),
			},
			Alloc: core.GenesisAlloc{
				address: {Balance: funds},
			},
		}
		genesis = gspec.MustCommit(db)
	)

	engine := ethash.NewFaker()
	blockchain, err := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}

	blockchain.EnableReceipts(true)

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOpts := bind.NewKeyedTransactor(key)
	transactOpts.GasLimit = 1000000

	var contractAddress common.Address
	var selfDestruct *contracts.Selfdestruct

	ctx := blockchain.WithContext(context.Background(), big.NewInt(genesis.Number().Int64()+1))
	// Here we generate 1 block with 2 transactions, first creates a contract with some initial values in the
	// It activates the SSTORE pricing rules specific to EIP-2200 (istanbul)
	blocks, _ := core.GenerateChain(ctx, gspec.Config, genesis, engine, db.MemCopy(), 3, func(i int, block *core.BlockGen) {
		var tx *types.Transaction

		switch i {
		case 0:
			contractAddress, tx, selfDestruct, err = contracts.DeploySelfdestruct(transactOpts, contractBackend)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)

			transactOpts.GasPrice = big.NewInt(1)
			tx, err = selfDestruct.Change(transactOpts)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		}
		contractBackend.Commit()
	})

	st, _, _ := blockchain.State()
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}
	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist before block 0", contractAddress.String())
	}
	balanceBefore := st.GetBalance(address)

	// BLOCK 1
	if _, err = blockchain.InsertChain(context.Background(), types.Blocks{blocks[0]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 1", contractAddress.String())
	}
	balanceAfter := st.GetBalance(address)
	gasSpent := big.NewInt(0).Sub(balanceBefore, balanceAfter)
	expectedGasSpent := big.NewInt(192245) // In the incorrect version, it is 179645
	if gasSpent.Cmp(expectedGasSpent) != 0 {
		t.Errorf("Expected gas spent: %d, got %d", expectedGasSpent, gasSpent)
	}
}

//Create contract, drop trie, reload trie from disk and add block with contract call
func TestWrongIncarnation(t *testing.T) {
	// Configure and generate a sample block chain
	var (
		db      = ethdb.NewMemDatabase()
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &core.Genesis{
			Config: &params.ChainConfig{
				ChainID:        big.NewInt(1),
				HomesteadBlock: new(big.Int),
				EIP150Block:    new(big.Int),
				EIP155Block:    new(big.Int),
				EIP158Block:    big.NewInt(1),
			},
			Alloc: core.GenesisAlloc{
				address: {Balance: funds},
			},
		}
		genesis = gspec.MustCommit(db)
	)

	engine := ethash.NewFaker()
	blockchain, err := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}

	blockchain.EnableReceipts(true)

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOpts := bind.NewKeyedTransactor(key)
	transactOpts.GasLimit = 1000000

	var contractAddress common.Address
	var changer *contracts.Changer

	ctx := blockchain.WithContext(context.Background(), big.NewInt(genesis.Number().Int64()+1))

	blocks, _ := core.GenerateChain(ctx, gspec.Config, genesis, engine, db.MemCopy(), 2, func(i int, block *core.BlockGen) {
		var tx *types.Transaction

		switch i {
		case 0:
			contractAddress, tx, changer, err = contracts.DeployChanger(transactOpts, contractBackend)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		case 1:
			tx, err = changer.Change(transactOpts)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		}
		contractBackend.Commit()
	})

	st, _, _ := blockchain.State()
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}
	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist before block 0", contractAddress.String())
	}

	// BLOCK 1
	if _, err = blockchain.InsertChain(context.Background(), types.Blocks{blocks[0]}); err != nil {
		t.Fatal(err)
	}

	addrHash := crypto.Keccak256(contractAddress[:])
	v, _ := db.Get(dbutils.AccountsBucket, addrHash)
	var acc accounts.Account
	err = acc.DecodeForStorage(v)
	if err != nil {
		t.Fatal(err)
	}
	if acc.Incarnation != state.FirstContractIncarnation {
		t.Fatal("Incorrect incarnation", acc.Incarnation)
	}

	st, _, _ = blockchain.State()
	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 1", contractAddress.String())
	}

	// Reload blockchain from the database
	blockchain, err = core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// BLOCKS 2
	if _, err = blockchain.InsertChain(context.Background(), types.Blocks{blocks[1]}); err != nil {
		t.Fatal(err)
	}
	addrHash = crypto.Keccak256(contractAddress[:])
	v, _ = db.Get(dbutils.AccountsBucket, addrHash)
	err = acc.DecodeForStorage(v)
	if err != nil {
		t.Fatal(err)
	}
	if acc.Incarnation != state.FirstContractIncarnation {
		t.Fatal("Incorrect incarnation", acc.Incarnation)
	}

	var startKey [common.HashLength + 8 + common.HashLength]byte
	copy(startKey[:], addrHash)
	err = db.Walk(dbutils.StorageBucket, startKey[:], 8*common.HashLength, func(k, v []byte) (bool, error) {
		fmt.Printf("%x: %x\n", k, v)
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

//create acc, deploy to it contract, reorg to state without contract
func TestWrongIncarnation2(t *testing.T) {
	// Configure and generate a sample block chain
	var (
		db      = ethdb.NewMemDatabase()
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &core.Genesis{
			Config: &params.ChainConfig{
				ChainID:        big.NewInt(1),
				HomesteadBlock: new(big.Int),
				EIP150Block:    new(big.Int),
				EIP155Block:    new(big.Int),
				EIP158Block:    big.NewInt(1),
			},
			Alloc: core.GenesisAlloc{
				address: {Balance: funds},
			},
		}
		genesis = gspec.MustCommit(db)
		signer  = types.HomesteadSigner{}
	)

	knownContractAddress := common.HexToAddress("0xdb7d6ab1f17c6b31909ae466702703daef9269cf")

	engine := ethash.NewFaker()
	blockchain, err := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}

	blockchain.EnableReceipts(true)

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOpts := bind.NewKeyedTransactor(key)
	transactOpts.GasLimit = 1000000

	var contractAddress common.Address
	//var changer *contracts.Changer

	ctx := blockchain.WithContext(context.Background(), big.NewInt(genesis.Number().Int64()+1))

	blocks, _ := core.GenerateChain(ctx, gspec.Config, genesis, engine, db.MemCopy(), 2, func(i int, block *core.BlockGen) {
		var tx *types.Transaction

		switch i {
		case 0:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), knownContractAddress, big.NewInt(1000), 1000000, new(big.Int), nil), signer, key)
			if err != nil {
				t.Fatal(err)
			}
			err = contractBackend.SendTransaction(ctx, tx)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		case 1:
			contractAddress, tx, _, err = contracts.DeployChanger(transactOpts, contractBackend)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		}
		contractBackend.Commit()
	})

	if knownContractAddress != contractAddress {
		t.Errorf("Expexted contractAddress: %x, got %x", knownContractAddress, contractAddress)
	}

	// Create a longer chain, with 4 blocks (with higher total difficulty) that reverts the change of stroage self-destruction of the contract
	contractBackendLonger := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOptsLonger := bind.NewKeyedTransactor(key)
	transactOptsLonger.GasLimit = 1000000
	longerBlocks, _ := core.GenerateChain(ctx, gspec.Config, genesis, engine, db.MemCopy(), 3, func(i int, block *core.BlockGen) {
		var tx *types.Transaction

		switch i {
		case 0:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), knownContractAddress, big.NewInt(1000), 1000000, new(big.Int), nil), signer, key)
			if err != nil {
				t.Fatal(err)
			}
			err = contractBackendLonger.SendTransaction(ctx, tx)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		}
		contractBackendLonger.Commit()
	})

	st, _, _ := blockchain.State()
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}

	// BLOCK 1
	if _, err = blockchain.InsertChain(context.Background(), types.Blocks{blocks[0]}); err != nil {
		t.Fatal(err)
	}

	// BLOCKS 2
	if _, err = blockchain.InsertChain(context.Background(), types.Blocks{blocks[1]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 1", contractAddress.String())
	}

	addrHash := crypto.Keccak256(contractAddress[:])
	v, err := db.Get(dbutils.AccountsBucket, addrHash)
	if err != nil {
		t.Fatal(err)
	}
	var acc accounts.Account
	err = acc.DecodeForStorage(v)
	if err != nil {
		t.Fatal(err)
	}
	if acc.Incarnation != state.FirstContractIncarnation {
		t.Fatal("wrong incarnation")
	}
	// REORG of block 2 and 3, and insert new (empty) BLOCK 2, 3, and 4
	if _, err = blockchain.InsertChain(context.Background(), types.Blocks{longerBlocks[1], longerBlocks[2]}); err != nil {
		t.Fatal(err)
	}

	v, err = db.Get(dbutils.AccountsBucket, addrHash)
	if err != nil {
		t.Fatal(err)
	}

	err = acc.DecodeForStorage(v)
	if err != nil {
		t.Fatal(err)
	}
	if acc.Incarnation != state.NonContractIncarnation {
		t.Fatal("wrong incarnation", acc.Incarnation)
	}

}

func TestClearTombstonesForReCreatedAccount(t *testing.T) {
	require, assert, db := require.New(t), assert.New(t), ethdb.NewMemDatabase()

	accKey := fmt.Sprintf("11%062x", 0)
	k1 := fmt.Sprintf("11%062x", 0)
	k2 := fmt.Sprintf("2211%062x", 0)
	k3 := fmt.Sprintf("2233%062x", 0)
	prefix := dbutils.GenerateStoragePrefix(common.HexToHash(accKey), 1)

	storageKey := func(storageKey string) []byte {
		return append(prefix, common.FromHex(storageKey)...)
	}

	putStorage := func(k string, v string) {
		err := db.Put(dbutils.StorageBucket, storageKey(k), common.FromHex(v))
		require.NoError(err)
	}
	putCache := func(k string, v string) {
		err := db.Put(dbutils.IntermediateTrieHashBucket, common.FromHex(k), common.FromHex(v))
		require.NoError(err)
	}

	putStorage(k1, "hi")
	putStorage(k2, "hi")
	putStorage(k3, "hi")
	// don't put k4 yet

	putCache(accKey, "")

	// step 1: re-create account
	err := state.ClearTombstonesForReCreatedAccount(db, common.HexToHash(accKey))
	require.NoError(err)

	checks := map[string]bool{
		accKey:        false,
		accKey + "11": true,
		accKey + "22": true,
		accKey + "aa": true,
	}

	for k, expect := range checks {
		ok, err1 := state.HasTombstone(db, common.FromHex(k))
		require.NoError(err1, k)
		assert.Equal(expect, ok, k)
	}

	// step 2: re-create storage
	someStorageExistsInThisSubtree1 := func(prefix []byte) bool {
		return false
	}

	err = state.ClearTombstonesForNewStorage(someStorageExistsInThisSubtree1, db, common.FromHex(accKey+k2))
	require.NoError(err)

	checks = map[string]bool{
		accKey + k2:         false,
		accKey + "2200":     true,
		accKey + "22ab":     true,
		accKey + "22110099": true,
	}

	for k, expect := range checks {
		ok, err1 := state.HasTombstone(db, common.FromHex(k))
		require.NoError(err1, k)
		assert.Equal(expect, ok, k)
	}

	// step 3: create one new storage
	someStorageExistsInThisSubtree2 := func(prefix []byte) bool {
		if bytes.HasPrefix(common.FromHex(accKey+k3), prefix) {
			return false
		}
		if bytes.HasPrefix(common.FromHex(accKey+k2), prefix) {
			return true
		}
		return false
	}

	err = state.ClearTombstonesForNewStorage(someStorageExistsInThisSubtree2, db, common.FromHex(accKey+k3))
	require.NoError(err)
	checks = map[string]bool{
		accKey + k2:                            false, // results of step2 preserved
		accKey + "22":                          false, // results of step2 preserved
		accKey + "2211":                        false, // results of step2 preserved
		accKey + "22110000":                    false, // results of step2 preserved
		accKey + k3:                            false,
		accKey + "223399":                      true,
		accKey + fmt.Sprintf("2233%04x99", 0):  true,
		accKey + fmt.Sprintf("2233%058x99", 0): true,  // len=64
		accKey + fmt.Sprintf("2233%060x99", 0): false, // len=66, too long key
		accKey + fmt.Sprintf("2233%062x99", 0): false, // len=68, too long key
	}

	for k, expect := range checks {
		ok, err := state.HasTombstone(db, common.FromHex(k))
		require.NoError(err, k)
		assert.Equal(expect, ok, k)
	}
}
