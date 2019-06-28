// Copyright 2016 The go-ethereum Authors
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

package tests

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"math/big"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/tests/contracts"
)

func TestEIP2027AccountStorageSize(t *testing.T) {
	// Configure and generate a sample block chain
	var (
		db      = ethdb.NewMemDatabase()
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		key1, _  = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
		key2, _  = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
		address = crypto.PubkeyToAddress(key.PublicKey)
		address1 = crypto.PubkeyToAddress(key1.PublicKey)
		address2 = crypto.PubkeyToAddress(key2.PublicKey)
		theAddr = common.Address{1}
		funds   = big.NewInt(1000000000)
		gspec   = &core.Genesis{
			Config: &params.ChainConfig{
				ChainID:        big.NewInt(1),
				HomesteadBlock: new(big.Int),
				EIP155Block:    new(big.Int),
				EIP158Block:    big.NewInt(1),
				EIP2027Block:   big.NewInt(1),
				ConstantinopleBlock: big.NewInt(1),
			},
			Alloc: core.GenesisAlloc{
				address: {Balance: funds},
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
	transactOpts := bind.NewKeyedTransactor(key)
	transactOpts1 := bind.NewKeyedTransactor(key1)
	transactOpts2 := bind.NewKeyedTransactor(key2)

	var contractAddress common.Address
	var eipContract *contracts.Eip2027

	fmt.Println("========================================================================")

	blocks, _ := core.GenerateChain(gspec.Config, genesis, engine, genesisDb, 7, func(i int, block *core.BlockGen) {
		var (
			tx  *types.Transaction
			err error
		)

		ctx := gspec.Config.WithEIPsEnabledCTX(context.Background(), block.Number())

		switch i {
		case 0:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, big.NewInt(1000), 21000, new(big.Int), nil), signer, key)
		case 1:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, big.NewInt(1000), 21000, new(big.Int), nil), signer, key)
		case 2:
			fmt.Println("\n\nDeploy Account")
			contractAddress, tx, eipContract, err = contracts.DeployEip2027(transactOpts, contractBackend)
			fmt.Printf("\n\n")
		case 3:
			fmt.Println("\n\nCreate Account1 value 2")
			tx, err = eipContract.Create(transactOpts1, big.NewInt(2))
			fmt.Printf("\n\n")
		case 4:
			fmt.Println("\n\nCreate Account2 value 3")
			tx, err = eipContract.Create(transactOpts2, big.NewInt(3))
			fmt.Printf("\n\n")
		case 5:
			fmt.Println("\n\nUpdate Account2 value 0")
			tx, err = eipContract.Update(transactOpts2, big.NewInt(0))
			fmt.Printf("\n\n")
		case 6:
			fmt.Println("\n\nRemove Account2")
			tx, err = eipContract.Remove(transactOpts2)
			fmt.Printf("\n\n")
		}

		if err != nil {
			t.Fatal(err)
		}

		if eipContract == nil {
			err = contractBackend.SendTransaction(ctx, tx)
			if err != nil {
				t.Fatal(err)
			}
		}

		block.AddTx(tx)
		contractBackend.Commit()
	})


	// BLOCK 0
	if _, err := blockchain.InsertChain(types.Blocks{blocks[0]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ := blockchain.State()
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}

	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist at the block 0", contractAddress.Hash().String())
	}

	storageSize := st.StorageSize(address)
	if storageSize != nil {
		t.Fatal("storage size should be nil at the block 0")
	}


	// BLOCK 1
	if _, err := blockchain.InsertChain(types.Blocks{blocks[1]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}

	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist at the block 1", contractAddress.Hash().String())
	}

	storageSize = st.StorageSize(address)
	if storageSize != nil {
		t.Fatal("storage size should be nil at the block 1")
	}


	// BLOCK 2
	if _, err := blockchain.InsertChain(types.Blocks{blocks[2]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	storageSize = st.StorageSize(contractAddress)
	if storageSize == nil {
		t.Fatal("storage size should not be nil", st.GetCodeHash(contractAddress).Hex())
	}
	if *storageSize != 0 {
		t.Fatal("storage size should be 0", *storageSize, st.GetCodeHash(contractAddress).Hex())
	}

	// BLock 3
	if _, err := blockchain.InsertChain(types.Blocks{blocks[3]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	storageSize = st.StorageSize(contractAddress)
	if storageSize == nil {
		t.Fatal("storage size should not be nil", st.GetCodeHash(contractAddress).Hex())
	}
	if *storageSize != state.HugeNumber+1 {
		t.Fatal("storage size should be HugeNumber+1", *storageSize, st.GetCodeHash(contractAddress).Hex())
	}


	// BLock 4
	if _, err := blockchain.InsertChain(types.Blocks{blocks[4]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	storageSize = st.StorageSize(contractAddress)
	if storageSize == nil {
		t.Fatal("storage size should not be nil", st.GetCodeHash(contractAddress).Hex())
	}
	if *storageSize != state.HugeNumber+2 {
		t.Fatal("storage size should be HugeNumber+2", *storageSize, st.GetCodeHash(contractAddress).Hex())
	}


	// BLock 5
	if _, err := blockchain.InsertChain(types.Blocks{blocks[5]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	storageSize = st.StorageSize(contractAddress)
	if storageSize == nil {
		t.Fatal("storage size should not be nil", st.GetCodeHash(contractAddress).Hex())
	}
	if *storageSize != state.HugeNumber+1 {
		t.Fatal("storage size should be HugeNumber+1", *storageSize, st.GetCodeHash(contractAddress).Hex())
	}

	// BLock 6
	if _, err := blockchain.InsertChain(types.Blocks{blocks[6]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}

	storageSize = st.StorageSize(contractAddress)
	if storageSize == nil {
		t.Fatal("storage size should not be nil", st.GetCodeHash(contractAddress).Hex())
	}
	if *storageSize != state.HugeNumber+1 {
		t.Fatal("storage size should be state.HugeNumber+1", *storageSize, st.GetCodeHash(contractAddress).Hex())
	}
}

func TestEIP2027AccountStorageSizeRevertRemove(t *testing.T) {
	// Configure and generate a sample block chain
	var (
		db      = ethdb.NewMemDatabase()
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		key1, _  = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
		key2, _  = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
		address = crypto.PubkeyToAddress(key.PublicKey)
		address1 = crypto.PubkeyToAddress(key1.PublicKey)
		address2 = crypto.PubkeyToAddress(key2.PublicKey)
		theAddr = common.Address{1}
		funds   = big.NewInt(1000000000)
		gspec   = &core.Genesis{
			Config: &params.ChainConfig{
				ChainID:        big.NewInt(1),
				HomesteadBlock: new(big.Int),
				EIP155Block:    new(big.Int),
				EIP158Block:    big.NewInt(1),
				EIP2027Block:   big.NewInt(1),
				ConstantinopleBlock: big.NewInt(1),
			},
			Alloc: core.GenesisAlloc{
				address: {Balance: funds},
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

	//contractBackend := backends.NewSimulatedBackend(gspec.Alloc, gspec.GasLimit)
	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOpts := bind.NewKeyedTransactor(key)
	transactOpts1 := bind.NewKeyedTransactor(key1)
	transactOpts2 := bind.NewKeyedTransactor(key2)

	var contractAddress common.Address
	var eipContract *contracts.Eip2027

	fmt.Println("========================================================================")

	blocks, _ := core.GenerateChain(gspec.Config, genesis, engine, genesisDb, 7, func(i int, block *core.BlockGen) {
		var (
			tx  *types.Transaction
			err error
		)

		ctx := gspec.Config.WithEIPsEnabledCTX(context.Background(), block.Number())

		switch i {
		case 0:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, big.NewInt(1000), 21000, new(big.Int), nil), signer, key)
		case 1:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, big.NewInt(1000), 21000, new(big.Int), nil), signer, key)
		case 2:
			fmt.Println("\n\nDeploy Account")
			contractAddress, tx, eipContract, err = contracts.DeployEip2027(transactOpts, contractBackend)
			fmt.Printf("\n\n")
		case 3:
			fmt.Println("\n\nCreate Account1 value 2")
			tx, err = eipContract.Create(transactOpts1, big.NewInt(2))
			fmt.Printf("\n\n")
		case 4:
			fmt.Println("\n\nCreate Account2 value 3")
			tx, err = eipContract.Create(transactOpts2, big.NewInt(3))
			fmt.Printf("\n\n")
		case 5:
			fmt.Println("\n\nUpdate Account2 value 0")
			tx, err = eipContract.Update(transactOpts2, big.NewInt(0))
			fmt.Printf("\n\n")
		case 6:
			fmt.Println("\n\nRemove Account2")
			tx, _ = eipContract.RemoveAndRevert(transactOpts2)
			fmt.Printf("\n\n")
		}


		if err != nil {
			t.Fatal(err)
		}

		if tx != nil {
			if eipContract == nil {
				err = contractBackend.SendTransaction(ctx, tx)
				if err != nil {
					t.Fatal(err)
				}
			}

			block.AddTx(tx)
		}

		contractBackend.Commit()
	})


	// BLOCK 0
	if _, err := blockchain.InsertChain(types.Blocks{blocks[0]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ := blockchain.State()
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}

	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist at the block 0", contractAddress.Hash().String())
	}

	storageSize := st.StorageSize(address)
	if storageSize != nil {
		t.Fatal("storage size should be nil at the block 0")
	}


	// BLOCK 1
	if _, err := blockchain.InsertChain(types.Blocks{blocks[1]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}

	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist at the block 1", contractAddress.Hash().String())
	}

	storageSize = st.StorageSize(address)
	if storageSize != nil {
		t.Fatal("storage size should be nil at the block 1")
	}


	// BLOCK 2
	if _, err := blockchain.InsertChain(types.Blocks{blocks[2]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	storageSize = st.StorageSize(contractAddress)
	if storageSize == nil {
		t.Fatal("storage size should not be nil", st.GetCodeHash(contractAddress).Hex())
	}
	if *storageSize != 0 {
		t.Fatal("storage size should be 0", *storageSize, st.GetCodeHash(contractAddress).Hex())
	}

	// BLock 3
	if _, err := blockchain.InsertChain(types.Blocks{blocks[3]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	storageSize = st.StorageSize(contractAddress)
	if storageSize == nil {
		t.Fatal("storage size should not be nil", st.GetCodeHash(contractAddress).Hex())
	}
	if *storageSize != state.HugeNumber+1 {
		t.Fatal("storage size should be HugeNumber+1", *storageSize, st.GetCodeHash(contractAddress).Hex())
	}


	// BLock 4
	if _, err := blockchain.InsertChain(types.Blocks{blocks[4]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	storageSize = st.StorageSize(contractAddress)
	if storageSize == nil {
		t.Fatal("storage size should not be nil", st.GetCodeHash(contractAddress).Hex())
	}
	if *storageSize != state.HugeNumber+2 {
		t.Fatal("storage size should be HugeNumber+2", *storageSize, st.GetCodeHash(contractAddress).Hex())
	}


	// BLock 5
	if _, err := blockchain.InsertChain(types.Blocks{blocks[5]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	storageSize = st.StorageSize(contractAddress)
	if storageSize == nil {
		t.Fatal("storage size should not be nil", st.GetCodeHash(contractAddress).Hex())
	}
	if *storageSize != state.HugeNumber+1 {
		t.Fatal("storage size should be HugeNumber+1", *storageSize, st.GetCodeHash(contractAddress).Hex())
	}

	// BLock 6
	if _, err := blockchain.InsertChain(types.Blocks{blocks[6]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}

	storageSize = st.StorageSize(contractAddress)
	if storageSize == nil {
		t.Fatal("storage size should not be nil", st.GetCodeHash(contractAddress).Hex())
	}
	if *storageSize != state.HugeNumber+1 {
		t.Fatal("storage size should be state.HugeNumber+1", *storageSize, st.GetCodeHash(contractAddress).Hex())
	}
}

func TestEIP2027AccountStorageSizeRevertUpdate(t *testing.T) {
	// Configure and generate a sample block chain
	var (
		db      = ethdb.NewMemDatabase()
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		key1, _  = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
		key2, _  = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
		address = crypto.PubkeyToAddress(key.PublicKey)
		address1 = crypto.PubkeyToAddress(key1.PublicKey)
		address2 = crypto.PubkeyToAddress(key2.PublicKey)
		theAddr = common.Address{1}
		funds   = big.NewInt(1000000000)
		gspec   = &core.Genesis{
			Config: &params.ChainConfig{
				ChainID:        big.NewInt(1),
				HomesteadBlock: new(big.Int),
				EIP155Block:    new(big.Int),
				EIP158Block:    big.NewInt(1),
				EIP2027Block:   big.NewInt(1),
				ConstantinopleBlock: big.NewInt(1),
			},
			Alloc: core.GenesisAlloc{
				address: {Balance: funds},
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

	//contractBackend := backends.NewSimulatedBackend(gspec.Alloc, gspec.GasLimit)
	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOpts := bind.NewKeyedTransactor(key)
	transactOpts1 := bind.NewKeyedTransactor(key1)
	transactOpts2 := bind.NewKeyedTransactor(key2)

	var contractAddress common.Address
	var eipContract *contracts.Eip2027

	fmt.Println("========================================================================")

	blocks, _ := core.GenerateChain(gspec.Config, genesis, engine, genesisDb, 6, func(i int, block *core.BlockGen) {
		var (
			tx  *types.Transaction
			err error
		)

		ctx := gspec.Config.WithEIPsEnabledCTX(context.Background(), block.Number())

		switch i {
		case 0:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, big.NewInt(1000), 21000, new(big.Int), nil), signer, key)
		case 1:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, big.NewInt(1000), 21000, new(big.Int), nil), signer, key)
		case 2:
			fmt.Println("\n\nDeploy Account")
			contractAddress, tx, eipContract, err = contracts.DeployEip2027(transactOpts, contractBackend)
			fmt.Printf("\n\n")
		case 3:
			fmt.Println("\n\nCreate Account1 value 2")
			tx, err = eipContract.Create(transactOpts1, big.NewInt(2))
			fmt.Printf("\n\n")
		case 4:
			fmt.Println("\n\nCreate Account2 value 3")
			tx, err = eipContract.Create(transactOpts2, big.NewInt(3))
			fmt.Printf("\n\n")
		case 5:
			fmt.Println("\n\nUpdate Account2 value 0")
			tx, _ = eipContract.UpdateAndRevert(transactOpts2, big.NewInt(0))
		}


		if err != nil {
			t.Fatal(err)
		}

		if tx != nil {
			if eipContract == nil {
				err = contractBackend.SendTransaction(ctx, tx)
				if err != nil {
					t.Fatal(err)
				}
			}

			block.AddTx(tx)
		}

		contractBackend.Commit()
	})


	// BLOCK 0
	// account must exist pre eip 161
	if _, err := blockchain.InsertChain(types.Blocks{blocks[0]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ := blockchain.State()
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}

	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist at the block 0", contractAddress.Hash().String())
	}

	storageSize := st.StorageSize(address)
	if storageSize != nil {
		t.Fatal("storage size should be nil at the block 0")
	}


	// BLOCK 1
	if _, err := blockchain.InsertChain(types.Blocks{blocks[1]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}

	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist at the block 1", contractAddress.Hash().String())
	}

	storageSize = st.StorageSize(address)
	if storageSize != nil {
		t.Fatal("storage size should be nil at the block 1")
	}


	// BLOCK 2
	if _, err := blockchain.InsertChain(types.Blocks{blocks[2]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	storageSize = st.StorageSize(contractAddress)
	if storageSize == nil {
		t.Fatal("storage size should not be nil", st.GetCodeHash(contractAddress).Hex())
	}
	if *storageSize != 0 {
		t.Fatal("storage size should be 0", *storageSize, st.GetCodeHash(contractAddress).Hex())
	}

	// BLock 3
	if _, err := blockchain.InsertChain(types.Blocks{blocks[3]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	storageSize = st.StorageSize(contractAddress)
	if storageSize == nil {
		t.Fatal("storage size should not be nil", st.GetCodeHash(contractAddress).Hex())
	}
	if *storageSize != state.HugeNumber+1 {
		t.Fatal("storage size should be HugeNumber+1", *storageSize, st.GetCodeHash(contractAddress).Hex())
	}


	// BLock 4
	if _, err := blockchain.InsertChain(types.Blocks{blocks[4]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	storageSize = st.StorageSize(contractAddress)
	if storageSize == nil {
		t.Fatal("storage size should not be nil", st.GetCodeHash(contractAddress).Hex())
	}
	if *storageSize != state.HugeNumber+2 {
		t.Fatal("storage size should be HugeNumber+2", *storageSize, st.GetCodeHash(contractAddress).Hex())
	}


	// BLock 5
	if _, err := blockchain.InsertChain(types.Blocks{blocks[5]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	storageSize = st.StorageSize(contractAddress)
	if storageSize == nil {
		t.Fatal("storage size should not be nil", st.GetCodeHash(contractAddress).Hex())
	}
	if *storageSize != state.HugeNumber+2 {
		t.Fatal("storage size should be HugeNumber+1", *storageSize, st.GetCodeHash(contractAddress).Hex(), *storageSize)
	}
}

func TestEIP2027AccountStorageSizeExceptionUpdate(t *testing.T) {
	// Configure and generate a sample block chain
	var (
		db      = ethdb.NewMemDatabase()
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		key1, _  = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
		key2, _  = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
		address = crypto.PubkeyToAddress(key.PublicKey)
		address1 = crypto.PubkeyToAddress(key1.PublicKey)
		address2 = crypto.PubkeyToAddress(key2.PublicKey)
		theAddr = common.Address{1}
		funds   = big.NewInt(1000000000)
		gspec   = &core.Genesis{
			Config: &params.ChainConfig{
				ChainID:        big.NewInt(1),
				HomesteadBlock: new(big.Int),
				EIP155Block:    new(big.Int),
				EIP158Block:    big.NewInt(1),
				EIP2027Block:   big.NewInt(1),
				ConstantinopleBlock: big.NewInt(1),
			},
			Alloc: core.GenesisAlloc{
				address: {Balance: funds},
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
	transactOpts := bind.NewKeyedTransactor(key)
	transactOpts1 := bind.NewKeyedTransactor(key1)
	transactOpts2 := bind.NewKeyedTransactor(key2)

	var contractAddress common.Address
	var eipContract *contracts.Eip2027

	fmt.Println("========================================================================")

	blocks, _ := core.GenerateChain(gspec.Config, genesis, engine, genesisDb, 6, func(i int, block *core.BlockGen) {
		var (
			tx  *types.Transaction
			err error
		)

		ctx := gspec.Config.WithEIPsEnabledCTX(context.Background(), block.Number())

		switch i {
		case 0:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, big.NewInt(1000), 21000, new(big.Int), nil), signer, key)
		case 1:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, big.NewInt(1000), 21000, new(big.Int), nil), signer, key)
		case 2:
			fmt.Println("\n\nDeploy Account")
			contractAddress, tx, eipContract, err = contracts.DeployEip2027(transactOpts, contractBackend)
			fmt.Printf("\n\n")
		case 3:
			fmt.Println("\n\nCreate Account1 value 2")
			tx, err = eipContract.Create(transactOpts1, big.NewInt(2))
			fmt.Printf("\n\n")
		case 4:
			fmt.Println("\n\nCreate Account2 value 3")
			tx, err = eipContract.Create(transactOpts2, big.NewInt(3))
			fmt.Printf("\n\n")
		case 5:
			fmt.Println("\n\nUpdate Account2 value 0")
			tx, _ = eipContract.UpdateAndException(transactOpts2, big.NewInt(0))
		}


		if err != nil {
			t.Fatal(err)
		}

		if tx != nil {
			if eipContract == nil {
				err = contractBackend.SendTransaction(ctx, tx)
				if err != nil {
					t.Fatal(err)
				}
			}

			block.AddTx(tx)
		}

		contractBackend.Commit()
	})


	// BLOCK 0
	if _, err := blockchain.InsertChain(types.Blocks{blocks[0]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ := blockchain.State()
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}

	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist at the block 0", contractAddress.Hash().String())
	}

	storageSize := st.StorageSize(address)
	if storageSize != nil {
		t.Fatal("storage size should be nil at the block 0")
	}


	// BLOCK 1
	if _, err := blockchain.InsertChain(types.Blocks{blocks[1]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}

	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist at the block 1", contractAddress.Hash().String())
	}

	storageSize = st.StorageSize(address)
	if storageSize != nil {
		t.Fatal("storage size should be nil at the block 1")
	}


	// BLOCK 2
	if _, err := blockchain.InsertChain(types.Blocks{blocks[2]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	storageSize = st.StorageSize(contractAddress)
	if storageSize == nil {
		t.Fatal("storage size should not be nil", st.GetCodeHash(contractAddress).Hex())
	}
	if *storageSize != 0 {
		t.Fatal("storage size should be 0", *storageSize, st.GetCodeHash(contractAddress).Hex())
	}

	// BLock 3
	if _, err := blockchain.InsertChain(types.Blocks{blocks[3]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	storageSize = st.StorageSize(contractAddress)
	if storageSize == nil {
		t.Fatal("storage size should not be nil", st.GetCodeHash(contractAddress).Hex())
	}
	if *storageSize != state.HugeNumber+1 {
		t.Fatal("storage size should be HugeNumber+1", *storageSize, st.GetCodeHash(contractAddress).Hex())
	}


	// BLock 4
	if _, err := blockchain.InsertChain(types.Blocks{blocks[4]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	storageSize = st.StorageSize(contractAddress)
	if storageSize == nil {
		t.Fatal("storage size should not be nil", st.GetCodeHash(contractAddress).Hex())
	}
	if *storageSize != state.HugeNumber+2 {
		t.Fatal("storage size should be HugeNumber+2", *storageSize, st.GetCodeHash(contractAddress).Hex())
	}


	// BLock 5
	if _, err := blockchain.InsertChain(types.Blocks{blocks[5]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	storageSize = st.StorageSize(contractAddress)
	if storageSize == nil {
		t.Fatal("storage size should not be nil", st.GetCodeHash(contractAddress).Hex())
	}
	if *storageSize != state.HugeNumber+2 {
		t.Fatal("storage size should be HugeNumber+1", *storageSize, st.GetCodeHash(contractAddress).Hex(), *storageSize)
	}
}
