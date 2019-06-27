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
	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
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
		address = crypto.PubkeyToAddress(key.PublicKey)
		theAddr = common.Address{1}
		funds   = big.NewInt(1000000000)
		gspec   = &core.Genesis{
			Config: &params.ChainConfig{
				ChainID:        big.NewInt(1),
				HomesteadBlock: new(big.Int),
				EIP155Block:    new(big.Int),
				EIP158Block:    big.NewInt(1),
				EIP2027Block:   big.NewInt(2),
			},
			Alloc: core.GenesisAlloc{address: {Balance: funds}},
		}
		genesis   = gspec.MustCommit(db)
		genesisDb = db.MemCopy()
		// this code generates a log
		signer = types.NewEIP155Signer(gspec.Config.ChainID)
	)

	engine := ethash.NewFaker()
	blockchain, err := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}

	blockchain.EnableReceipts(true)

	contractBackend := backends.NewSimulatedBackendMock(genesis, gspec.Config, blockchain, engine, db)
	transactOpts := bind.NewKeyedTransactor(key)

	var contractAddress common.Address
	var eipContract *contracts.Eip2027

	blocks, _ := core.GenerateChain(gspec.Config, genesis, engine, genesisDb, 3, func(i int, block *core.BlockGen) {
		var (
			tx  *types.Transaction
			err error
		)
		switch i {
		case 0:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, big.NewInt(1000), 21000, new(big.Int), nil), signer, key)
		case 1:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, big.NewInt(1000), 21000, new(big.Int), nil), signer, key)
		case 2:
			//tx, err = types.SignTx(types.NewContractCreation(block.TxNonce(address), new(big.Int), 1000000, new(big.Int), code), signer, key)
			contractAddress, tx, eipContract, err = contracts.DeployEip2027(transactOpts, contractBackend)

			block.TxNonce(address)
			//contractBackend.Commit()

		}
		if err != nil {
			t.Fatal(err)
		}
		contractBackend.SendTransaction(gspec.Config.WithEIPsEnabledCTX(context.Background(), block.Number()), tx)
		//block.AddTx(tx)
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

	st, tr, _ := blockchain.State()
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}

	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 2")
	}

	t.Log(string(tr.Dump()))
	storageSize = st.StorageSize(contractAddress)
	if storageSize == nil {
		t.Fatal("storage size should not be nil", st.GetCodeHash(contractAddress).Hex())
	}
	if *storageSize == 0 {
		t.Fatal("storage size should not be 0", *storageSize, st.GetCodeHash(contractAddress).Hex())
	}

	_ = eipContract
}
