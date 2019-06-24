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
	"math/big"
	"testing"
	"time"

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
		code   = common.Hex2Bytes("60606040525b7f24ec1d3ff24c2f6ff210738839dbc339cd45a5294d85c79361016243157aae7b60405180905060405180910390a15b600a8060416000396000f360606040526008565b00")
	)

	engine := ethash.NewFaker()
	blockchain, err := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}

	blockchain.EnableReceipts(true)

	backend := backends.NewSimulatedBackendMock(genesis, gspec.Config, blockchain, engine, genesisDb)
	defer blockchain.Stop()

	var contractTx *types.Transaction
	blocks, _ := core.GenerateChain(gspec.Config, genesis, engine, genesisDb, 3, func(i int, block *core.BlockGen) {
		var (
			tx  *types.Transaction
			err error
		)
		switch i {
		case 0:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, big.NewInt(1000), 21000, new(big.Int), nil), signer, key)

			fmt.Println("tx 1", tx.Hash().String())
		case 1:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), theAddr, big.NewInt(1000), 21000, new(big.Int), nil), signer, key)
			fmt.Println("tx 2", tx.Hash().String())
		case 2:
			tx, err = types.SignTx(types.NewContractCreation(block.TxNonce(address), new(big.Int), 1000000, new(big.Int), code), signer, key)
			fmt.Println("tx 3", tx.Hash().String())
			contractTx = tx
		}
		if err != nil {
			t.Fatal(err)
		}
		block.AddTx(tx)
	})

	// account must exist pre eip 161
	if _, err := blockchain.InsertChain(types.Blocks{blocks[0]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ := blockchain.State()
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}
	storageSize := st.StorageSize(address)
	if storageSize != nil {
		t.Fatal("storage size should be nil")
	}

	if _, err := blockchain.InsertChain(types.Blocks{blocks[1]}); err != nil {
		t.Fatal(err)
	}
	st, _, _ = blockchain.State()
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}
	storageSize = st.StorageSize(address)
	if storageSize != nil {
		t.Fatal("storage size should be nil")
	}

	if _, err := blockchain.InsertChain(types.Blocks{blocks[2]}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	fmt.Println("!!!", contractTx.Hash().String())

	var contractAddress common.Address
	go func() {
		contractAddress, err = bind.WaitDeployed(ctx, backend, contractTx)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println("!!! 1")
	}()

	time.Sleep(2*time.Second)
	cancel()
	time.Sleep(2*time.Second)

	fmt.Println("!!!! 2", contractAddress.String())
	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist")
	}

	st, _, _ = blockchain.State()
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}
	storageSize = st.StorageSize(address)
	if storageSize == nil {
		t.Fatal("storage size should be XXX")
	}
	if *storageSize != 0 {
		t.Fatal("storage size should be XXX")
	}
}
