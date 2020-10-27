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
	"math/big"
	"runtime"
	"testing"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/tests/contracts"
)

func TestSelfDestructReceive(t *testing.T) {
	// Configure and generate a sample block chain
	db := ethdb.NewMemDatabase()
	defer db.Close()
	var (
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &core.Genesis{
			Config: &params.ChainConfig{
				ChainID:             big.NewInt(1),
				HomesteadBlock:      new(big.Int),
				ByzantiumBlock:      new(big.Int),
				ConstantinopleBlock: new(big.Int),
				EIP150Block:         new(big.Int),
				EIP155Block:         new(big.Int),
				EIP158Block:         big.NewInt(0),
			},
			Alloc: core.GenesisAlloc{
				address: {Balance: funds},
			},
		}
		genesis = gspec.MustCommit(db)
		// this code generates a log
		signer = types.HomesteadSigner{}
	)
	genesisDB := db.MemCopy()
	defer genesisDB.Close()

	engine := ethash.NewFaker()
	txCacher := core.NewTxSenderCacher(runtime.NumCPU())
	blockchain, err := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil, txCacher)
	if err != nil {
		t.Fatal(err)
	}
	defer txCacher.Close()

	blockchain.EnableReceipts(true)

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOpts := bind.NewKeyedTransactor(key)

	var contractAddress common.Address
	var selfDestructorContract *contracts.SelfDestructor

	// There are two blocks
	// First block deploys a contract, then makes it self-destruct, and then sends 1 wei to the address of the contract,
	// effectively turning it from contract account to a non-contract account
	// The second block is empty and is only used to force the newly created blockchain object to reload the trie
	// from the database.
	blocks, _, err := core.GenerateChain(gspec.Config, genesis, engine, genesisDB, 2, func(i int, block *core.BlockGen) {
		var tx *types.Transaction

		switch i {
		case 0:
			contractAddress, tx, selfDestructorContract, err = contracts.DeploySelfDestructor(transactOpts, contractBackend)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
			tx, err = selfDestructorContract.SelfDestruct(transactOpts)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
			// Send 1 wei to contract after self-destruction
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), contractAddress, uint256.NewInt().SetUint64(1000), 21000, uint256.NewInt().SetUint64(1), nil), signer, key)
			block.AddTx(tx)
		}
		contractBackend.Commit()
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	st := state.New(state.NewDbStateReader(db))
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

	// Reload blockchain from the database, then inserting an empty block (3) will cause rebuilding of the trie
	txCacher1 := core.NewTxSenderCacher(runtime.NumCPU())
	blockchain1, err := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil, txCacher1)
	if err != nil {
		t.Fatal(err)
	}
	defer blockchain1.Stop()

	blockchain1.EnableReceipts(true)

	// BLOCK 2
	if _, err := blockchain1.InsertChain(context.Background(), types.Blocks{blocks[1]}); err != nil {
		t.Fatal(err)
	}
	// If we got this far, the newly created blockchain (with empty trie cache) loaded trie from the database
	// and that means that the state of the accounts written in the first block was correct.
	// This test checks that the storage root of the account is properly set to the root of the empty tree

	st = state.New(state.NewDbStateReader(db))
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}
	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 2", contractAddress.String())
	}
	if len(st.GetCode(contractAddress)) != 0 {
		t.Error("expected empty code in contract at block 2", contractAddress.String())
	}

}
