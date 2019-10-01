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
	"context"
	"math/big"
	"testing"

	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state/contracts"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
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
				EIP155Block:         new(big.Int),
				EIP158Block:         big.NewInt(1),
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
	if _, err = blockchain.InsertChain(types.Blocks{blocks[0]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 1", contractAddress.String())
	}

	// BLOCK 2
	if _, err = blockchain.InsertChain(types.Blocks{blocks[1]}); err != nil {
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
	if _, err = blockchain.InsertChain(types.Blocks{blocks[2]}); err != nil {
		t.Fatal(err)
	}
	st, _, _ = blockchain.State()
	if st.Exist(create2address) {
		t.Error("expected create2address to be self-destructed at the block 3", create2address.String())
	}

	// BLOCK 4
	if _, err = blockchain.InsertChain(types.Blocks{blocks[3]}); err != nil {
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
				EIP155Block:         new(big.Int),
				EIP158Block:         big.NewInt(1),
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
	if _, err = blockchain.InsertChain(types.Blocks{blocks[0]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 1", contractAddress.String())
	}

	// BLOCKS 2 + 3
	if _, err = blockchain.InsertChain(types.Blocks{blocks[1], blocks[2]}); err != nil {
		t.Fatal(err)
	}

	st, _, _ = blockchain.State()
	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist at the block 3", contractAddress.String())
	}

	// REORG of block 2 and 3, and insert new (empty) BLOCK 2, 3, and 4
	if _, err = blockchain.InsertChain(types.Blocks{longerBlocks[1], longerBlocks[2], longerBlocks[3]}); err != nil {
		t.Fatal(err)
	}
	st, _, _ = blockchain.State()
	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 4", contractAddress.String())
	}
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
				EIP155Block:         new(big.Int),
				EIP158Block:         big.NewInt(1),
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
	if _, err = blockchain.InsertChain(types.Blocks{blocks[0]}); err != nil {
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
