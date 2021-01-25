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
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/consensus/process"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/state/contracts"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

// Create revival problem
func TestCreate2Revive(t *testing.T) {
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
		signer  = types.HomesteadSigner{}
	)

	engine := ethash.NewFaker()

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackend.Close()
	transactOpts := bind.NewKeyedTransactor(key)
	transactOpts.GasLimit = 1000000

	var contractAddress common.Address
	var revive *contracts.Revive
	var err error
	// Change this address whenever you make any changes in the code of the revive contract in
	// contracts/revive.sol
	var create2address = common.HexToAddress("e70fd65144383e1189bd710b1e23b61e26315ff4")

	// There are 4 blocks
	// In the first block, we deploy the "factory" contract Revive, which can create children contracts via CREATE2 opcode
	// In the second block, we create the first child contract
	// In the third block, we cause the first child contract to selfdestruct
	// In the forth block, we create the second child contract, and we expect it to have a "clean slate" of storage,
	// i.e. without any storage items that "inherited" from the first child contract by mistake
	blocks, _, err := core.GenerateChain(gspec.Config, genesis, engine, db, 4, func(i int, block *core.BlockGen) {
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
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), create2address, uint256.NewInt(), 1000000, new(uint256.Int), nil), signer, key)
			if err != nil {
				t.Fatal(err)
			}
			err = contractBackend.SendTransaction(context.Background(), tx)
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
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	st := state.New(state.NewPlainStateReader(db))
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}
	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist before block 0", contractAddress.String())
	}

	// BLOCK 1
	exit := make(chan struct{})
	eng := process.NewConsensusProcess(engine, params.AllEthashProtocolChanges, exit)
	defer common.SafeClose(exit)
	if _, err = stagedsync.InsertBlockInStages(db, gspec.Config, &vm.Config{}, engine, eng, blocks[0], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	st = state.New(state.NewDbStateReader(db))
	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 1", contractAddress.String())
	}

	// BLOCK 2
	if _, err = stagedsync.InsertBlockInStages(db, gspec.Config, &vm.Config{}, engine, eng, blocks[1], true /* checkRoot */); err != nil {
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
	st = state.New(state.NewDbStateReader(db))
	if !st.Exist(create2address) {
		t.Error("expected create2address to exist at the block 2", create2address.String())
	}
	// We expect number 0x42 in the position [2], because it is the block number 2
	key2 := common.BigToHash(big.NewInt(2))
	var check2 uint256.Int
	st.GetState(create2address, &key2, &check2)
	if check2.Uint64() != 0x42 {
		t.Errorf("expected 0x42 in position 2, got: %x", check2.Uint64())
	}

	// BLOCK 3
	if _, err = stagedsync.InsertBlockInStages(db, gspec.Config, &vm.Config{}, engine, eng, blocks[2], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}
	st = state.New(state.NewDbStateReader(db))
	if st.Exist(create2address) {
		t.Error("expected create2address to be self-destructed at the block 3", create2address.String())
	}

	// BLOCK 4
	if _, err = stagedsync.InsertBlockInStages(db, gspec.Config, &vm.Config{}, engine, eng, blocks[3], true /* checkRoot */); err != nil {
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
	st = state.New(state.NewDbStateReader(db))
	if !st.Exist(create2address) {
		t.Error("expected create2address to exist at the block 2", create2address.String())
	}
	// We expect number 0x42 in the position [4], because it is the block number 4
	key4 := common.BigToHash(big.NewInt(4))
	var check4 uint256.Int
	st.GetState(create2address, &key4, &check4)
	if check4.Uint64() != 0x42 {
		t.Errorf("expected 0x42 in position 4, got: %x", check4.Uint64())
	}
	// We expect number 0x0 in the position [2], because it is the block number 4
	st.GetState(create2address, &key2, &check2)
	if !check2.IsZero() {
		t.Errorf("expected 0x0 in position 2, got: %x", check2)
	}
}

// Polymorthic contracts via CREATE2
func TestCreate2Polymorth(t *testing.T) {
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
		signer  = types.HomesteadSigner{}
	)

	engine := ethash.NewFaker()

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackend.Close()
	transactOpts := bind.NewKeyedTransactor(key)
	transactOpts.GasLimit = 1000000

	var contractAddress common.Address
	var poly *contracts.Poly
	var err error
	// Change this address whenever you make any changes in the code of the poly contract in
	// contracts/poly.sol
	var create2address = common.HexToAddress("c66aa74c220476f244b7f45897a124d1a01ca8a8")

	// There are 5 blocks
	// In the first block, we deploy the "factory" contract Poly, which can create children contracts via CREATE2 opcode
	// In the second block, we create the first child contract
	// In the third block, we cause the first child contract to selfdestruct
	// In the forth block, we create the second child contract
	// In the 5th block, we delete and re-create the child contract twice
	blocks, _, err := core.GenerateChain(gspec.Config, genesis, engine, db, 5, func(i int, block *core.BlockGen) {
		var tx *types.Transaction

		switch i {
		case 0:
			contractAddress, tx, poly, err = contracts.DeployPoly(transactOpts, contractBackend)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		case 1:
			tx, err = poly.Deploy(transactOpts, big.NewInt(0))
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		case 2:
			// Trigger self-destruct
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), create2address, uint256.NewInt(), 1000000, new(uint256.Int), nil), signer, key)
			if err != nil {
				t.Fatal(err)
			}
			err = contractBackend.SendTransaction(context.Background(), tx)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		case 3:
			tx, err = poly.Deploy(transactOpts, big.NewInt(0))
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		case 4:
			// Trigger self-destruct
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), create2address, uint256.NewInt(), 1000000, new(uint256.Int), nil), signer, key)
			if err != nil {
				t.Fatal(err)
			}
			err = contractBackend.SendTransaction(context.Background(), tx)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
			// Recreate in the same block
			tx, err = poly.Deploy(transactOpts, big.NewInt(0))
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
			// Trigger self-destruct
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), create2address, uint256.NewInt(), 1000000, new(uint256.Int), nil), signer, key)
			if err != nil {
				t.Fatal(err)
			}
			err = contractBackend.SendTransaction(context.Background(), tx)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
			// Recreate in the same block
			tx, err = poly.Deploy(transactOpts, big.NewInt(0))
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		}
		contractBackend.Commit()
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	st := state.New(state.NewPlainStateReader(db))
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}
	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist before block 0", contractAddress.String())
	}

	// BLOCK 1
	exit := make(chan struct{})
	eng := process.NewConsensusProcess(engine, params.AllEthashProtocolChanges, exit)
	defer common.SafeClose(exit)
	if _, err = stagedsync.InsertBlockInStages(db, gspec.Config, &vm.Config{}, engine, eng, blocks[0], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	st = state.New(state.NewDbStateReader(db))
	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 1", contractAddress.String())
	}

	// BLOCK 2
	if _, err = stagedsync.InsertBlockInStages(db, gspec.Config, &vm.Config{}, engine, eng, blocks[1], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}
	var it *contracts.PolyDeployEventIterator
	it, err = poly.FilterDeployEvent(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !it.Next() {
		t.Error("Expected DeployEvent")
	}
	if it.Event.D != create2address {
		t.Errorf("Wrong create2address: %x, expected %x", it.Event.D, create2address)
	}
	st = state.New(state.NewDbStateReader(db))
	if !st.Exist(create2address) {
		t.Error("expected create2address to exist at the block 2", create2address.String())
	}
	if !bytes.Equal(st.GetCode(create2address), common.FromHex("6002ff")) {
		t.Errorf("Expected CREATE2 deployed code 6002ff, got %x", st.GetCode(create2address))
	}
	if st.GetIncarnation(create2address) != 1 {
		t.Errorf("expected incarnation 1, got %d", st.GetIncarnation(create2address))
	}

	// BLOCK 3
	if _, err = stagedsync.InsertBlockInStages(db, gspec.Config, &vm.Config{}, engine, eng, blocks[2], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}
	st = state.New(state.NewDbStateReader(db))
	if st.Exist(create2address) {
		t.Error("expected create2address to be self-destructed at the block 3", create2address.String())
	}

	// BLOCK 4
	if _, err = stagedsync.InsertBlockInStages(db, gspec.Config, &vm.Config{}, engine, eng, blocks[3], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}
	it, err = poly.FilterDeployEvent(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !it.Next() {
		t.Error("Expected DeployEvent")
	}
	if it.Event.D != create2address {
		t.Errorf("Wrong create2address: %x, expected %x", it.Event.D, create2address)
	}
	st = state.New(state.NewDbStateReader(db))
	if !st.Exist(create2address) {
		t.Error("expected create2address to exist at the block 4", create2address.String())
	}
	if !bytes.Equal(st.GetCode(create2address), common.FromHex("6004ff")) {
		t.Errorf("Expected CREATE2 deployed code 6004ff, got %x", st.GetCode(create2address))
	}
	if st.GetIncarnation(create2address) != 2 {
		t.Errorf("expected incarnation 2, got %d", st.GetIncarnation(create2address))
	}

	// BLOCK 5
	if _, err = stagedsync.InsertBlockInStages(db, gspec.Config, &vm.Config{}, engine, eng, blocks[4], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}
	it, err = poly.FilterDeployEvent(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !it.Next() {
		t.Error("Expected DeployEvent")
	}
	if it.Event.D != create2address {
		t.Errorf("Wrong create2address: %x, expected %x", it.Event.D, create2address)
	}
	st = state.New(state.NewDbStateReader(db))
	if !st.Exist(create2address) {
		t.Error("expected create2address to exist at the block 5", create2address.String())
	}
	if !bytes.Equal(st.GetCode(create2address), common.FromHex("6005ff")) {
		t.Errorf("Expected CREATE2 deployed code 6005ff, got %x", st.GetCode(create2address))
	}
	if st.GetIncarnation(create2address) != 4 {
		t.Errorf("expected incarnation 4 (two self-destructs and two-recreations within a block), got %d", st.GetIncarnation(create2address))
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

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackend.Close()
	transactOpts := bind.NewKeyedTransactor(key)
	transactOpts.GasLimit = 1000000

	var contractAddress common.Address
	var selfDestruct *contracts.Selfdestruct
	var err error

	// Here we generate 3 blocks, two of which (the one with "Change" invocation and "Destruct" invocation will be reverted during the reorg)
	blocks, _, err := core.GenerateChain(gspec.Config, genesis, engine, db, 3, func(i int, block *core.BlockGen) {
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
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	// Create a longer chain, with 4 blocks (with higher total difficulty) that reverts the change of stroage self-destruction of the contract
	contractBackendLonger := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackendLonger.Close()
	transactOptsLonger := bind.NewKeyedTransactor(key)
	transactOptsLonger.GasLimit = 1000000

	longerBlocks, _, err := core.GenerateChain(gspec.Config, genesis, engine, db, 4, func(i int, block *core.BlockGen) {
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
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate long blocks")
	}

	st := state.New(state.NewPlainStateReader(db))
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}
	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist before block 0", contractAddress.String())
	}

	// BLOCK 1
	exit := make(chan struct{})
	eng := process.NewConsensusProcess(engine, params.AllEthashProtocolChanges, exit)
	defer common.SafeClose(exit)
	if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, engine, eng, blocks[0:1], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	st = state.New(state.NewDbStateReader(db))
	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 1", contractAddress.String())
	}

	// Remember value of field "x" (storage item 0) after the first block, to check after rewinding
	var key0 common.Hash
	var correctValueX uint256.Int
	st.GetState(contractAddress, &key0, &correctValueX)

	// BLOCKS 2 + 3
	if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, engine, eng, blocks[1:], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	st = state.New(state.NewDbStateReader(db))
	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist at the block 3", contractAddress.String())
	}

	// REORG of block 2 and 3, and insert new (empty) BLOCK 2, 3, and 4
	if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, engine, eng, longerBlocks[1:4], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}
	st = state.New(state.NewDbStateReader(db))
	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 4", contractAddress.String())
	}

	st = state.New(state.NewDbStateReader(db))
	var valueX uint256.Int
	st.GetState(contractAddress, &key0, &valueX)
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

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackend.Close()
	transactOpts := bind.NewKeyedTransactor(key)
	transactOpts.GasLimit = 1000000

	var contractAddress common.Address
	var selfDestruct *contracts.Selfdestruct
	var err error

	// Here we generate 3 blocks, two of which (the one with "Change" invocation and "Destruct" invocation will be reverted during the reorg)
	blocks, _, err := core.GenerateChain(gspec.Config, genesis, engine, db, 2, func(i int, block *core.BlockGen) {
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
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	// Create a longer chain, with 4 blocks (with higher total difficulty) that reverts the change of stroage self-destruction of the contract
	contractBackendLonger := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackendLonger.Close()
	transactOptsLonger := bind.NewKeyedTransactor(key)
	transactOptsLonger.GasLimit = 1000000
	longerBlocks, _, err := core.GenerateChain(gspec.Config, genesis, engine, db, 3, func(i int, block *core.BlockGen) {
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
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate longer blocks: %v", err)
	}

	st := state.New(state.NewPlainStateReader(db))
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}
	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist before block 0", contractAddress.String())
	}

	// BLOCK 1
	exit := make(chan struct{})
	eng := process.NewConsensusProcess(engine, params.AllEthashProtocolChanges, exit)
	defer common.SafeClose(exit)
	if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, engine, eng, blocks[:1], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	st = state.New(state.NewDbStateReader(db))
	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 1", contractAddress.String())
	}

	// Remember value of field "x" (storage item 0) after the first block, to check after rewinding
	var key0 common.Hash
	var correctValueX uint256.Int
	st.GetState(contractAddress, &key0, &correctValueX)

	// BLOCK 2
	if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, engine, eng, blocks[1:], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	// REORG of block 2 and 3, and insert new (empty) BLOCK 2, 3, and 4
	if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, engine, eng, longerBlocks[1:3], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}
	st = state.New(state.NewDbStateReader(db))
	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 4", contractAddress.String())
	}

	// Reload blockchain from the database
	st = state.New(state.NewDbStateReader(db))
	var valueX uint256.Int
	st.GetState(contractAddress, &key0, &valueX)
	if valueX != correctValueX {
		t.Fatalf("storage value has changed after reorg: %x, expected %x", valueX, correctValueX)
	}
}

func TestDatabaseStateChangeDBSizeDebug(t *testing.T) {
	t.Skip()

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
		db    = ethdb.NewMemDatabase()
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

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackend.Close()

	var selfDestruct = make([]*contracts.Selfdestruct, numOfContracts)
	var err error

	// Here we generate 3 blocks, two of which (the one with "Change" invocation and "Destruct" invocation will be reverted during the reorg)
	blocks, _, err := core.GenerateChain(gspec.Config, genesis, engine, db, numOfBlocks, func(i int, block *core.BlockGen) {
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
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	exit := make(chan struct{})
	eng := process.NewConsensusProcess(engine, params.AllEthashProtocolChanges, exit)
	defer common.SafeClose(exit)
	if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, engine, eng, blocks, true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	stats := BucketsStats{}

	fmt.Println("==========================ACCOUNT===========================")
	err = db.Walk(dbutils.CurrentStateBucket, []byte{}, 0, func(k []byte, v []byte) (b bool, e error) {
		if len(k) > 32 {
			return true, nil
		}
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
	err = db.Walk(dbutils.AccountsHistoryBucket, []byte{}, 0, func(k []byte, v []byte) (b bool, e error) {
		stats.HAT += uint64(len(v))
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("==========================STORAGE===========================")
	err = db.Walk(dbutils.CurrentStateBucket, []byte{}, 0, func(k []byte, v []byte) (b bool, e error) {
		stats.Storage += uint64(len(v))
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("==========================StorageHISTORY===========================")
	err = db.Walk(dbutils.StorageHistoryBucket, []byte{}, 0, func(k []byte, v []byte) (b bool, e error) {
		stats.HST += uint64(len(v))
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("==========================CHANGESET===========================")
	err = db.Walk(dbutils.AccountChangeSetBucket, []byte{}, 0, func(k []byte, v []byte) (b bool, e error) {
		stats.ChangeSetHAT += uint64(len(v))
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Walk(dbutils.StorageChangeSetBucket, []byte{}, 0, func(k []byte, v []byte) (b bool, e error) {
		stats.ChangeSetHST += uint64(len(v))
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	spew.Dump(stats)
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
	db := ethdb.NewMemDatabase()
	defer db.Close()
	var (
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
		genesis = gspec.MustCommit(db)
	)

	engine := ethash.NewFaker()

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackend.Close()
	transactOpts := bind.NewKeyedTransactor(key)
	transactOpts.GasLimit = 1000000

	var contractAddress common.Address
	var err error
	// There is one block, and it ends up deploying Revive contract (could be any other contract, it does not really matter)
	// On the address contractAddr, where there is a storage item in the genesis, but no contract code
	// We expect the pre-existing storage items to be removed by the deployment
	blocks, _, err := core.GenerateChain(gspec.Config, genesis, engine, db, 4, func(i int, block *core.BlockGen) {
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
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	st := state.New(state.NewPlainStateReader(db))
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}
	if contractAddress != contractAddr {
		t.Errorf("expected contract address to be %x, got: %x", contractAddr, contractAddress)
	}

	// BLOCK 1
	exit := make(chan struct{})
	eng := process.NewConsensusProcess(engine, params.AllEthashProtocolChanges, exit)
	defer common.SafeClose(exit)
	if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, engine, eng, blocks[:1], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	st = state.New(state.NewPlainStateReader(db))
	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 1", contractAddress.String())
	}

	var key0 common.Hash
	var check0 uint256.Int
	st.GetState(contractAddress, &key0, &check0)
	if !check0.IsZero() {
		t.Errorf("expected 0x00 in position 0, got: %x", check0.Bytes())
	}
}

func TestReproduceCrash(t *testing.T) {
	// This example was taken from Ropsten contract that used to cause a crash
	// it is created in the block 598915 and then there are 3 transactions modifying
	// its storage in the same block:
	// 1. Setting storageKey 1 to a non-zero value
	// 2. Setting storageKey 2 to a non-zero value
	// 3. Setting both storageKey1 and storageKey2 to zero values
	value0 := uint256.NewInt()
	contract := common.HexToAddress("0x71dd1027069078091B3ca48093B00E4735B20624")
	storageKey1 := common.HexToHash("0x0e4c0e7175f9d22279a4f63ff74f7fa28b7a954a6454debaa62ce43dd9132541")
	value1 := uint256.NewInt().SetUint64(0x016345785d8a0000)
	storageKey2 := common.HexToHash("0x0e4c0e7175f9d22279a4f63ff74f7fa28b7a954a6454debaa62ce43dd9132542")
	value2 := uint256.NewInt().SetUint64(0x58c00a51)
	db := ethdb.NewMemDatabase()
	tds := state.NewTrieDbState(common.Hash{}, db, 0)

	tsw := tds.TrieStateWriter()
	intraBlockState := state.New(tds)
	ctx := context.Background()
	// Start the 1st transaction
	tds.StartNewBuffer()
	intraBlockState.CreateAccount(contract, true)
	if err := intraBlockState.FinalizeTx(ctx, tsw); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}
	// Start the 2nd transaction
	tds.StartNewBuffer()
	intraBlockState.SetState(contract, &storageKey1, *value1)
	if err := intraBlockState.FinalizeTx(ctx, tsw); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}
	// Start the 3rd transaction
	tds.StartNewBuffer()
	intraBlockState.AddBalance(contract, uint256.NewInt().SetUint64(1000000000))
	intraBlockState.SetState(contract, &storageKey2, *value2)
	if err := intraBlockState.FinalizeTx(ctx, tsw); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}
	// Start the 4th transaction - clearing both storage cells
	tds.StartNewBuffer()
	intraBlockState.SubBalance(contract, uint256.NewInt().SetUint64(1000000000))
	intraBlockState.SetState(contract, &storageKey1, *value0)
	intraBlockState.SetState(contract, &storageKey2, *value0)
	if err := intraBlockState.FinalizeTx(ctx, tsw); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}
	if _, err := tds.ComputeTrieRoots(); err != nil {
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

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackend.Close()
	transactOpts := bind.NewKeyedTransactor(key)
	transactOpts.GasLimit = 1000000

	var contractAddress common.Address
	var selfDestruct *contracts.Selfdestruct
	var err error

	// Here we generate 1 block with 2 transactions, first creates a contract with some initial values in the
	// It activates the SSTORE pricing rules specific to EIP-2200 (istanbul)
	blocks, _, err := core.GenerateChain(gspec.Config, genesis, engine, db, 3, func(i int, block *core.BlockGen) {
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
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	st := state.New(state.NewPlainStateReader(db))
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}
	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist before block 0", contractAddress.String())
	}
	balanceBefore := st.GetBalance(address)

	// BLOCK 1
	exit := make(chan struct{})
	eng := process.NewConsensusProcess(engine, params.AllEthashProtocolChanges, exit)
	defer common.SafeClose(exit)
	if _, err = stagedsync.InsertBlockInStages(db, gspec.Config, &vm.Config{}, engine, eng, blocks[0], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	st = state.New(state.NewDbStateReader(db))
	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 1", contractAddress.String())
	}
	balanceAfter := st.GetBalance(address)
	gasSpent := big.NewInt(0).Sub(balanceBefore.ToBig(), balanceAfter.ToBig())
	expectedGasSpent := big.NewInt(190373) //(192245) // In the incorrect version, it is 179645
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

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackend.Close()
	transactOpts := bind.NewKeyedTransactor(key)
	transactOpts.GasLimit = 1000000

	var contractAddress common.Address
	var changer *contracts.Changer
	var err error

	blocks, _, err := core.GenerateChain(gspec.Config, genesis, engine, db, 2, func(i int, block *core.BlockGen) {
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
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	st := state.New(state.NewPlainStateReader(db))
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}
	if st.Exist(contractAddress) {
		t.Error("expected contractAddress to not exist before block 0", contractAddress.String())
	}

	// BLOCK 1
	exit := make(chan struct{})
	eng := process.NewConsensusProcess(engine, params.AllEthashProtocolChanges, exit)
	defer common.SafeClose(exit)
	if _, err = stagedsync.InsertBlockInStages(db, gspec.Config, &vm.Config{}, engine, eng, blocks[0], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	addrHash := crypto.Keccak256(contractAddress[:])
	var acc accounts.Account
	ok, err := rawdb.ReadAccount(db, common.BytesToHash(addrHash), &acc)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal(errors.New("acc not found"))
	}

	if acc.Incarnation != state.FirstContractIncarnation {
		t.Fatal("Incorrect incarnation", acc.Incarnation)
	}

	st = state.New(state.NewDbStateReader(db))
	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 1", contractAddress.String())
	}

	// BLOCKS 2
	if _, err = stagedsync.InsertBlockInStages(db, gspec.Config, &vm.Config{}, engine, eng, blocks[1], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}
	addrHash = crypto.Keccak256(contractAddress[:])
	ok, err = rawdb.ReadAccount(db, common.BytesToHash(addrHash), &acc)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal(errors.New("acc not found"))
	}
	if acc.Incarnation != state.FirstContractIncarnation {
		t.Fatal("Incorrect incarnation", acc.Incarnation)
	}

	var startKey [common.HashLength + 8 + common.HashLength]byte
	copy(startKey[:], addrHash)
	err = db.Walk(dbutils.CurrentStateBucket, startKey[:], 8*common.HashLength, func(k, v []byte) (bool, error) {
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

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackend.Close()
	transactOpts := bind.NewKeyedTransactor(key)
	transactOpts.GasLimit = 1000000
	var err error

	var contractAddress common.Address

	blocks, _, err := core.GenerateChain(gspec.Config, genesis, engine, db, 2, func(i int, block *core.BlockGen) {
		var tx *types.Transaction

		switch i {
		case 0:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), knownContractAddress, uint256.NewInt().SetUint64(1000), 1000000, new(uint256.Int), nil), signer, key)
			if err != nil {
				t.Fatal(err)
			}
			err = contractBackend.SendTransaction(context.Background(), tx)
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
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	if knownContractAddress != contractAddress {
		t.Errorf("Expexted contractAddress: %x, got %x", knownContractAddress, contractAddress)
	}

	// Create a longer chain, with 4 blocks (with higher total difficulty) that reverts the change of stroage self-destruction of the contract
	contractBackendLonger := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackendLonger.Close()
	transactOptsLonger := bind.NewKeyedTransactor(key)
	transactOptsLonger.GasLimit = 1000000
	longerBlocks, _, err := core.GenerateChain(gspec.Config, genesis, engine, db, 3, func(i int, block *core.BlockGen) {
		var tx *types.Transaction

		switch i {
		case 0:
			tx, err = types.SignTx(types.NewTransaction(block.TxNonce(address), knownContractAddress, uint256.NewInt().SetUint64(1000), 1000000, new(uint256.Int), nil), signer, key)
			if err != nil {
				t.Fatal(err)
			}
			err = contractBackendLonger.SendTransaction(context.Background(), tx)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(tx)
		}
		contractBackendLonger.Commit()
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate longer blocks: %v", err)
	}

	st := state.New(state.NewPlainStateReader(db))
	if !st.Exist(address) {
		t.Error("expected account to exist")
	}

	// BLOCK 1
	exit := make(chan struct{})
	eng := process.NewConsensusProcess(engine, params.AllEthashProtocolChanges, exit)
	defer common.SafeClose(exit)
	if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, engine, eng, blocks[:1], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	// BLOCKS 2
	if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, engine, eng, blocks[1:], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	st = state.New(state.NewDbStateReader(db))
	if !st.Exist(contractAddress) {
		t.Error("expected contractAddress to exist at the block 1", contractAddress.String())
	}

	addrHash := crypto.Keccak256(contractAddress[:])
	var acc accounts.Account
	ok, err := rawdb.ReadAccount(db, common.BytesToHash(addrHash), &acc)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal(errors.New("acc not found"))
	}
	if acc.Incarnation != state.FirstContractIncarnation {
		t.Fatal("wrong incarnation")
	}
	// REORG of block 2 and 3, and insert new (empty) BLOCK 2, 3, and 4
	if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, engine, eng, longerBlocks[1:], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	ok, err = rawdb.ReadAccount(db, common.BytesToHash(addrHash), &acc)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal(errors.New("acc not found"))
	}
	if acc.Incarnation != state.NonContractIncarnation {
		t.Fatal("wrong incarnation", acc.Incarnation)
	}

}

func TestChangeAccountCodeBetweenBlocks(t *testing.T) {
	contract := common.HexToAddress("0x71dd1027069078091B3ca48093B00E4735B20624")

	db := ethdb.NewMemDatabase()
	tds := state.NewTrieDbState(common.Hash{}, db, 0)
	tsw := tds.TrieStateWriter()
	intraBlockState := state.New(tds)
	ctx := context.Background()
	// Start the 1st transaction
	tds.StartNewBuffer()
	intraBlockState.CreateAccount(contract, true)

	oldCode := []byte{0x01, 0x02, 0x03, 0x04}

	intraBlockState.SetCode(contract, oldCode)
	intraBlockState.AddBalance(contract, uint256.NewInt().SetUint64(1000000000))
	if err := intraBlockState.FinalizeTx(ctx, tsw); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}

	tds.ComputeTrieRoots()

	oldCodeHash := common.BytesToHash(crypto.Keccak256(oldCode))

	trieCode, err := tds.ReadAccountCode(contract, 1, oldCodeHash)
	assert.NoError(t, err, "you can receive the new code")
	assert.Equal(t, oldCode, trieCode, "new code should be received")

	tds.StartNewBuffer()

	newCode := []byte{0x04, 0x04, 0x04, 0x04}
	intraBlockState.SetCode(contract, newCode)

	if err = intraBlockState.FinalizeTx(ctx, tsw); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}

	tds.ComputeTrieRoots()

	newCodeHash := common.BytesToHash(crypto.Keccak256(newCode))
	trieCode, err = tds.ReadAccountCode(contract, 1, newCodeHash)
	assert.NoError(t, err, "you can receive the new code")
	assert.Equal(t, newCode, trieCode, "new code should be received")
}

// TestCacheCodeSizeSeparately makes sure that we don't store CodeNodes for code sizes
func TestCacheCodeSizeSeparately(t *testing.T) {
	contract := common.HexToAddress("0x71dd1027069078091B3ca48093B00E4735B20624")
	root := common.HexToHash("0xb939e5bcf5809adfb87ab07f0795b05b95a1d64a90f0eddd0c3123ac5b433854")

	db := ethdb.NewMemDatabase()
	tds := state.NewTrieDbState(root, db, 0)
	tds.SetResolveReads(true)
	intraBlockState := state.New(tds)
	ctx := context.Background()
	// Start the 1st transaction
	tds.StartNewBuffer()
	intraBlockState.CreateAccount(contract, true)

	code := []byte{0x01, 0x02, 0x03, 0x04}

	intraBlockState.SetCode(contract, code)
	intraBlockState.AddBalance(contract, uint256.NewInt().SetUint64(1000000000))
	if err := intraBlockState.FinalizeTx(ctx, tds.TrieStateWriter()); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}
	if err := intraBlockState.CommitBlock(ctx, tds.DbStateWriter()); err != nil {
		t.Errorf("error committing block: %v", err)
	}

	if _, err := tds.ResolveStateTrie(false /* extractWitness */, false /* trace */); err != nil {
		assert.NoError(t, err)
	}

	oldSize := tds.Trie().TrieSize()

	tds.StartNewBuffer()

	codeHash := common.BytesToHash(crypto.Keccak256(code))
	codeSize, err := tds.ReadAccountCodeSize(contract, 1, codeHash)
	assert.NoError(t, err, "you can receive the new code")
	assert.Equal(t, len(code), codeSize, "new code should be received")

	if _, err = tds.ResolveStateTrie(false, false); err != nil {
		assert.NoError(t, err)
	}

	newSize := tds.Trie().TrieSize()
	assert.Equal(t, oldSize, newSize, "should not load codeNode, so the size shouldn't change")

	tds.StartNewBuffer()

	code2, err := tds.ReadAccountCode(contract, 1, codeHash)
	assert.NoError(t, err, "you can receive the new code")
	assert.Equal(t, code, code2, "new code should be received")

	if _, err = tds.ResolveStateTrie(false, false); err != nil {
		assert.NoError(t, err)
	}

	newSize2 := tds.Trie().TrieSize()
	assert.Equal(t, oldSize, newSize2-len(code), "should load codeNode when requesting new data ")
}

// TestCacheCodeSizeInTrie makes sure that we dont just read from the DB all the time
func TestCacheCodeSizeInTrie(t *testing.T) {
	contract := common.HexToAddress("0x71dd1027069078091B3ca48093B00E4735B20624")
	root := common.HexToHash("0xb939e5bcf5809adfb87ab07f0795b05b95a1d64a90f0eddd0c3123ac5b433854")

	db := ethdb.NewMemDatabase()
	tds := state.NewTrieDbState(root, db, 0)
	tds.SetResolveReads(true)
	intraBlockState := state.New(tds)
	ctx := context.Background()
	// Start the 1st transaction
	tds.StartNewBuffer()
	intraBlockState.CreateAccount(contract, true)

	code := []byte{0x01, 0x02, 0x03, 0x04}

	intraBlockState.SetCode(contract, code)
	intraBlockState.AddBalance(contract, uint256.NewInt().SetUint64(1000000000))
	if err := intraBlockState.FinalizeTx(ctx, tds.TrieStateWriter()); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}
	if err := intraBlockState.CommitBlock(ctx, tds.DbStateWriter()); err != nil {
		t.Errorf("error committing block: %v", err)
	}
	if _, err := tds.ResolveStateTrie(false, false); err != nil {
		assert.NoError(t, err)
	}

	tds.StartNewBuffer()

	codeHash := common.BytesToHash(crypto.Keccak256(code))
	codeSize, err := tds.ReadAccountCodeSize(contract, 1, codeHash)
	assert.NoError(t, err, "you can receive the code size ")
	assert.Equal(t, len(code), codeSize, "you can receive the code size")

	if _, err = tds.ResolveStateTrie(false, false); err != nil {
		assert.NoError(t, err)
	}

	assert.NoError(t, db.Delete(dbutils.CodeBucket, codeHash[:], nil), nil)

	codeSize2, err := tds.ReadAccountCodeSize(contract, 1, codeHash)
	assert.NoError(t, err, "you can still receive code size even with empty DB")
	assert.Equal(t, len(code), codeSize2, "code size should be received even with empty DB")
}

func TestRecreateAndRewind(t *testing.T) {
	// Configure and generate a sample block chain
	var (
		db      = ethdb.NewMemDatabase()
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &core.Genesis{
			Config: params.AllEthashProtocolChanges,
			Alloc: core.GenesisAlloc{
				address: {Balance: funds},
			},
		}
		genesis = gspec.MustCommit(db)
		//signer  = types.HomesteadSigner{}
	)

	engine := ethash.NewFaker()
	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackend.Close()
	transactOpts := bind.NewKeyedTransactor(key)
	transactOpts.GasLimit = 1000000
	var revive *contracts.Revive2
	var phoenix *contracts.Phoenix
	var reviveAddress common.Address
	var phoenixAddress common.Address
	var err error

	blocks, _, err1 := core.GenerateChain(gspec.Config, genesis, engine, db, 4, func(i int, block *core.BlockGen) {
		var tx *types.Transaction

		switch i {
		case 0:
			// Deploy phoenix factory
			reviveAddress, tx, revive, err = contracts.DeployRevive2(transactOpts, contractBackend)
			if err != nil {
				panic(err)
			}
			block.AddTx(tx)
		case 1:
			// Calculate the address of the Phoenix and create handle to phoenix contract
			var codeHash common.Hash
			if codeHash, err = common.HashData(common.FromHex(contracts.PhoenixBin)); err != nil {
				panic(err)
			}
			phoenixAddress = crypto.CreateAddress2(reviveAddress, [32]byte{}, codeHash.Bytes())
			if phoenix, err = contracts.NewPhoenix(phoenixAddress, contractBackend); err != nil {
				panic(err)
			}
			// Deploy phoenix
			if tx, err = revive.Deploy(transactOpts, [32]byte{}); err != nil {
				panic(err)
			}
			block.AddTx(tx)
			// Modify phoenix storage
			if tx, err = phoenix.Increment(transactOpts); err != nil {
				panic(err)
			}
			block.AddTx(tx)
			if tx, err = phoenix.Increment(transactOpts); err != nil {
				panic(err)
			}
			block.AddTx(tx)
		case 2:
			// Destruct the phoenix
			if tx, err = phoenix.Die(transactOpts); err != nil {
				panic(err)
			}
			block.AddTx(tx)
		case 3:
			// Recreate the phoenix, and change the storage
			if tx, err = revive.Deploy(transactOpts, [32]byte{}); err != nil {
				panic(err)
			}
			block.AddTx(tx)
			if tx, err = phoenix.Increment(transactOpts); err != nil {
				panic(err)
			}
			block.AddTx(tx)
		}
		contractBackend.Commit()
	}, false /* intermediateHashes */)
	if err1 != nil {
		t.Fatalf("generate blocks: %v", err1)
	}

	contractBackendLonger := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackendLonger.Close()
	transactOptsLonger := bind.NewKeyedTransactor(key)
	transactOptsLonger.GasLimit = 1000000
	longerBlocks, _, err1 := core.GenerateChain(gspec.Config, genesis, engine, db, 5, func(i int, block *core.BlockGen) {
		var tx *types.Transaction

		switch i {
		case 0:
			// Deploy phoenix factory
			reviveAddress, tx, revive, err = contracts.DeployRevive2(transactOptsLonger, contractBackendLonger)
			if err != nil {
				panic(err)
			}
			block.AddTx(tx)
		case 1:
			// Calculate the address of the Phoenix and create handle to phoenix contract
			var codeHash common.Hash
			if codeHash, err = common.HashData(common.FromHex(contracts.PhoenixBin)); err != nil {
				panic(err)
			}
			phoenixAddress = crypto.CreateAddress2(reviveAddress, [32]byte{}, codeHash.Bytes())
			if phoenix, err = contracts.NewPhoenix(phoenixAddress, contractBackendLonger); err != nil {
				panic(err)
			}
			// Deploy phoenix
			if tx, err = revive.Deploy(transactOptsLonger, [32]byte{}); err != nil {
				panic(err)
			}
			block.AddTx(tx)
			// Modify phoenix storage
			if tx, err = phoenix.Increment(transactOptsLonger); err != nil {
				panic(err)
			}
			block.AddTx(tx)
			if tx, err = phoenix.Increment(transactOptsLonger); err != nil {
				panic(err)
			}
			block.AddTx(tx)
		case 2:
			// Destruct the phoenix
			if tx, err = phoenix.Die(transactOptsLonger); err != nil {
				panic(err)
			}
			block.AddTx(tx)
		case 3:
			// Recreate the phoenix, but now with the empty storage
			if tx, err = revive.Deploy(transactOptsLonger, [32]byte{}); err != nil {
				panic(err)
			}
			block.AddTx(tx)
		}
		contractBackendLonger.Commit()
	}, false /* intermediateHashes */)
	if err1 != nil {
		t.Fatalf("generate longer blocks: %v", err1)
	}

	// BLOCKS 1 and 2
	exit := make(chan struct{})
	eng := process.NewConsensusProcess(engine, params.AllEthashProtocolChanges, exit)
	defer common.SafeClose(exit)
	if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, engine, eng, blocks[:2], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	st := state.New(state.NewDbStateReader(db))
	if !st.Exist(phoenixAddress) {
		t.Errorf("expected phoenix %x to exist after first insert", phoenixAddress)
	}

	var key0 common.Hash
	var check0 uint256.Int
	st.GetState(phoenixAddress, &key0, &check0)
	if check0.Cmp(uint256.NewInt().SetUint64(2)) != 0 {
		t.Errorf("expected 0x02 in position 0, got: 0x%x", check0.Bytes())
	}

	// Block 3 and 4
	if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, engine, eng, blocks[2:], true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	st = state.New(state.NewDbStateReader(db))
	if !st.Exist(phoenixAddress) {
		t.Errorf("expected phoenix %x to exist after second insert", phoenixAddress)
	}

	st.GetState(phoenixAddress, &key0, &check0)
	if check0.Cmp(uint256.NewInt().SetUint64(1)) != 0 {
		t.Errorf("expected 0x01 in position 0, got: 0x%x", check0.Bytes())
	}

	// Reorg
	if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, engine, eng, longerBlocks, true /* checkRoot */); err != nil {
		t.Fatal(err)
	}

	st = state.New(state.NewPlainStateReader(db))
	if !st.Exist(phoenixAddress) {
		t.Errorf("expected phoenix %x to exist after second insert", phoenixAddress)
	}

	st.GetState(phoenixAddress, &key0, &check0)
	if check0.Cmp(uint256.NewInt().SetUint64(0)) != 0 {
		t.Errorf("expected 0x00 in position 0, got: 0x%x", check0.Bytes())
	}
}
