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

package clique

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/process"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

// This test case is a repro of an annoying bug that took us forever to catch.
// In Clique PoA networks (Rinkeby, Görli, etc), consecutive blocks might have
// the same state root (no block subsidy, empty block). If a node crashes, the
// chain ends up losing the recent state and needs to regenerate it from blocks
// already in the database. The bug was that processing the block *prior* to an
// empty one **also completes** the empty one, ending up in a known-block error.
func TestReimportMirroredState(t *testing.T) {
	// Initialize a Clique chain with a single signer
	var (
		db     = ethdb.NewMemDatabase()
		key, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		addr   = crypto.PubkeyToAddress(key.PublicKey)
		engine = New(params.AllCliqueProtocolChanges.Clique, db)
		signer = new(types.HomesteadSigner)
	)
	genspec := &core.Genesis{
		ExtraData: make([]byte, extraVanity+common.AddressLength+extraSeal),
		Alloc: map[common.Address]core.GenesisAccount{
			addr: {Balance: big.NewInt(1)},
		},
	}
	copy(genspec.ExtraData[extraVanity:], addr[:])
	genesis := genspec.MustCommit(db)

	// Generate a batch of blocks, each properly signed
	txCacher := core.NewTxSenderCacher(1)
	chain, _ := core.NewBlockChain(db, nil, params.AllCliqueProtocolChanges, engine, vm.Config{}, nil, txCacher)
	defer chain.Stop()

	blocks, _, err := core.GenerateChain(params.AllCliqueProtocolChanges, genesis, engine, db, 3, func(i int, block *core.BlockGen) {
		// The chain maker doesn't have access to a chain, so the difficulty will be
		// lets unset (nil). Set it here to the correct value.
		block.SetDifficulty(diffInTurn)

		// We want to simulate an empty middle block, having the same state as the
		// first one. The last is needs a state change again to force a reorg.
		if i != 1 {
			tx, err := types.SignTx(types.NewTransaction(block.TxNonce(addr), common.Address{0x00}, new(uint256.Int), params.TxGas, nil, nil), signer, key)
			if err != nil {
				panic(err)
			}
			block.AddTxWithChain(chain, tx)
		}
	}, false /* intermediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	for i, block := range blocks {
		header := block.Header()
		if i > 0 {
			header.ParentHash = blocks[i-1].Hash()
		}
		header.Extra = make([]byte, extraVanity+extraSeal)
		header.Difficulty = diffInTurn

		sig, _ := crypto.Sign(SealHash(header).Bytes(), key)
		copy(header.Extra[len(header.Extra)-extraSeal:], sig)
		blocks[i] = block.WithSeal(header)
	}
	// Insert the first two blocks and make sure the chain is valid
	db = ethdb.NewMemDatabase()
	genspec.MustCommit(db)

	exit := make(chan struct{})
	eng := process.NewConsensusProcess(NewCliqueVerifier(engine), params.AllEthashProtocolChanges, exit)
	defer common.SafeClose(exit)
	if _, err := stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, params.AllCliqueProtocolChanges, &vm.Config{}, engine, eng, blocks[:2], true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert initial blocks: %v", err)
	}
	if head, err1 := rawdb.ReadBlockByHash(db, rawdb.ReadHeadHeaderHash(db)); err1 != nil {
		t.Errorf("could not read chain head: %v", err1)
	} else if head.NumberU64() != 2 {
		t.Errorf("chain head mismatch: have %d, want %d", head.NumberU64(), 2)
	}

	// Simulate a crash by creating a new chain on top of the database, without
	// flushing the dirty states out. Insert the last block, triggering a sidechain
	// reimport.
	txCacher2 := core.NewTxSenderCacher(1)
	chain2, _ := core.NewBlockChain(db, nil, params.AllCliqueProtocolChanges, engine, vm.Config{}, nil, txCacher2)
	defer chain2.Stop()

	if _, err := stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, params.AllCliqueProtocolChanges, &vm.Config{}, engine, eng, blocks[2:], true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert final block: %v", err)
	}
	if head, err1 := rawdb.ReadBlockByHash(db, rawdb.ReadHeadHeaderHash(db)); err1 != nil {
		t.Errorf("could not read chain head: %v", err1)
	} else if head.NumberU64() != 3 {
		t.Errorf("chain head mismatch: have %d, want %d", head.NumberU64(), 3)
	}
}
