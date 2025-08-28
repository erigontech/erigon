// Copyright 2019 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package clique_test

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain/params"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/consensus/clique"
	"github.com/erigontech/erigon/execution/stages/mock"
	"github.com/erigontech/erigon/execution/types"
)

// This test case is a repro of an annoying bug that took us forever to catch.
// In Clique PoA networks (e.g. Görli), consecutive blocks might have
// the same state root (no block subsidy, empty block). If a node crashes, the
// chain ends up losing the recent state and needs to regenerate it from blocks
// already in the database. The bug was that processing the block *prior* to an
// empty one **also completes** the empty one, ending up in a known-block error.
func TestReimportMirroredState(t *testing.T) {
	// Initialize a Clique chain with a single signer
	var (
		cliqueDB = memdb.NewTestDB(t, kv.ConsensusDB)
		key, _   = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		addr     = crypto.PubkeyToAddress(key.PublicKey)
		engine   = clique.New(chainspec.AllCliqueProtocolChanges, chainspec.CliqueSnapshot, cliqueDB, log.New())
		signer   = types.LatestSignerForChainID(nil)
	)
	genspec := &types.Genesis{
		ExtraData: make([]byte, clique.ExtraVanity+length.Addr+clique.ExtraSeal),
		Alloc: map[common.Address]types.GenesisAccount{
			addr: {Balance: big.NewInt(10000000000000000)},
		},
		Config: chainspec.AllCliqueProtocolChanges,
	}
	copy(genspec.ExtraData[clique.ExtraVanity:], addr[:])
	m := mock.MockWithGenesisEngine(t, genspec, engine, false)

	// Generate a batch of blocks, each properly signed
	getHeader := func(hash common.Hash, number uint64) (h *types.Header, err error) {
		err = m.DB.View(m.Ctx, func(tx kv.Tx) (err error) {
			h, err = m.BlockReader.Header(m.Ctx, tx, hash, number)
			return err
		})
		return h, err
	}

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 3, func(i int, block *core.BlockGen) {
		// The chain maker doesn't have access to a chain, so the difficulty will be
		// lets unset (nil). Set it here to the correct value.
		block.SetDifficulty(clique.DiffInTurn)

		// We want to simulate an empty middle block, having the same state as the
		// first one. The last is needs a state change again to force a reorg.
		if i != 1 {
			baseFee, _ := uint256.FromBig(block.GetHeader().BaseFee)
			tx, err := types.SignTx(types.NewTransaction(block.TxNonce(addr), common.Address{0x00}, new(uint256.Int), params.TxGas, baseFee, nil), *signer, key)
			if err != nil {
				panic(err)
			}
			block.AddTxWithChain(getHeader, engine, tx)
		}
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	for i, block := range chain.Blocks {
		header := block.Header()
		if i > 0 {
			header.ParentHash = chain.Blocks[i-1].Hash()
		}
		header.Extra = make([]byte, clique.ExtraVanity+clique.ExtraSeal)
		header.Difficulty = clique.DiffInTurn

		sig, _ := crypto.Sign(clique.SealHash(header).Bytes(), key)
		copy(header.Extra[len(header.Extra)-clique.ExtraSeal:], sig)
		chain.Headers[i] = header
		chain.Blocks[i] = block.WithSeal(header)
	}

	// Insert the first two blocks and make sure the chain is valid
	if err := m.InsertChain(chain.Slice(0, 2)); err != nil {
		t.Fatalf("failed to insert initial blocks: %v", err)
	}
	if err := m.DB.View(m.Ctx, func(tx kv.Tx) error {
		if head, err1 := m.BlockReader.BlockByHash(m.Ctx, tx, rawdb.ReadHeadHeaderHash(tx)); err1 != nil {
			t.Errorf("could not read chain head: %v", err1)
		} else if head.NumberU64() != 2 {
			t.Errorf("chain head mismatch: have %d, want %d", head.NumberU64(), 2)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash by creating a new chain on top of the database, without
	// flushing the dirty states out. Insert the last block, triggering a sidechain
	// reimport.
	if err := m.InsertChain(chain.Slice(2, chain.Length())); err != nil {
		t.Fatalf("failed to insert final block: %v", err)
	}
	if err := m.DB.View(m.Ctx, func(tx kv.Tx) error {
		if head, err1 := m.BlockReader.CurrentBlock(tx); err1 != nil {
			t.Errorf("could not read chain head: %v", err1)
		} else if head.NumberU64() != 3 {
			t.Errorf("chain head mismatch: have %d, want %d", head.NumberU64(), 3)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

}
