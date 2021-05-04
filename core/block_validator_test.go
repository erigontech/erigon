// Copyright 2015 The go-ethereum Authors
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

package core_test

import (
	"testing"

	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

// Tests that simple header verification works, for both good and bad blocks.
func TestHeaderVerification(t *testing.T) {
	// Create a simple chain to verify
	db := ethdb.NewMemDatabase()
	defer db.Close()
	var (
		gspec   = &core.Genesis{Config: params.TestChainConfig}
		genesis = gspec.MustCommit(db)
		engine  = ethash.NewFaker()
	)

	blocks, _, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db, 8, nil, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("genetate chain: %v", err)
	}

	headers := make([]*types.Header, len(blocks))
	for i, block := range blocks {
		headers[i] = block.Header()
	}
	// Run the header checker for blocks one-by-one, checking for both valid and invalid nonces
	for i := 0; i < len(blocks); i++ {
		for j, valid := range []bool{true, false} {
			if valid {
				engine := ethash.NewFaker()
				err = engine.VerifyHeaders(stagedsync.ChainReader{Cfg: params.TestChainConfig, Db: db}, []*types.Header{headers[i]}, []bool{true})
			} else {
				engine := ethash.NewFakeFailer(headers[i].Number.Uint64())
				err = engine.VerifyHeaders(stagedsync.ChainReader{Cfg: params.TestChainConfig, Db: db}, []*types.Header{headers[i]}, []bool{true})
			}
			if (err == nil) != valid {
				t.Errorf("test %d.%d: validity mismatch: have %v, want %v", i, j, err, valid)
			}
		}
		engine := ethash.NewFaker()
		if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, params.TestChainConfig, &vm.Config{}, engine, blocks[i:i+1], true /* checkRoot */); err != nil {
			t.Fatalf("test %d: error inserting the block: %v", i, err)
		}

		engine.Close()
	}
}
