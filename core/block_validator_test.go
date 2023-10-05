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
	"context"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/stages/mock"
)

// Tests that simple header verification works, for both good and bad blocks.
func TestHeaderVerification(t *testing.T) {
	// Create a simple chain to verify
	var (
		gspec  = &types.Genesis{Config: params.TestChainConfig}
		engine = ethash.NewFaker()
	)
	checkStateRoot := true
	m := mock.MockWithGenesisEngine(t, gspec, engine, false, checkStateRoot)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 8, nil)
	if err != nil {
		t.Fatalf("genetate chain: %v", err)
	}
	// Run the header checker for blocks one-by-one, checking for both valid and invalid nonces
	for i := 0; i < chain.Length(); i++ {
		if err := m.DB.View(context.Background(), func(tx kv.Tx) error {
			for j, valid := range []bool{true, false} {
				if valid {
					engine := ethash.NewFaker()
					err = engine.VerifyHeader(stagedsync.ChainReader{Cfg: *params.TestChainConfig, Db: tx, BlockReader: m.BlockReader}, chain.Headers[i], true)
				} else {
					engine := ethash.NewFakeFailer(chain.Headers[i].Number.Uint64())
					err = engine.VerifyHeader(stagedsync.ChainReader{Cfg: *params.TestChainConfig, Db: tx, BlockReader: m.BlockReader}, chain.Headers[i], true)
				}
				if (err == nil) != valid {
					t.Errorf("test %d.%d: validity mismatch: have %v, want %v", i, j, err, valid)
				}
			}
			return nil
		}); err != nil {
			panic(err)
		}
		if err = m.InsertChain(chain.Slice(i, i+1)); err != nil {
			t.Fatalf("test %d: error inserting the block: %v", i, err)
		}

		engine.Close()
	}
}

// Tests that simple header with seal verification works, for both good and bad blocks.
func TestHeaderWithSealVerification(t *testing.T) {
	// Create a simple chain to verify
	var (
		gspec  = &types.Genesis{Config: params.TestChainAuraConfig}
		engine = ethash.NewFaker()
	)
	checkStateRoot := true
	m := mock.MockWithGenesisEngine(t, gspec, engine, false, checkStateRoot)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 8, nil)
	if err != nil {
		t.Fatalf("genetate chain: %v", err)
	}

	// Run the header checker for blocks one-by-one, checking for both valid and invalid nonces
	for i := 0; i < chain.Length(); i++ {
		if err := m.DB.View(context.Background(), func(tx kv.Tx) error {
			for j, valid := range []bool{true, false} {
				if valid {
					engine := ethash.NewFaker()
					err = engine.VerifyHeader(stagedsync.ChainReader{Cfg: *params.TestChainAuraConfig, Db: tx, BlockReader: m.BlockReader}, chain.Headers[i], true)
				} else {
					engine := ethash.NewFakeFailer(chain.Headers[i].Number.Uint64())
					err = engine.VerifyHeader(stagedsync.ChainReader{Cfg: *params.TestChainAuraConfig, Db: tx, BlockReader: m.BlockReader}, chain.Headers[i], true)
				}
				if (err == nil) != valid {
					t.Errorf("test %d.%d: validity mismatch: have %v, want %v", i, j, err, valid)
				}
			}
			return nil
		}); err != nil {
			panic(err)
		}
		if err = m.InsertChain(chain.Slice(i, i+1)); err != nil {
			t.Fatalf("test %d: error inserting the block: %v", i, err)
		}

		engine.Close()
	}
}
