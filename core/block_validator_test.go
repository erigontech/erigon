// Copyright 2015 The go-ethereum Authors
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

package core_test

import (
	"context"
	"testing"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/testlog"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/db/kv"
	libchain "github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/consensus/ethash"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stages/mock"
	"github.com/erigontech/erigon/execution/types"
)

// Tests that simple header verification works, for both good and bad blocks.
func TestHeaderVerification(t *testing.T) {
	t.Parallel()
	// Create a simple chain to verify
	var (
		gspec  = &types.Genesis{Config: libchain.TestChainConfig}
		engine = ethash.NewFaker()
	)
	logger := testlog.Logger(t, log.LvlInfo)
	m := mock.MockWithGenesisEngine(t, gspec, engine, false)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 8, nil)
	if err != nil {
		t.Fatalf("genetate chain: %v", err)
	}
	// Run the header checker for blocks one-by-one, checking for both valid and invalid nonces
	for i := 0; i < chain.Length(); i++ {
		if err := m.DB.View(context.Background(), func(tx kv.Tx) error {
			for j, valid := range []bool{true, false} {
				chainReader := stagedsync.ChainReader{
					Cfg:         libchain.TestChainConfig,
					Db:          tx,
					BlockReader: m.BlockReader,
					Logger:      logger,
				}
				var engine consensus.Engine
				if valid {
					engine = ethash.NewFaker()
				} else {
					engine = ethash.NewFakeFailer(chain.Headers[i].Number.Uint64())
				}
				err = engine.VerifyHeader(chainReader, chain.Headers[i], true)
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
	t.Parallel()
	// Create a simple chain to verify
	var (
		gspec  = &types.Genesis{Config: libchain.TestChainAuraConfig}
		engine = ethash.NewFaker()
	)
	logger := testlog.Logger(t, log.LvlInfo)
	m := mock.MockWithGenesisEngine(t, gspec, engine, false)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 8, nil)
	if err != nil {
		t.Fatalf("genetate chain: %v", err)
	}

	// Run the header checker for blocks one-by-one, checking for both valid and invalid nonces
	for i := 0; i < chain.Length(); i++ {
		if err := m.DB.View(context.Background(), func(tx kv.Tx) error {
			for j, valid := range []bool{true, false} {
				chainReader := stagedsync.ChainReader{
					Cfg:         libchain.TestChainAuraConfig,
					Db:          tx,
					BlockReader: m.BlockReader,
					Logger:      logger,
				}
				var engine consensus.Engine
				if valid {
					engine = ethash.NewFaker()
				} else {
					engine = ethash.NewFakeFailer(chain.Headers[i].Number.Uint64())
				}
				err = engine.VerifyHeader(chainReader, chain.Headers[i], true)
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
