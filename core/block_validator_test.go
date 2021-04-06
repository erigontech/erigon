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
	"time"

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
			var results <-chan error

			if valid {
				engine := ethash.NewFaker()
				_, results = engine.VerifyHeaders(stagedsync.ChainReader{params.TestChainConfig, db}, []*types.Header{headers[i]}, []bool{true})
			} else {
				engine := ethash.NewFakeFailer(headers[i].Number.Uint64())
				_, results = engine.VerifyHeaders(stagedsync.ChainReader{params.TestChainConfig, db}, []*types.Header{headers[i]}, []bool{true})
			}
			// Wait for the verification result
			select {
			case result := <-results:
				if (result == nil) != valid {
					t.Errorf("test %d.%d: validity mismatch: have %v, want %v", i, j, result, valid)
				}
			case <-time.After(time.Second):
				t.Fatalf("test %d.%d: verification timeout", i, j)
			}
			// Make sure no more data is returned
			select {
			case result := <-results:
				t.Fatalf("test %d.%d: unexpected result returned: %v", i, j, result)
			case <-time.After(25 * time.Millisecond):
			}
		}
		engine := ethash.NewFaker()
		if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, params.TestChainConfig, &vm.Config{}, engine, blocks[i:i+1], true /* checkRoot */); err != nil {
			t.Fatalf("test %d: error inserting the block: %v", i, err)
		}
	}
}

// Tests that concurrent header verification works, for both good and bad blocks.
func TestHeaderConcurrentVerification2(t *testing.T)  { testHeaderConcurrentVerification(t, 2) }
func TestHeaderConcurrentVerification8(t *testing.T)  { testHeaderConcurrentVerification(t, 8) }
func TestHeaderConcurrentVerification32(t *testing.T) { testHeaderConcurrentVerification(t, 32) }

func testHeaderConcurrentVerification(t *testing.T, threads int) {
	// Create a simple chain to verify
	testdb := ethdb.NewMemDatabase()
	defer testdb.Close()
	var (
		gspec          = &core.Genesis{Config: params.TestChainConfig}
		genesis        = gspec.MustCommit(testdb)
		blocks, _, err = core.GenerateChain(params.TestChainConfig, genesis, ethash.NewFaker(), testdb, 8, nil, false /* intemediateHashes */)
	)
	if err != nil {
		t.Fatalf("genetate chain: %v", err)
	}
	headers := make([]*types.Header, len(blocks))
	seals := make([]bool, len(blocks))

	for i, block := range blocks {
		headers[i] = block.Header()
		seals[i] = true
	}

	// Run the header checker for the entire block chain at once both for a valid and
	// also an invalid chain (enough if one arbitrary block is invalid).
	for i, valid := range []bool{true, false} {
		var results <-chan error

		if valid {
			_, results = ethash.NewFaker().VerifyHeaders(stagedsync.ChainReader{params.TestChainConfig, testdb}, headers, seals)
		} else {
			_, results = ethash.NewFakeFailer(uint64(len(headers)-1)).VerifyHeaders(stagedsync.ChainReader{params.TestChainConfig, testdb}, headers, seals)
		}
		// Wait for all the verification results
		checks := make(map[int]error)
		for j := 0; j < len(blocks); j++ {
			select {
			case result := <-results:
				checks[j] = result

			case <-time.After(time.Second):
				t.Fatalf("test %d.%d: verification timeout", i, j)
			}
		}
		// Check nonce check validity
		for j := 0; j < len(blocks); j++ {
			want := valid || (j < len(blocks)-2) // We chose the last-but-one nonce in the chain to fail
			if (checks[j] == nil) != want {
				t.Errorf("test %d.%d: validity mismatch: have %v, want %v", i, j, checks[j], want)
			}
			if !want {
				// A few blocks after the first error may pass verification due to concurrent
				// workers. We don't care about those in this test, just that the correct block
				// errors out.
				break
			}
		}
		// Make sure no more data is returned
		select {
		case result := <-results:
			t.Fatalf("test %d: unexpected result returned: %v", i, result)
		case <-time.After(25 * time.Millisecond):
		}
	}
}

// Tests that aborting a header validation indeed prevents further checks from being
// run, as well as checks that no left-over goroutines are leaked.
func TestHeaderConcurrentAbortion2(t *testing.T)  { testHeaderConcurrentAbortion(t, 2) }
func TestHeaderConcurrentAbortion8(t *testing.T)  { testHeaderConcurrentAbortion(t, 8) }
func TestHeaderConcurrentAbortion32(t *testing.T) { testHeaderConcurrentAbortion(t, 32) }

func testHeaderConcurrentAbortion(t *testing.T, threads int) {
	// Create a simple chain to verify
	testdb := ethdb.NewMemDatabase()
	defer testdb.Close()

	var (
		gspec          = &core.Genesis{Config: params.TestChainConfig}
		genesis        = gspec.MustCommit(testdb)
		blocks, _, err = core.GenerateChain(params.TestChainConfig, genesis, ethash.NewFaker(), testdb, 1024, nil, false /* intemediateHashes */)
	)
	if err != nil {
		t.Fatalf("genetate chain: %v", err)
	}
	headers := make([]*types.Header, len(blocks))
	seals := make([]bool, len(blocks))

	for i, block := range blocks {
		headers[i] = block.Header()
		seals[i] = true
	}

	cancel, results := ethash.NewFakeDelayer(time.Millisecond).VerifyHeaders(stagedsync.ChainReader{params.TestChainConfig, testdb}, headers, seals)
	cancel()

	// Deplete the results channel
	verified := 0
	for depleted := false; !depleted; {
		select {
		case result, ok := <-results:
			if !ok {
				// the channel is closed
				depleted = true
			} else {
				if result != nil {
					t.Errorf("header %d: validation failed: %v", verified, result)
				}
				verified++
			}
		case <-time.After(50 * time.Millisecond):
			depleted = true
		}
	}
	// Check that abortion was honored by not processing too many POWs
	if verified > 2*threads {
		t.Errorf("verification count too large: have %d, want below %d", verified, 2*threads)
	}
}
