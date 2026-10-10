// Copyright 2017 The go-ethereum Authors
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
	"bytes"
	"context"
	"crypto/ecdsa"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	memdb "github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/execution/chain"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/rules/clique"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

func TestCliqueToPoSImport(t *testing.T) {
	accounts := newTesterAccountPool()
	signer := accounts.address("signer")
	config := chainspec.AllCliqueProtocolChanges.Copy()
	config.ChainID = uint256.NewInt(59141)
	config.Clique = &chain.CliqueConfig{Period: 1, Epoch: 30000}
	config.TerminalTotalDifficulty = uint256.NewInt(7)
	genesis := &types.Genesis{
		Config:     config,
		Difficulty: uint256.NewInt(1),
		ExtraData:  make([]byte, clique.ExtraVanity+length.Addr+clique.ExtraSeal),
	}
	copy(genesis.ExtraData[clique.ExtraVanity:], signer[:])
	cliqueEngine := clique.New(config, chainspec.CliqueSnapshot, memdb.NewTestDB(t, dbcfg.ConsensusDB), log.New())
	engine := merge.New(cliqueEngine)
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithEngine(engine))
	blocks, err := blockgen.GenerateChain(config, m.Genesis, cliqueEngine, m.DB, 4, func(i int, gen *blockgen.BlockGen) {
		if i < 3 {
			gen.SetDifficulty(2)
			gen.SetExtra(make([]byte, clique.ExtraVanity+clique.ExtraSeal))
		} else {
			gen.SetDifficulty(0)
			gen.SetExtra(nil)
		}
	})
	require.NoError(t, err)
	for i, block := range blocks.Blocks {
		header := block.Header()
		if i > 0 {
			header.ParentHash = blocks.Blocks[i-1].Hash()
		}
		if i < 3 {
			accounts.sign(header, "signer")
		}
		blocks.Blocks[i] = block.WithSeal(header)
		blocks.Headers[i] = header
	}
	for _, batch := range [][]*types.Block{blocks.Blocks[:2], blocks.Blocks[2:]} {
		status, err := m.InsertBlocks(m.Ctx, batch)
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, status)
		tip := batch[len(batch)-1].Header()
		validation, err := m.ValidateChain(m.Ctx, tip)
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, validation.ValidationStatus, validation.ValidationError)
		result, err := m.UpdateForkChoice(m.Ctx, tip)
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status, result.ValidationError)
	}
	head, err := m.ExecModule.CurrentHeader(m.Ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(4), head.Number.Uint64())
}

type testerAccountPool struct {
	accounts map[string]*ecdsa.PrivateKey
}

func newTesterAccountPool() *testerAccountPool {
	return &testerAccountPool{
		accounts: make(map[string]*ecdsa.PrivateKey),
	}
}

func (ap *testerAccountPool) checkpoint(header *types.Header, signers []string) {
	auths := make(common.Addresses, len(signers))
	for i, signer := range signers {
		auths[i] = ap.address(signer)
	}
	auths.Sort()
	for i, auth := range auths {
		copy(header.Extra[clique.ExtraVanity+i*length.Addr:], auth[:])
	}
}

func (ap *testerAccountPool) address(account string) common.Address {
	if account == "" {
		return common.Address{}
	}
	if ap.accounts[account] == nil {
		ap.accounts[account], _ = crypto.GenerateKey()
	}
	return crypto.PubkeyToAddress(ap.accounts[account].PublicKey)
}

func (ap *testerAccountPool) sign(header *types.Header, signer string) {
	if ap.accounts[signer] == nil {
		ap.accounts[signer], _ = crypto.GenerateKey()
	}
	sealHash := clique.SealHash(header)
	sig, _ := crypto.Sign(sealHash[:], ap.accounts[signer])
	copy(header.Extra[len(header.Extra)-clique.ExtraSeal:], sig)
}

type testerVote struct {
	signer     string
	voted      string
	auth       bool
	checkpoint []string
	newbatch   bool
}

func TestClique(t *testing.T) {
	tests := []struct {
		name    string
		epoch   uint64
		signers []string
		votes   []testerVote
		results []string
		failure error
	}{
		{
			name:    "Single signer, no votes cast",
			signers: []string{"A"},
			votes:   []testerVote{{signer: "A"}},
			results: []string{"A"},
		}, {
			name:    "Single signer, voting to add two others (only accept first, second needs 2 votes)",
			signers: []string{"A"},
			votes: []testerVote{
				{signer: "A", voted: "B", auth: true},
				{signer: "B"},
				{signer: "A", voted: "C", auth: true},
			},
			results: []string{"A", "B"},
		}, {
			name:    "Two signers, voting to add three others (only accept first two, third needs 3 votes already)",
			signers: []string{"A", "B"},
			votes: []testerVote{
				{signer: "A", voted: "C", auth: true},
				{signer: "B", voted: "C", auth: true},
				{signer: "A", voted: "D", auth: true},
				{signer: "B", voted: "D", auth: true},
				{signer: "C"},
				{signer: "A", voted: "E", auth: true},
				{signer: "B", voted: "E", auth: true},
			},
			results: []string{"A", "B", "C", "D"},
		}, {
			name:    "Single signer, dropping itself (weird, but one less cornercase by explicitly allowing this)",
			signers: []string{"A"},
			votes: []testerVote{
				{signer: "A", voted: "A", auth: false},
			},
			results: []string{},
		}, {
			name:    "Two signers, actually needing mutual consent to drop either of them (not fulfilled)",
			signers: []string{"A", "B"},
			votes: []testerVote{
				{signer: "A", voted: "B", auth: false},
			},
			results: []string{"A", "B"},
		}, {
			name:    "Two signers, actually needing mutual consent to drop either of them (fulfilled)",
			signers: []string{"A", "B"},
			votes: []testerVote{
				{signer: "A", voted: "B", auth: false},
				{signer: "B", voted: "B", auth: false},
			},
			results: []string{"A"},
		}, {
			name:    "Three signers, two of them deciding to drop the third",
			signers: []string{"A", "B", "C"},
			votes: []testerVote{
				{signer: "A", voted: "C", auth: false},
				{signer: "B", voted: "C", auth: false},
			},
			results: []string{"A", "B"},
		}, {
			name:    "Four signers, consensus of two not being enough to drop anyone",
			signers: []string{"A", "B", "C", "D"},
			votes: []testerVote{
				{signer: "A", voted: "C", auth: false},
				{signer: "B", voted: "C", auth: false},
			},
			results: []string{"A", "B", "C", "D"},
		}, {
			name:    "Four signers, consensus of three already being enough to drop someone",
			signers: []string{"A", "B", "C", "D"},
			votes: []testerVote{
				{signer: "A", voted: "D", auth: false},
				{signer: "B", voted: "D", auth: false},
				{signer: "C", voted: "D", auth: false},
			},
			results: []string{"A", "B", "C"},
		}, {
			name:    "Authorizations are counted once per signer per target",
			signers: []string{"A", "B"},
			votes: []testerVote{
				{signer: "A", voted: "C", auth: true},
				{signer: "B"},
				{signer: "A", voted: "C", auth: true},
				{signer: "B"},
				{signer: "A", voted: "C", auth: true},
			},
			results: []string{"A", "B"},
		}, {
			name:    "Authorizing multiple accounts concurrently is permitted",
			signers: []string{"A", "B"},
			votes: []testerVote{
				{signer: "A", voted: "C", auth: true},
				{signer: "B"},
				{signer: "A", voted: "D", auth: true},
				{signer: "B"},
				{signer: "A"},
				{signer: "B", voted: "D", auth: true},
				{signer: "A"},
				{signer: "B", voted: "C", auth: true},
			},
			results: []string{"A", "B", "C", "D"},
		}, {
			name:    "Deauthorizations are counted once per signer per target",
			signers: []string{"A", "B"},
			votes: []testerVote{
				{signer: "A", voted: "B", auth: false},
				{signer: "B"},
				{signer: "A", voted: "B", auth: false},
				{signer: "B"},
				{signer: "A", voted: "B", auth: false},
			},
			results: []string{"A", "B"},
		}, {
			name:    "Deauthorizing multiple accounts concurrently is permitted",
			signers: []string{"A", "B", "C", "D"},
			votes: []testerVote{
				{signer: "A", voted: "C", auth: false},
				{signer: "B"},
				{signer: "C"},
				{signer: "A", voted: "D", auth: false},
				{signer: "B"},
				{signer: "C"},
				{signer: "A"},
				{signer: "B", voted: "D", auth: false},
				{signer: "C", voted: "D", auth: false},
				{signer: "A"},
				{signer: "B", voted: "C", auth: false},
			},
			results: []string{"A", "B"},
		}, {
			name:    "Votes from deauthorized signers are discarded immediately (deauth votes)",
			signers: []string{"A", "B", "C"},
			votes: []testerVote{
				{signer: "C", voted: "B", auth: false},
				{signer: "A", voted: "C", auth: false},
				{signer: "B", voted: "C", auth: false},
				{signer: "A", voted: "B", auth: false},
			},
			results: []string{"A", "B"},
		}, {
			name:    "Votes from deauthorized signers are discarded immediately (auth votes)",
			signers: []string{"A", "B", "C"},
			votes: []testerVote{
				{signer: "C", voted: "D", auth: true},
				{signer: "A", voted: "C", auth: false},
				{signer: "B", voted: "C", auth: false},
				{signer: "A", voted: "D", auth: true},
			},
			results: []string{"A", "B"},
		}, {
			name:    "Cascading changes are not allowed, only the account being voted on may change",
			signers: []string{"A", "B", "C", "D"},
			votes: []testerVote{
				{signer: "A", voted: "C", auth: false},
				{signer: "B"},
				{signer: "C"},
				{signer: "A", voted: "D", auth: false},
				{signer: "B", voted: "C", auth: false},
				{signer: "C"},
				{signer: "A"},
				{signer: "B", voted: "D", auth: false},
				{signer: "C", voted: "D", auth: false},
			},
			results: []string{"A", "B", "C"},
		}, {
			name:    "Changes reaching consensus out of bounds (via a deauth) execute on touch",
			signers: []string{"A", "B", "C", "D"},
			votes: []testerVote{
				{signer: "A", voted: "C", auth: false},
				{signer: "B"},
				{signer: "C"},
				{signer: "A", voted: "D", auth: false},
				{signer: "B", voted: "C", auth: false},
				{signer: "C"},
				{signer: "A"},
				{signer: "B", voted: "D", auth: false},
				{signer: "C", voted: "D", auth: false},
				{signer: "A"},
				{signer: "C", voted: "C", auth: true},
			},
			results: []string{"A", "B"},
		}, {
			name:    "Changes reaching consensus out of bounds (via a deauth) may go out of consensus on first touch",
			signers: []string{"A", "B", "C", "D"},
			votes: []testerVote{
				{signer: "A", voted: "C", auth: false},
				{signer: "B"},
				{signer: "C"},
				{signer: "A", voted: "D", auth: false},
				{signer: "B", voted: "C", auth: false},
				{signer: "C"},
				{signer: "A"},
				{signer: "B", voted: "D", auth: false},
				{signer: "C", voted: "D", auth: false},
				{signer: "A"},
				{signer: "B", voted: "C", auth: true},
			},
			results: []string{"A", "B", "C"},
		}, {
			name:    "pending votes don't survive authorization status changes",
			signers: []string{"A", "B", "C", "D", "E"},
			votes: []testerVote{
				{signer: "A", voted: "F", auth: true}, // Authorize F, 3 votes needed
				{signer: "B", voted: "F", auth: true},
				{signer: "C", voted: "F", auth: true},
				{signer: "D", voted: "F", auth: false}, // Deauthorize F, 4 votes needed (leave A's previous vote "unchanged")
				{signer: "E", voted: "F", auth: false},
				{signer: "B", voted: "F", auth: false},
				{signer: "C", voted: "F", auth: false},
				{signer: "D", voted: "F", auth: true}, // Almost authorize F, 2/3 votes needed
				{signer: "E", voted: "F", auth: true},
				{signer: "B", voted: "A", auth: false}, // Deauthorize A, 3 votes needed
				{signer: "C", voted: "A", auth: false},
				{signer: "D", voted: "A", auth: false},
				{signer: "B", voted: "F", auth: true}, // Finish authorizing F, 3/3 votes needed
			},
			results: []string{"B", "C", "D", "E", "F"},
		}, {
			name:    "Epoch transitions reset all votes to allow chain checkpointing",
			epoch:   3,
			signers: []string{"A", "B"},
			votes: []testerVote{
				{signer: "A", voted: "C", auth: true},
				{signer: "B"},
				{signer: "A", checkpoint: []string{"A", "B"}},
				{signer: "B", voted: "C", auth: true},
			},
			results: []string{"A", "B"},
		}, {
			name:    "An unauthorized signer should not be able to sign blocks",
			signers: []string{"A"},
			votes: []testerVote{
				{signer: "B"},
			},
			failure: clique.ErrUnauthorizedSigner,
		}, {
			name:    "An authorized signer that signed recenty should not be able to sign again",
			signers: []string{"A", "B"},
			votes: []testerVote{
				{signer: "A"},
				{signer: "A"},
			},
			failure: clique.ErrRecentlySigned,
		}, {
			name:    "Recent signatures should not reset on checkpoint blocks imported in a batch",
			epoch:   3,
			signers: []string{"A", "B", "C"},
			votes: []testerVote{
				{signer: "A"},
				{signer: "B"},
				{signer: "A", checkpoint: []string{"A", "B", "C"}},
				{signer: "A"},
			},
			failure: clique.ErrRecentlySigned,
		}, {
			name:    "Recent signatures should not reset on checkpoint blocks imported in a new batch",
			epoch:   3,
			signers: []string{"A", "B", "C"},
			votes: []testerVote{
				{signer: "A"},
				{signer: "B"},
				{signer: "A", checkpoint: []string{"A", "B", "C"}},
				{signer: "A", newbatch: true},
			},
			failure: clique.ErrRecentlySigned,
		},
	}
	for i, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			logger := testlog.Logger(t, log.LvlInfo)
			accounts := newTesterAccountPool()

			signers := make([]common.Address, len(tt.signers))
			for j, signer := range tt.signers {
				signers[j] = accounts.address(signer)
			}
			for j := 0; j < len(signers); j++ {
				for k := j + 1; k < len(signers); k++ {
					if bytes.Compare(signers[j][:], signers[k][:]) > 0 {
						signers[j], signers[k] = signers[k], signers[j]
					}
				}
			}
			genesis := &types.Genesis{
				ExtraData: make([]byte, clique.ExtraVanity+length.Addr*len(signers)+clique.ExtraSeal),
				Config:    chainspec.AllCliqueProtocolChanges,
			}
			for j, signer := range signers {
				copy(genesis.ExtraData[clique.ExtraVanity+j*length.Addr:], signer[:])
			}

			config := chainspec.AllCliqueProtocolChanges.Copy()
			config.Clique = &chain.CliqueConfig{
				Period: 1,
				Epoch:  tt.epoch,
			}

			cliqueDB := memdb.NewTestDB(t, dbcfg.ConsensusDB)

			engine := clique.New(config, chainspec.CliqueSnapshot, cliqueDB, log.New())
			engine.FakeDiff = true
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithEngine(engine))

			chain, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, len(tt.votes), func(j int, gen *blockgen.BlockGen) {
				gen.SetCoinbase(accounts.address(tt.votes[j].voted))
				if tt.votes[j].auth {
					var nonce types.BlockNonce
					copy(nonce[:], clique.NonceAuthVote)
					gen.SetNonce(nonce)
				}
			})
			if err != nil {
				t.Fatalf("generate blocks: %v", err)
			}
			for j, block := range chain.Blocks {
				header := block.Header()
				if j > 0 {
					header.ParentHash = chain.Blocks[j-1].Hash()
				}
				header.Extra = make([]byte, clique.ExtraVanity+clique.ExtraSeal)
				if auths := tt.votes[j].checkpoint; auths != nil {
					header.Extra = make([]byte, clique.ExtraVanity+len(auths)*length.Addr+clique.ExtraSeal)
					accounts.checkpoint(header, auths)
				}
				header.Difficulty.SetUint64(clique.DiffInTurn) // Ignored, we just need a valid number

				accounts.sign(header, tt.votes[j].signer)
				chain.Blocks[j] = block.WithSeal(header)
			}
			batches := [][]*types.Block{nil}
			for j, block := range chain.Blocks {
				if tt.votes[j].newbatch {
					batches = append(batches, nil)
				}
				batches[len(batches)-1] = append(batches[len(batches)-1], block)
			}
			failed := false
			for j := 0; j < len(batches)-1; j++ {
				chainX := &blockgen.ChainPack{Blocks: batches[j]}
				chainX.Headers = make([]*types.Header, len(batches[j]))
				for k, b := range batches[j] {
					chainX.Headers[k] = b.Header()
				}
				chainX.TopBlock = batches[j][len(batches[j])-1]
				if err = m.InsertChain(chainX); err != nil {
					t.Errorf("test %d: failed to import batch %d, %v", i, j, err)
					failed = true
					break
				}
			}
			if failed {
				engine.Close()
				return
			}
			chainX := &blockgen.ChainPack{Blocks: batches[len(batches)-1]}
			chainX.Headers = make([]*types.Header, len(batches[len(batches)-1]))
			for k, b := range batches[len(batches)-1] {
				chainX.Headers[k] = b.Header()
			}
			chainX.TopBlock = batches[len(batches)-1][len(batches[len(batches)-1])-1]
			err = m.InsertChain(chainX)
			if tt.failure != nil && err == nil {
				t.Errorf("test %d: expected failure", i)
			}
			if tt.failure == nil && err != nil {
				t.Errorf("test %d: unexpected failure: %v", i, err)
			}
			if tt.failure != nil {
				engine.Close()
				return
			}
			head := chain.Blocks[len(chain.Blocks)-1]

			var snap *clique.Snapshot
			if err := m.DB.View(context.Background(), func(tx kv.Tx) error {
				chainReader := stagedsync.ChainReader{
					Cfg:         config,
					Db:          tx,
					BlockReader: m.BlockReader,
					Logger:      logger,
				}
				snap, err = engine.Snapshot(chainReader, head.NumberU64(), head.Hash(), nil)
				if err != nil {
					return err
				}
				return nil
			}); err != nil {
				t.Errorf("test %d: failed to retrieve voting snapshot %d(%s): %v",
					i, head.NumberU64(), head.Hash().Hex(), err)
				engine.Close()
				return
			}

			signers = make([]common.Address, len(tt.results))
			for j, signer := range tt.results {
				signers[j] = accounts.address(signer)
			}
			for j := 0; j < len(signers); j++ {
				for k := j + 1; k < len(signers); k++ {
					if bytes.Compare(signers[j][:], signers[k][:]) > 0 {
						signers[j], signers[k] = signers[k], signers[j]
					}
				}
			}
			result := snap.GetSigners()
			if len(result) != len(signers) {
				t.Errorf("test %d: signers mismatch: have %x, want %x", i, result, signers)
				engine.Close()
				return
			}
			for j := range result {
				if !bytes.Equal(result[j][:], signers[j][:]) {
					t.Errorf("test %d, signer %d: signer mismatch: have %x, want %x", i, j, result[j], signers[j])
				}
			}
			engine.Close()
		})
	}
}
