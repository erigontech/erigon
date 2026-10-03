// Copyright 2026 The Erigon Authors
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

package app

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
)

func TestExportPinDomainMatrix(t *testing.T) {
	for _, test := range []struct {
		name       string
		variant    string
		registered bool
		forked     bool
		want       kv.Domain
		wantErr    string
	}{
		{name: "hex before fork", variant: "hex", forked: false, want: kv.CommitmentDomain},
		{name: "hex bin before fork", variant: "hex+bin", registered: true, forked: false, want: kv.CommitmentDomain},
		{name: "hex bin after fork", variant: "hex+bin", registered: true, forked: true, want: kv.CommitmentBinDomain},
		{name: "bin after fork", variant: "bin", forked: true, want: kv.CommitmentDomain},
		{name: "bin before fork", variant: "bin", wantErr: "before the binary trie fork"},
		{name: "hex after fork", variant: "hex", forked: true, wantErr: "after the binary trie fork"},
		{name: "hex bin missing bin", variant: "hex+bin", forked: true, wantErr: "no binary commitment domain"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := exportPinDomain(test.variant, test.registered, test.forked)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestSharedExportPinRejectsMissingBlockMapping(t *testing.T) {
	db := newExportPinTestDB(t)
	root := seedState(t, db, 7, [][]byte{addr(0xaa)}, nil)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], 7)
	require.NoError(t, tx.Delete(kv.MaxTxNum, key[:]))
	require.NoError(t, tx.Commit())
	readTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer readTx.Rollback()
	_, err = sharedExportPinWithTxNumReader(t.Context(), readTx, func(uint64) (*types.Header, error) {
		return &types.Header{Root: root}, nil
	}, rawdbv3.TxNums, log.New())
	require.ErrorContains(t, err, "has no txNum mapping")
}

func TestExportPinNamesNextBlockInMidBlockRemedy(t *testing.T) {
	db := newExportPinTestDB(t)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, rawdbv3.TxNums.Append(tx, 4, 7))
	require.NoError(t, tx.Commit())
	readTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer readTx.Rollback()
	err = checkExportPinTxNum(t.Context(), readTx, rawdbv3.TxNums, 4, 6)
	require.ErrorContains(t, err, "--block=5")
}

func TestSharedExportPinUsesTheMappedCheckpoint(t *testing.T) {
	db := newExportPinTestDB(t)
	root := seedState(t, db, 7, [][]byte{addr(0xaa)}, nil)
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	pin, err := sharedExportPinWithTxNumReader(t.Context(), tx, func(block uint64) (*types.Header, error) {
		require.Equal(t, uint64(7), block)
		return &types.Header{Root: root}, nil
	}, rawdbv3.TxNums, log.New())
	require.NoError(t, err)
	require.Equal(t, uint64(7), pin.Block)
	require.Equal(t, uint64(1), pin.TxNum)
	require.Equal(t, kv.CommitmentDomain, pin.Domain)
	require.Equal(t, root, pin.Root)
}

func TestSharedExportPinKeepsV3HexForDualBeforeFork(t *testing.T) {
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousSchema := statecfg.Schema
	previousHash := statecfg.BinCommitmentHash
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.Schema = previousSchema
		statecfg.BinCommitmentHash = previousHash
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = true
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	dirs := datadir.New(t.TempDir())
	refs := false
	variant := state.TrieVariantHexBin
	hash := commitment.PBinHashBlake3
	require.NoError(t, state.WriteErigonDBSettings(dirs, &state.ErigonDBSettings{
		StepSize: 8, StepsInFrozenFile: 1, ReferencesInCommitmentBranches: &refs,
		TrieVariant: &variant, TrieHash: &hash,
	}))
	db := temporaltest.NewTestDB(t, dirs)
	root := seedState(t, db, 7, [][]byte{addr(0xaa)}, nil)
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	pin, err := sharedExportPinWithTxNumReader(t.Context(), tx, func(block uint64) (*types.Header, error) {
		require.Equal(t, uint64(7), block)
		return &types.Header{Root: root}, nil
	}, rawdbv3.TxNums, log.New())
	require.NoError(t, err)
	require.Equal(t, kv.CommitmentDomain, pin.Domain)
	require.Equal(t, commitment.VariantCommitmentV3, pin.Variant)
}

func TestSharedExportPinUsesBinAfterFork(t *testing.T) {
	for _, variant := range []string{state.TrieVariantBin, state.TrieVariantHexBin} {
		t.Run(variant, func(t *testing.T) {
			previousBin := statecfg.ExperimentalBinCommitment
			previousHexBin := statecfg.ExperimentalHexBinCommitment
			previousV3 := statecfg.ExperimentalCommitmentV3
			previousSchema := statecfg.Schema
			previousHash := statecfg.BinCommitmentHash
			previousSuite := commitment.PBinHashSuiteName()
			t.Cleanup(func() {
				statecfg.ExperimentalBinCommitment = previousBin
				statecfg.ExperimentalHexBinCommitment = previousHexBin
				statecfg.ExperimentalCommitmentV3 = previousV3
				statecfg.Schema = previousSchema
				statecfg.BinCommitmentHash = previousHash
				require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
			})
			statecfg.ExperimentalBinCommitment = true
			statecfg.ExperimentalHexBinCommitment = variant == state.TrieVariantHexBin
			statecfg.ExperimentalCommitmentV3 = variant == state.TrieVariantHexBin
			if statecfg.ExperimentalCommitmentV3 {
				statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
			}
			statecfg.BinCommitmentHash = commitment.PBinHashBlake3
			require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
			db, root := newBinExportPinDB(t, variant)
			tx, err := db.BeginTemporalRo(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			pin, err := sharedExportPinWithTxNumReader(t.Context(), tx, func(block uint64) (*types.Header, error) {
				require.Equal(t, uint64(7), block)
				return &types.Header{Time: 10, Root: root}, nil
			}, rawdbv3.TxNums, log.New())
			require.NoError(t, err)
			wantDomain := kv.CommitmentBinDomain
			if variant == state.TrieVariantBin {
				wantDomain = kv.CommitmentDomain
			}
			require.Equal(t, wantDomain, pin.Domain)
			require.Equal(t, commitment.VariantBinPatriciaTrie, pin.Variant)
		})
	}
}

func newExportPinTestDB(t *testing.T) kv.TemporalRwDB {
	return temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
}

func newBinExportPinDB(t *testing.T, variant string) (kv.TemporalRwDB, common.Hash) {
	dirs := datadir.New(t.TempDir())
	refs := false
	hash := commitment.PBinHashBlake3
	require.NoError(t, state.WriteErigonDBSettings(dirs, &state.ErigonDBSettings{
		StepSize: 8, StepsInFrozenFile: 1, ReferencesInCommitmentBranches: &refs,
		TrieVariant: &variant, TrieHash: &hash,
	}))
	db := temporaltest.NewTestDB(t, dirs)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	genesisHash := common.Hash{0x42}
	forkTime := uint64(10)
	require.NoError(t, rawdb.WriteCanonicalHash(tx, genesisHash, 0))
	require.NoError(t, rawdb.WriteChainConfig(tx, genesisHash, &chain.Config{BinaryTrieTime: &forkTime}))
	require.NoError(t, rawdbv3.TxNums.Append(tx, 7, 1))
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantBinPatriciaTrie
	cfg.EnableTrieWarmup = false
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithTrieConfig(cfg))
	require.NoError(t, err)
	defer domains.Close()
	feed := &commitment.PBinFeed{Accounts: []commitment.PBinFeedAccount{{
		Address: bytes.Repeat([]byte{0xaa}, 20), Exists: true, CodeWritten: true,
		Balance: *uint256.NewInt(1), CodeHash: common.Hash{},
	}}}
	binCtx := domains.GetCommitmentCtxForDomain(kv.CommitmentBinDomain)
	if variant == state.TrieVariantBin {
		binCtx = domains.GetCommitmentContext()
	}
	binCtx.SetPBinFeed(feed)
	root, err := binCtx.ComputeCommitment(t.Context(), tx, true, 7, 1, "export-pin-test", nil)
	require.NoError(t, err)
	require.NoError(t, domains.Flush(t.Context(), tx))
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 7))
	require.NoError(t, tx.Commit())
	return db, common.BytesToHash(root)
}
