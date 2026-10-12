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

package genesiswrite_test

import (
	"fmt"
	"io/fs"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	pbt "github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/internal/commitmenttest/commitmentflags"
)

func withBinCommitment(t *testing.T, on bool) {
	t.Helper()
	commitmentflags.Restore(t)
	statecfg.ExperimentalBinCommitment = on
	if !on {
		// The hash goes with the flag. A run under COMMITMENT_BIN_HASH leaves it set
		// otherwise, and the resolver refuses a hash without the trie it names.
		statecfg.BinCommitmentHash = ""
	}
}

func withCommitmentVariant(t *testing.T, bin, hexBin bool) {
	t.Helper()
	withBinCommitment(t, bin)
	commitmentflags.Restore(t)
	statecfg.ExperimentalHexBinCommitment, statecfg.BinCommitmentHash = hexBin, ""
	statecfg.ExperimentalCommitmentV3 = hexBin
	if hexBin {
		statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	}
}

func pbinTestGenesis() *types.Genesis {
	return &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			common.HexToAddress("0x0000000000000000000000000000000000000042"): {Balance: big.NewInt(1)},
			common.HexToAddress("0x00000000000000000000000000000000000000ff"): {Balance: big.NewInt(0xdeadbeef), Nonce: 3},
		},
	}
}

func delayedPBinGenesis() *types.Genesis {
	amsterdamTime := uint64(10)
	binaryTrieTime := uint64(20)
	cfg := chain.AllProtocolChanges.Copy()
	cfg.AmsterdamTime = &amsterdamTime
	cfg.BinaryTrieTime = &binaryTrieTime
	g := pbinTestGenesis()
	g.Config = cfg
	g.Timestamp = amsterdamTime
	return g
}

func TestPBinGenesisDelayedScheduleRequiresDualDatadir(t *testing.T) {
	for _, tc := range []struct {
		name    string
		bin     bool
		hexBin  bool
		wantErr bool
	}{
		{name: "hex", wantErr: true},
		{name: "bin", bin: true, wantErr: true},
		{name: "hex+bin", bin: true, hexBin: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withCommitmentVariant(t, tc.bin, tc.hexBin)
			_, _, err := genesiswrite.GenesisToBlock(delayedPBinGenesis(), datadir.New(t.TempDir()), log.New())
			if tc.wantErr {
				require.ErrorContains(t, err, "binaryTrieTime")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestPBinGenesisRejectsBinaryTrieBeforeAmsterdam(t *testing.T) {
	withCommitmentVariant(t, true, true)
	g := delayedPBinGenesis()
	binaryTrieTime := uint64(5)
	g.Config.BinaryTrieTime = &binaryTrieTime
	_, _, err := genesiswrite.GenesisToBlock(g, datadir.New(t.TempDir()), log.New())
	require.ErrorContains(t, err, "needs amsterdamTime")
}

func TestBinaryTrieOverrideNeedsDualDatadir(t *testing.T) {
	for _, tc := range []struct {
		name    string
		hexBin  bool
		wantErr bool
	}{
		{name: "hex", wantErr: true},
		{name: "hex+bin", hexBin: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withCommitmentVariant(t, tc.hexBin, tc.hexBin)
			dirs := datadir.New(t.TempDir())
			_, err := dbstate.ResolveErigonDBSettings(dirs, log.New(), true)
			require.NoError(t, err)
			db := temporaltest.NewTestDB(t, dirs)
			g := delayedPBinGenesis()
			g.Config.BinaryTrieTime = nil
			_, _, err = genesiswrite.CommitGenesisBlockWithOverride(db, g, "", nil, nil, nil, false, dirs, log.New())
			require.NoError(t, err)

			binaryTrieTime := uint64(20)
			_, _, err = genesiswrite.CommitGenesisBlockWithOverride(db, g, "", nil, nil, &binaryTrieTime, false, dirs, log.New())
			if tc.wantErr {
				require.ErrorContains(t, err, "binaryTrieTime")
				require.Nil(t, readStoredChainConfig(t, db).BinaryTrieTime)
				return
			}
			require.NoError(t, err)
			require.Equal(t, &binaryTrieTime, readStoredChainConfig(t, db).BinaryTrieTime)

			postponed := uint64(30)
			_, _, err = genesiswrite.CommitGenesisBlockWithOverride(db, g, "", nil, nil, &postponed, false, dirs, log.New())
			require.NoError(t, err)
			require.Equal(t, &postponed, readStoredChainConfig(t, db).BinaryTrieTime)
		})
	}
}

func TestBinaryTrieOverrideMustFollowGenesis(t *testing.T) {
	withCommitmentVariant(t, true, true)
	g := delayedPBinGenesis()
	g.Config.BinaryTrieTime = nil
	atGenesis := g.Timestamp

	t.Run("fresh", func(t *testing.T) {
		dirs := datadir.New(t.TempDir())
		db := temporaltest.NewTestDB(t, dirs)
		_, _, err := genesiswrite.CommitGenesisBlockWithOverride(db, g, "", nil, nil, &atGenesis, false, dirs, log.New())
		require.ErrorContains(t, err, "is not after the genesis timestamp")
	})
	t.Run("existing", func(t *testing.T) {
		dirs := datadir.New(t.TempDir())
		_, err := dbstate.ResolveErigonDBSettings(dirs, log.New(), true)
		require.NoError(t, err)
		db := temporaltest.NewTestDB(t, dirs)
		_, _, err = genesiswrite.CommitGenesisBlockWithOverride(db, g, "", nil, nil, nil, false, dirs, log.New())
		require.NoError(t, err)
		_, _, err = genesiswrite.CommitGenesisBlockWithOverride(db, g, "", nil, nil, &atGenesis, false, dirs, log.New())
		require.ErrorContains(t, err, "is not after the genesis timestamp")
		require.Nil(t, readStoredChainConfig(t, db).BinaryTrieTime)
	})
}

func TestBinaryTrieOverrideKeepsAGenesisBinaryTrie(t *testing.T) {
	withCommitmentVariant(t, true, true)
	g := delayedPBinGenesis()
	atGenesis := g.Timestamp
	g.Config.BinaryTrieTime = &atGenesis
	later := uint64(20)

	t.Run("fresh", func(t *testing.T) {
		dirs := datadir.New(t.TempDir())
		db := temporaltest.NewTestDB(t, dirs)
		_, _, err := genesiswrite.CommitGenesisBlockWithOverride(db, g, "", nil, nil, &later, false, dirs, log.New())
		require.ErrorContains(t, err, "starts at genesis")
	})
	t.Run("existing", func(t *testing.T) {
		dirs := datadir.New(t.TempDir())
		_, err := dbstate.ResolveErigonDBSettings(dirs, log.New(), true)
		require.NoError(t, err)
		db := temporaltest.NewTestDB(t, dirs)
		_, _, err = genesiswrite.CommitGenesisBlockWithOverride(db, g, "", nil, nil, nil, false, dirs, log.New())
		require.NoError(t, err)
		_, _, err = genesiswrite.CommitGenesisBlockWithOverride(db, g, "", nil, nil, &later, false, dirs, log.New())
		require.ErrorContains(t, err, "starts at genesis")
		require.Equal(t, &atGenesis, readStoredChainConfig(t, db).BinaryTrieTime)
	})
}

func TestBinaryTrieOverrideRefusedWithoutStoredChainConfig(t *testing.T) {
	withCommitmentVariant(t, false, false)
	dirs := datadir.New(t.TempDir())
	_, err := dbstate.ResolveErigonDBSettings(dirs, log.New(), true)
	require.NoError(t, err)
	db := temporaltest.NewTestDB(t, dirs)
	g := delayedPBinGenesis()
	g.Config.BinaryTrieTime = nil
	_, _, err = genesiswrite.CommitGenesisBlockWithOverride(db, g, "", nil, nil, nil, false, dirs, log.New())
	require.NoError(t, err)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			return err
		}
		return tx.Delete(kv.ConfigTable, genesisHash[:])
	}))

	binaryTrieTime := uint64(20)
	_, _, err = genesiswrite.CommitGenesisBlockWithOverride(db, g, "", nil, nil, &binaryTrieTime, false, dirs, log.New())
	require.ErrorContains(t, err, "hex+bin")
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			return err
		}
		cfg, err := rawdb.ReadChainConfig(tx, genesisHash)
		require.Nil(t, cfg)
		return err
	}))
}

func TestPBinGenesisComputesBothRootsAtBlockZero(t *testing.T) {
	withCommitmentVariant(t, true, true)
	g := delayedPBinGenesis()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithSequentialCommitment())
	require.NoError(t, err)
	defer sd.Close()
	head, _ := genesiswrite.GenesisWithoutStateToBlock(g)
	headerRoot, _, err := genesiswrite.ComputeGenesisCommitment(t.Context(), g, tx, sd, head)
	require.NoError(t, err)

	hexRoot, err := sd.GetCommitmentCtxForDomain(kv.CommitmentDomain).Trie().RootHash()
	require.NoError(t, err)
	binRoot, err := sd.GetCommitmentCtxForDomain(kv.CommitmentBinDomain).Trie().RootHash()
	require.NoError(t, err)
	stateBlob, _, ok := sd.GetLatestFromMemory(kv.CommitmentBinDomain, commitment.KeyCommitmentState)
	require.True(t, ok)
	require.NoError(t, pbt.ValidateEngineStateBlob(stateBlob))
	require.Equal(t, hexRoot, headerRoot)
	require.NotEqual(t, hexRoot, binRoot)
	got, err := rawdb.ReadShadowStateRoot(tx, head.Hash(), 0)
	require.NoError(t, err)
	require.Equal(t, binRoot, got)
}

func TestHexGenesisWithoutScheduleIsStable(t *testing.T) {
	withCommitmentVariant(t, false, false)
	g := pbinTestGenesis()
	first, _, err := genesiswrite.GenesisToBlock(g, datadir.New(t.TempDir()), log.New())
	require.NoError(t, err)
	second, _, err := genesiswrite.GenesisToBlock(g, datadir.New(t.TempDir()), log.New())
	require.NoError(t, err)
	require.Equal(t, first.Hash(), second.Hash())
	require.Equal(t, first.Root(), second.Root())
}

// Genesis produces the block-0 root the executor is later checked against, so it must
// use the variant the datadir uses, not always the hex trie.
func TestPBinGenesisComputesBinaryRoot(t *testing.T) {
	// No t.Parallel: mutates process-global statecfg flags.
	logger := log.New()
	g := pbinTestGenesis()

	withBinCommitment(t, false)
	hexBlock, _, err := genesiswrite.GenesisToBlock(g, datadir.New(t.TempDir()), logger)
	require.NoError(t, err)

	withBinCommitment(t, true)
	binBlock, _, err := genesiswrite.GenesisToBlock(g, datadir.New(t.TempDir()), logger)
	require.NoError(t, err)

	require.NotEqual(t, hexBlock.Root(), binBlock.Root(), "genesis under the bin variant returned the hex root")
	require.Equal(t, common.BytesToHash(pbinGenesisRoot(t, g)), binBlock.Root())
}

// Oracle for GenesisToBlock: the same root computed through SharedDomains on bin.
func pbinGenesisRoot(t *testing.T, g *types.Genesis) []byte {
	return pbinGenesisRootForDomain(t, g, kv.CommitmentDomain)
}

func pbinGenesisRootForDomain(t *testing.T, g *types.Genesis, domain kv.Domain) []byte {
	t.Helper()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	require.Equal(t, commitment.VariantBinPatriciaTrie, sd.GetCommitmentCtxForDomain(domain).Trie().Variant())

	head, _ := genesiswrite.GenesisWithoutStateToBlock(g)
	root, _, err := genesiswrite.ComputeGenesisCommitment(t.Context(), g, tx, sd, head)
	require.NoError(t, err)
	stateBlob, _, ok := sd.GetLatestFromMemory(domain, commitment.KeyCommitmentState)
	require.True(t, ok)
	require.NoError(t, pbt.ValidateEngineStateBlob(stateBlob))
	require.NoError(t, sd.Flush(t.Context(), tx))
	require.NoError(t, pbt.ValidateEngineIdentityFromTx(tx, domain))
	return root
}

func TestPBinGenesisDelayedScheduleRetryWithHexBin(t *testing.T) {
	for _, bin := range []bool{false, true} {
		t.Run(fmt.Sprintf("bin=%t", bin), func(t *testing.T) {
			dirs := datadir.New(t.TempDir())
			withCommitmentVariant(t, bin, false)
			_, _, refused := genesiswrite.GenesisToBlock(delayedPBinGenesis(), dirs, log.New())
			require.ErrorContains(t, refused, "binaryTrieTime")
			_, err := dbstate.ReadErigonDBSettings(dirs)
			require.ErrorIs(t, err, fs.ErrNotExist)
			require.ErrorContains(t, refused, "COMMITMENT_HEX_BIN")

			withCommitmentVariant(t, false, true)
			_, _, err = genesiswrite.GenesisToBlock(delayedPBinGenesis(), dirs, log.New())
			require.NoError(t, err)
			if bin {
				pbinGenesisRootForDomain(t, delayedPBinGenesis(), kv.CommitmentBinDomain)
			}
		})
	}
}
