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

package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types"
)

func TestPBinAggregatorRestoresCanonicalDomain(t *testing.T) {
	for _, variant := range []string{TrieVariantHexBin, TrieVariantBin} {
		t.Run(variant, func(t *testing.T) {
			dirs := datadir.New(t.TempDir())
			require.NoError(t, WriteErigonDBSettings(dirs, &ErigonDBSettings{TrieVariant: &variant}))
			db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
			activation := uint64(10)
			require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
				genesis := common.Hash{1}
				if err := rawdb.WriteCanonicalHash(tx, genesis, 0); err != nil {
					return err
				}
				if err := rawdb.WriteChainConfig(tx, genesis, &chain.Config{BinaryTrieTime: &activation}); err != nil {
					return err
				}
				header := &types.Header{Number: *uint256.NewInt(1), Time: 11}
				if err := rawdb.WriteHeader(tx, header); err != nil {
					return err
				}
				rawdb.WriteHeadBlockHash(tx, header.Hash())
				return nil
			}))
			agg, err := newAggregator(t.Context(), dirs, db, log.New())
			require.NoError(t, err)
			t.Cleanup(agg.Close)
			want := kv.CommitmentDomain
			if variant == TrieVariantHexBin {
				want = kv.CommitmentBinDomain
			}
			require.Equal(t, want, agg.CanonicalCommitmentDomain())
		})
	}
}

func TestPBinLaggingShadowDoesNotClampVisibleFiles(t *testing.T) {
	agg := pbinDualAggregator(t)
	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateDomainFiles(t, "commitment-bin", agg.Dirs(), []testFileRange{{0, 1}})
	require.NoError(t, agg.OpenFolder())
	at := agg.BeginFilesRo()
	defer at.Close()
	require.EqualValues(t, 2*alignStepSize, at.d[kv.AccountsDomain].files.EndTxNum())
	require.EqualValues(t, alignStepSize, at.d[kv.CommitmentBinDomain].files.EndTxNum())
}

func TestPBinCanonicalRoleDoesNotChangeHexReferences(t *testing.T) {
	agg := &Aggregator{trieVariant: TrieVariantHexBin}
	agg.SetCanonicalCommitmentDomain(kv.CommitmentBinDomain)
	agg.d[kv.CommitmentDomain] = &Domain{}
	agg.d[kv.CommitmentDomain].ReferencesInCommitmentBranches = true
	agg.d[kv.CommitmentBinDomain] = &Domain{}
	require.True(t, agg.referencesInCommitmentBranches())
	at := &AggregatorRoTx{a: agg}
	agg.stepSize.Store(1)
	at.d[kv.CommitmentDomain] = &DomainRoTx{files: visibleFiles{{endTxNum: commitment.DefaultKeyReferencingMinSteps, src: &FilesItem{version: version.V2_2}}}}
	at.d[kv.CommitmentBinDomain] = &DomainRoTx{files: visibleFiles{{endTxNum: commitment.DefaultKeyReferencingMinSteps, src: &FilesItem{version: version.V1_0}}}}
	require.False(t, at.commitmentVisibleFilesReferenced())
}

func TestPBinStoppedDomainSharedAcrossViews(t *testing.T) {
	agg := &Aggregator{}
	type stopper interface {
		StopCommitmentDomain(kv.Domain)
		CommitmentDomainStopped(kv.Domain) bool
	}
	owner, ok := any(agg).(stopper)
	require.True(t, ok, "aggregator must retain shadow failure state across calculator batches")
	owner.StopCommitmentDomain(kv.CommitmentBinDomain)
	view, ok := any(&AggregatorRoTx{a: agg}).(stopper)
	require.True(t, ok)
	require.True(t, view.CommitmentDomainStopped(kv.CommitmentBinDomain))
	require.False(t, view.CommitmentDomainStopped(kv.CommitmentDomain))
}

func TestPBinLaggingHexShadowDoesNotClampVisibleFiles(t *testing.T) {
	agg := pbinDualAggregator(t)
	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	agg.SetCanonicalCommitmentDomain(kv.CommitmentBinDomain)
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}})
	generateDomainFiles(t, "commitment-bin", agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	require.NoError(t, agg.OpenFolder())
	at := agg.BeginFilesRo()
	defer at.Close()
	require.EqualValues(t, 2*alignStepSize, at.d[kv.AccountsDomain].files.EndTxNum())
	require.EqualValues(t, alignStepSize, at.d[kv.CommitmentDomain].files.EndTxNum())
	require.True(t, agg.checker.CheckDependentPresent(FromDomain(kv.AccountsDomain), Any, 0, alignStepSize))
}

func pbinDualAggregator(t *testing.T) *Aggregator {
	t.Helper()
	_, agg := testDbAndAggregatorv3(t, alignStepSize)
	agg.trieVariant = TrieVariantHexBin
	binCfg := statecfg.Schema.GetDomainCfg(kv.CommitmentBinDomain)
	binCfg.Accessors = statecfg.AccessorBTree | statecfg.AccessorExistence
	binCfg.FileVersion.AccessorBT = version.V1_0_standart
	binCfg.FileVersion.AccessorKVEI = version.V1_0_standart
	require.NoError(t, agg.RegisterDomain(binCfg, agg.savedSalt, agg.Dirs(), log.New()))
	agg.EnableDomain(kv.CommitmentBinDomain)
	return agg
}

func TestPBinStoppedShadowDoesNotMerge(t *testing.T) {
	agg := pbinDualAggregator(t)
	agg.SetCanonicalCommitmentDomain(kv.CommitmentBinDomain)
	ranges := []testFileRange{{0, 1}, {1, 2}}
	generateStateFiles(t, agg.Dirs(), ranges)
	generateCommitmentFile(t, agg.Dirs(), ranges)
	generateDomainFiles(t, "commitment-bin", agg.Dirs(), ranges)
	require.NoError(t, agg.OpenFolder())
	agg.StopCommitmentDomain(kv.CommitmentDomain)
	at := agg.BeginFilesRo()
	defer at.Close()
	merges := at.findMergeRange(2*alignStepSize, alignStepSize, 8)
	require.False(t, merges.domain[kv.CommitmentDomain].any())
	require.True(t, merges.domain[kv.AccountsDomain].values.needMerge)
}
