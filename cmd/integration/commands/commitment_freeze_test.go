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

package commands

import (
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
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	chainpkg "github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	etypes "github.com/erigontech/erigon/execution/types"
)

func TestFreezeHexCommitmentUsesExecutedState(t *testing.T) {
	tx, agg := newCommitmentFreezeTest(t, 40)
	txNum, err := freezeHexCommitment(tx, agg)
	require.NoError(t, err)
	require.Equal(t, uint64(3), txNum)
	frozenAt, frozen := agg.IsDomainFrozen(kv.CommitmentDomain)
	require.True(t, frozen)
	require.Equal(t, uint64(3), frozenAt)
	settings, err := dbstate.ReadErigonDBSettings(agg.Dirs())
	require.NoError(t, err)
	frozenAt, frozen = settings.FrozenAt(kv.CommitmentDomain)
	require.True(t, frozen)
	require.Equal(t, uint64(3), frozenAt)
}

func TestFreezeHexCommitmentRejectsCanonicalHex(t *testing.T) {
	tx, agg := newCommitmentFreezeTest(t, 20)
	_, err := freezeHexCommitment(tx, agg)
	require.ErrorContains(t, err, "hex commitment is still canonical")
	_, frozen := agg.IsDomainFrozen(kv.CommitmentDomain)
	require.False(t, frozen)
	settings, err := dbstate.ReadErigonDBSettings(agg.Dirs())
	require.NoError(t, err)
	_, frozen = settings.FrozenAt(kv.CommitmentDomain)
	require.False(t, frozen)
}

func TestFreezeHexCommitmentRejectsUnalignedBinary(t *testing.T) {
	tx, agg := newCommitmentFreezeTest(t, 40)
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	defer domains.Close()
	state, err := commitmentdb.NewCommitmentState(2, 1, nil).Encode()
	require.NoError(t, err)
	require.NoError(t, domains.DomainPut(kv.CommitmentBinDomain, tx, commitment.KeyCommitmentState, state, 4, nil))
	require.NoError(t, domains.Flush(t.Context(), tx))
	_, err = freezeHexCommitment(tx, agg)
	require.ErrorContains(t, err, "binary commitment is not aligned")
	_, frozen := agg.IsDomainFrozen(kv.CommitmentDomain)
	require.False(t, frozen)
	settings, err := dbstate.ReadErigonDBSettings(agg.Dirs())
	require.NoError(t, err)
	_, frozen = settings.FrozenAt(kv.CommitmentDomain)
	require.False(t, frozen)
}

func newCommitmentFreezeTest(t *testing.T, blockTime uint64) (kv.TemporalRwTx, *dbstate.Aggregator) {
	t.Helper()
	withBinCommitmentProcess(t, "")
	previousDual := statecfg.ExperimentalHexBinCommitment
	t.Cleanup(func() { statecfg.ExperimentalHexBinCommitment = previousDual })
	statecfg.ExperimentalHexBinCommitment = true
	dirs := datadir.New(t.TempDir())
	_, err := dbstate.ResolveErigonDBSettings(dirs, log.New(), true)
	require.NoError(t, err)
	db := temporaltest.NewTestDB(t, dirs, temporaltest.WithStepSize(16))
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	agg := db.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	state, err := commitmentdb.NewCommitmentState(3, 1, nil).Encode()
	require.NoError(t, err)
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	t.Cleanup(domains.Close)
	for _, domain := range agg.CommitmentDomains() {
		require.NoError(t, domains.DomainPut(domain, tx, commitment.KeyCommitmentState, state, 3, nil))
	}
	require.NoError(t, domains.Flush(t.Context(), tx))
	require.NoError(t, rawdbv3.TxNums.Append(tx, 1, 3))
	require.NoError(t, rawdbv3.TxNums.Append(tx, 2, 6))
	genesisHash := common.Hash{1}
	activation := uint64(30)
	require.NoError(t, rawdb.WriteCanonicalHash(tx, genesisHash, 0))
	require.NoError(t, rawdb.WriteChainConfig(tx, genesisHash, &chainpkg.Config{BinaryTrieTime: &activation}))
	header := &etypes.Header{Number: *uint256.NewInt(1), Time: blockTime}
	require.NoError(t, rawdb.WriteHeader(tx, header))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, header.Hash(), 1))
	return tx, agg
}
