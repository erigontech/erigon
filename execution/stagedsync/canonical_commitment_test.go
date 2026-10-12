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

package stagedsync

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

type canonicalDomainTx struct {
	domainStepFrontierTx
	domain   kv.Domain
	selector bool
}

func (tx *canonicalDomainTx) AggTx() any {
	if !tx.selector {
		return nil
	}
	return canonicalDomainSelector{domain: tx.domain}
}

type canonicalDomainSelector struct {
	domain kv.Domain
}

func (s canonicalDomainSelector) CanonicalCommitmentDomain() kv.Domain {
	return s.domain
}

func TestSnapshotStepAlignmentUsesCanonicalDomain(t *testing.T) {
	tx := &canonicalDomainTx{
		domain:   kv.CommitmentBinDomain,
		selector: true,
		domainStepFrontierTx: domainStepFrontierTx{frontiers: map[kv.Domain]kv.Step{
			kv.CommitmentDomain:    2,
			kv.CommitmentBinDomain: 1,
			kv.AccountsDomain:      1,
			kv.StorageDomain:       1,
			kv.CodeDomain:          1,
		}},
	}

	step, err := snapshotStepAlignment(tx)
	require.NoError(t, err)
	require.Equal(t, kv.Step(1), step)
}

func TestCanonicalCommitmentDomainDefaultsToHex(t *testing.T) {
	require.Equal(t, kv.CommitmentDomain, canonicalCommitmentDomain(&canonicalDomainTx{}))
}

func TestHistoryRetireCutoffsApplyCommitmentWindowToBin(t *testing.T) {
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	snaps := db.(freezeblocks.HasBlockFiles).DebugBlockFiles()
	br := freezeblocks.NewBlockReader(snaps)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	const perBlock = uint64(10)
	const forward = uint64(30)
	for block := uint64(0); block <= forward; block++ {
		require.NoError(t, rawdbv3.TxNums.Append(tx, block, block*perBlock+perBlock-1))
	}

	cutoffs, err := historyRetireCutoffs(context.Background(), tx, br,
		prune.Mode{Initialised: true, History: prune.Distance(10), CommitmentHistory: prune.Distance(5)}, forward)
	require.NoError(t, err)
	require.Equal(t, uint64(250), cutoffs.PerDomain[kv.CommitmentDomain])
	require.Equal(t, uint64(250), cutoffs.PerDomain[kv.CommitmentBinDomain])
}
