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

package execctx_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
)

func cloneDeltas(in []commitment.BranchDelta) []commitment.BranchDelta {
	out := make([]commitment.BranchDelta, len(in))
	for i, d := range in {
		out[i] = commitment.BranchDelta{Key: bytes.Clone(d.Key), Data: bytes.Clone(d.Data), Prev: bytes.Clone(d.Prev)}
	}
	return out
}

func splitParts(deltas []commitment.BranchDelta, size int) [][]commitment.BranchDelta {
	var parts [][]commitment.BranchDelta
	for len(deltas) > 0 {
		n := min(size, len(deltas))
		parts = append(parts, deltas[:n])
		deltas = deltas[n:]
	}
	return parts
}

func TestPutCommitmentBranchesMatchesPerRecordPuts(t *testing.T) {
	seed := commitmentPutCorpus(3*8192 + 17)
	next := make([]commitment.BranchDelta, 0, len(seed)+2)
	for i, d := range seed {
		switch i % 4 {
		case 0:
			next = append(next, commitment.BranchDelta{Key: d.Key, Data: []byte{}, Prev: d.Data})
		case 1:
			next = append(next, commitment.BranchDelta{Key: d.Key, Data: d.Data, Prev: d.Data})
		case 2:
			next = append(next, commitment.BranchDelta{Key: d.Key, Data: append(bytes.Clone(d.Data), 7), Prev: nil})
		default:
			next = append(next, commitment.BranchDelta{Key: d.Key, Data: []byte{byte(i)}, Prev: d.Data})
		}
	}
	next = append(next, commitment.BranchDelta{Key: seed[3].Key, Data: []byte{0xaa}, Prev: []byte{byte(3)}})

	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	run := func(batch bool) (map[string][]byte, []kv.DomainEntryDiff) {
		tx, err := db.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		defer tx.Rollback()
		sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
		require.NoError(t, err)
		defer sd.Close()

		var diff kv.DomainDiff
		for _, round := range []struct {
			txNum  uint64
			deltas []commitment.BranchDelta
		}{{1, seed}, {2, next}} {
			if batch {
				require.NoError(t, sd.PutCommitmentBranches(tx, splitParts(cloneDeltas(round.deltas), 5), round.txNum, &diff))
				continue
			}
			for _, d := range cloneDeltas(round.deltas) {
				require.NoError(t, sd.DomainPutCommitmentDiff(tx, d.Key, d.Data, round.txNum, d.Prev, &diff))
			}
		}
		latest := make(map[string][]byte, len(seed))
		for _, d := range seed {
			v, _, err := sd.GetLatest(kv.CommitmentDomain, tx, d.Key)
			require.NoError(t, err)
			latest[string(d.Key)] = bytes.Clone(v)
		}
		return latest, diff.GetDiffSet()
	}

	wantLatest, wantDiff := run(false)
	gotLatest, gotDiff := run(true)
	require.Equal(t, wantLatest, gotLatest)
	require.Equal(t, wantDiff, gotDiff)
}

func TestPutCommitmentBranchesResolvesNilPrevAfterEarlierWrites(t *testing.T) {
	key := []byte{0x41, 1, 2, 3}
	a, b := []byte{0x0a}, []byte{0x0b}
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	run := func(batch bool) []byte {
		tx, err := db.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		defer tx.Rollback()
		sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
		require.NoError(t, err)
		defer sd.Close()

		rounds := []struct {
			txNum uint64
			parts [][]commitment.BranchDelta
		}{
			{1, [][]commitment.BranchDelta{{{Key: key, Data: a, Prev: []byte{}}}}},
			{2, [][]commitment.BranchDelta{{{Key: key, Data: b, Prev: a}}, {{Key: key, Data: a, Prev: nil}}}},
		}
		for _, round := range rounds {
			if batch {
				require.NoError(t, sd.PutCommitmentBranches(tx, round.parts, round.txNum, nil))
				continue
			}
			for _, part := range round.parts {
				for _, d := range part {
					require.NoError(t, sd.DomainPutCommitmentDiff(tx, d.Key, d.Data, round.txNum, d.Prev, nil))
				}
			}
		}
		v, _, err := sd.GetLatest(kv.CommitmentDomain, tx, key)
		require.NoError(t, err)
		return bytes.Clone(v)
	}
	require.Equal(t, run(false), run(true))
}

func TestFlushPendingDeltasLandInTheBlockChangeset(t *testing.T) {
	seed := commitmentPutCorpus(64)
	next := make([]commitment.BranchDelta, len(seed))
	for i, d := range seed {
		next[i] = commitment.BranchDelta{Key: d.Key, Data: []byte{byte(i), 1}, Prev: d.Data}
	}
	blockHash := common.Hash{7}
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	run := func(pending bool) (map[string][]byte, []kv.DomainEntryDiff, []kv.DomainEntryDiff) {
		tx, err := db.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		defer tx.Rollback()
		sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
		require.NoError(t, err)
		defer sd.Close()

		require.NoError(t, sd.PutCommitmentBranches(tx, [][]commitment.BranchDelta{cloneDeltas(seed)}, 1, nil))
		block, live := &changeset.StateChangeSet{}, &changeset.StateChangeSet{}
		sd.SavePastChangesetAccumulator(blockHash, 5, block)
		sd.SetChangesetAccumulator(live)
		if pending {
			sd.GetCommitmentContext().SetPendingUpdate(&commitment.PendingCommitmentUpdate{
				BlockNum: 5, BlockHash: blockHash, TxNum: 2, Deltas: splitParts(cloneDeltas(next), 5),
			})
			require.NoError(t, sd.FlushPendingUpdates(t.Context(), tx))
		} else {
			restore := sd.SwapCommitmentDiffLocked(block)
			for _, d := range cloneDeltas(next) {
				require.NoError(t, sd.DomainPut(kv.CommitmentDomain, tx, d.Key, d.Data, 2, d.Prev))
			}
			restore()
		}
		latest := make(map[string][]byte, len(seed))
		for _, d := range seed {
			v, _, err := sd.GetLatest(kv.CommitmentDomain, tx, d.Key)
			require.NoError(t, err)
			latest[string(d.Key)] = bytes.Clone(v)
		}
		return latest, block.Diffs[kv.CommitmentDomain].GetDiffSet(), live.Diffs[kv.CommitmentDomain].GetDiffSet()
	}

	wantLatest, wantBlock, wantLive := run(false)
	gotLatest, gotBlock, gotLive := run(true)
	require.Len(t, wantBlock, len(seed))
	require.Empty(t, wantLive)
	require.Equal(t, wantLatest, gotLatest)
	require.Equal(t, wantBlock, gotBlock)
	require.Equal(t, wantLive, gotLive)
}
