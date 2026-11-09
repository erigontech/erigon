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
	"encoding/binary"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/internal/commitmenttest/runner"
)

func commitmentPutCorpus(n int) []commitment.BranchDelta {
	rng := rand.New(rand.NewPCG(1, 2))
	writes := make([]commitment.BranchDelta, n)
	for i := range writes {
		key := make([]byte, 37)
		key[0] = 0x41
		for j := 1; j < len(key); j++ {
			key[j] = byte(rng.Uint32())
		}
		data := make([]byte, 300+rng.IntN(150))
		prev := make([]byte, len(data))
		for j := range data {
			data[j] = byte(rng.Uint32())
			prev[j] = byte(rng.Uint32())
		}
		writes[i] = commitment.BranchDelta{Key: key, Data: data, Prev: prev}
	}
	return writes
}

func TestPutCommitmentBranchesMatchesPerRecordPuts(t *testing.T) {
	previous := statecfg.Schema
	t.Cleanup(func() { statecfg.Schema = previous })
	statecfg.EnableHistoricalCommitment()
	seed := commitmentPutCorpus(3*8192 + 17)
	next := make([]commitment.BranchDelta, 0, len(seed)+2)
	for i, d := range seed {
		switch i % 4 {
		case 0:
			next = append(next, commitment.BranchDelta{Key: d.Key, Data: []byte{}, Prev: d.Data})
		case 1:
			next = append(next, commitment.BranchDelta{Key: d.Key, Data: d.Data, Prev: d.Data})
		case 2:
			next = append(next, commitment.BranchDelta{Key: d.Key, Data: append(bytes.Clone(d.Data), 7), Prev: d.Data})
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
		}{{1, seed}, {2, next}, {2, []commitment.BranchDelta{{Key: seed[2].Key, Data: []byte{0xaa, 0xbb}, Prev: next[2].Data}}}} {
			if batch {
				require.NoError(t, sd.PutCommitmentBranches(tx, slices.Collect(slices.Chunk(runner.CloneDeltas(round.deltas), 5)), round.txNum, &diff))
				continue
			}
			for _, d := range runner.CloneDeltas(round.deltas) {
				require.NoError(t, sd.DomainPutCommitmentDiff(tx, d.Key, d.Data, round.txNum, d.Prev, &diff))
			}
		}
		latest := make(map[string][]byte, len(seed))
		for _, d := range seed {
			v, _, readErr := sd.GetLatest(kv.CommitmentDomain, tx, d.Key)
			require.NoError(t, readErr)
			latest[string(d.Key)] = bytes.Clone(v)
		}
		require.NoError(t, sd.GetMemBatch().Flush(t.Context(), tx))
		cursor, err := tx.CursorDupSort(kv.TblCommitmentHistoryVals)
		require.NoError(t, err)
		defer cursor.Close()
		count := 0
		key, prev, err := cursor.SeekExact(seed[2].Key)
		for key != nil {
			require.NoError(t, err)
			if binary.BigEndian.Uint64(prev) == 2 {
				count++
				require.Equal(t, seed[2].Data, prev[8:])
			}
			key, prev, err = cursor.NextDup()
		}
		require.NoError(t, err)
		require.Equal(t, 1, count)
		return latest, diff.GetDiffSet()
	}

	wantLatest, wantDiff := run(false)
	gotLatest, gotDiff := run(true)
	require.Equal(t, wantLatest, gotLatest)
	require.Equal(t, wantDiff, gotDiff)
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

		require.NoError(t, sd.PutCommitmentBranches(tx, [][]commitment.BranchDelta{runner.CloneDeltas(seed)}, 1, nil))
		block, live := &changeset.StateChangeSet{}, &changeset.StateChangeSet{}
		sd.SavePastChangesetAccumulator(blockHash, 5, block)
		sd.SetChangesetAccumulator(live)
		if pending {
			sd.GetCommitmentContext().SetPendingUpdate(&commitment.PendingCommitmentUpdate{
				BlockNum: 5, BlockHash: blockHash, TxNum: 2, Deltas: slices.Collect(slices.Chunk(runner.CloneDeltas(next), 5)),
			})
			require.NoError(t, sd.FlushPendingUpdates(tx))
		} else {
			restore := sd.SwapCommitmentDiffLocked(block)
			for _, d := range runner.CloneDeltas(next) {
				require.NoError(t, sd.DomainPut(kv.CommitmentDomain, tx, d.Key, d.Data, 2, d.Prev))
			}
			restore()
		}
		latest := make(map[string][]byte, len(seed))
		for _, d := range seed {
			v, _, readErr := sd.GetLatest(kv.CommitmentDomain, tx, d.Key)
			require.NoError(t, readErr)
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

type latestOptionsTx struct {
	kv.TemporalTx
	buf []byte
}

func (tx *latestOptionsTx) GetLatest(d kv.Domain, key []byte, opts kv.GetLatestOptions) ([]byte, kv.Step, error) {
	return tx.buf, 0, nil
}

func TestCommitmentV3OwnsOnlyCommitmentReads(t *testing.T) {
	for _, v3 := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "v3"}[v3], func(t *testing.T) {
			t.Cleanup(statecfg.SetCommitmentV3(v3))
			db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
			tx, err := db.BeginTemporalRo(t.Context())
			require.NoError(t, err)
			t.Cleanup(tx.Rollback)
			spy := &latestOptionsTx{TemporalTx: tx, buf: []byte{1, 2, 3}}
			sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
			require.NoError(t, err)
			t.Cleanup(sd.Close)
			for _, d := range []kv.Domain{kv.CommitmentDomain, kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain} {
				v, _, err := sd.GetLatest(d, spy, make([]byte, 52))
				require.NoError(t, err)
				require.NotEmpty(t, v)
				require.Equal(t, !v3 || d != kv.CommitmentDomain, &v[0] == &spy.buf[0], "domain %s value %x", d, v)
			}
		})
	}
}

func TestFlushPendingWithoutChangesetLeavesLiveAccumulatorEmpty(t *testing.T) {
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	t.Cleanup(sd.Close)
	seed := commitmentPutCorpus(3)
	require.NoError(t, sd.PutCommitmentBranches(tx, [][]commitment.BranchDelta{runner.CloneDeltas(seed)}, 1, nil))
	live := &changeset.StateChangeSet{}
	sd.SetChangesetAccumulator(live)
	next := commitment.BranchDelta{Key: seed[0].Key, Data: []byte{9}, Prev: seed[0].Data}
	sd.GetCommitmentContext().SetPendingUpdate(&commitment.PendingCommitmentUpdate{TxNum: 2, Deltas: [][]commitment.BranchDelta{{next}}})
	require.NoError(t, sd.FlushPendingUpdatesWithoutChangeset(tx))
	got, _, err := sd.GetLatest(kv.CommitmentDomain, tx, next.Key)
	require.NoError(t, err)
	require.Equal(t, next.Data, got)
	require.Zero(t, len(live.Diffs[kv.CommitmentDomain].GetDiffSet()))
}
