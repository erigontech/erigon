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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
	"github.com/erigontech/erigon/execution/types/accounts"
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

// TestFlushPendingLandsInTheBlockChangeset checks that a pending update whose
// block has a saved changeset lands in that changeset exactly as it would if
// the changeset were the live accumulator, leaves the live one empty, and does
// not wait on changesetMu.
func TestFlushPendingLandsInTheBlockChangeset(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	blockHash := common.Hash{7}
	seed := commitmentPutCorpus(64)
	pendDeltas := func(byHash bool) func(*testing.T, *execctx.SharedDomains, kv.TemporalRwTx) {
		return func(t *testing.T, sd *execctx.SharedDomains, tx kv.TemporalRwTx) {
			require.NoError(t, sd.PutCommitmentBranches(tx, [][]commitment.BranchDelta{runner.CloneDeltas(seed)}, 1, nil))
			next := make([]commitment.BranchDelta, len(seed))
			for i, d := range seed {
				next[i] = commitment.BranchDelta{Key: d.Key, Data: []byte{byte(i), 1}, Prev: d.Data}
			}
			upd := &commitment.PendingCommitmentUpdate{BlockNum: 5, TxNum: 2, Deltas: slices.Collect(slices.Chunk(next, 5))}
			if byHash {
				upd.BlockHash = blockHash
			}
			sd.GetCommitmentContext().SetPendingUpdate(upd)
		}
	}
	pendDeferred := func(t *testing.T, sd *execctx.SharedDomains, tx kv.TemporalRwTx) {
		sd.SetDeferCommitmentUpdates(true)
		for i := range 32 {
			addr := make([]byte, 20)
			binary.BigEndian.PutUint64(addr, uint64(i)+1)
			require.NoError(t, sd.DomainPut(kv.AccountsDomain, tx, addr, accounts.SerialiseV3(&accounts.Account{Nonce: uint64(i) + 1}), 2, nil))
		}
		_, err := sd.ComputeCommitment(t.Context(), tx, false, 5, 2, "", nil)
		require.NoError(t, err)
		require.NotEmpty(t, sd.GetCommitmentContext().PeekPendingUpdate().Deferred)
	}

	for _, tc := range []struct {
		name string
		pend func(*testing.T, *execctx.SharedDomains, kv.TemporalRwTx)
	}{
		{"deltas by hash", pendDeltas(true)},
		{"deltas by block number", pendDeltas(false)},
		{"deferred by block number", pendDeferred},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
			run := func(saved bool) (latest map[string][]byte, blockDiff, liveDiff []kv.DomainEntryDiff) {
				tx, err := db.BeginTemporalRw(t.Context())
				require.NoError(t, err)
				defer tx.Rollback()
				sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
				require.NoError(t, err)
				defer sd.Close()

				tc.pend(t, sd, tx)
				block, live := &changeset.StateChangeSet{}, &changeset.StateChangeSet{}
				if saved {
					sd.SavePastChangesetAccumulator(blockHash, 5, block)
					sd.SetChangesetAccumulator(live)
				} else {
					sd.SetChangesetAccumulator(block)
				}
				if saved {
					sd.LockChangesetAccumulator()
					done := make(chan error, 1)
					go func() { done <- sd.FlushPendingUpdates(tx) }()
					select {
					case err := <-done:
						sd.UnlockChangesetAccumulator()
						require.NoError(t, err)
					case <-time.After(5 * time.Second):
						sd.UnlockChangesetAccumulator()
						<-done
						t.Fatal("flush into a saved changeset waited on changesetMu")
					}
				} else {
					require.NoError(t, sd.FlushPendingUpdates(tx))
				}
				require.Nil(t, sd.GetCommitmentContext().PeekPendingUpdate())
				blockDiff = block.Diffs[kv.CommitmentDomain].GetDiffSet()
				latest = make(map[string][]byte, len(blockDiff))
				for _, e := range blockDiff {
					key := e.Key[:len(e.Key)-8]
					v, _, err := sd.GetLatest(kv.CommitmentDomain, tx, []byte(key))
					require.NoError(t, err)
					latest[key] = bytes.Clone(v)
				}
				return latest, blockDiff, live.Diffs[kv.CommitmentDomain].GetDiffSet()
			}

			wantLatest, wantBlock, _ := run(false)
			gotLatest, gotBlock, gotLive := run(true)
			require.NotEmpty(t, wantBlock)
			require.Equal(t, wantLatest, gotLatest)
			require.Equal(t, wantBlock, gotBlock)
			require.Empty(t, gotLive)
		})
	}
}

// TestFlushPendingRunsBesideTheFold flushes a block's pending update while
// another goroutine binds the same block's changeset and writes state into it;
// run it with -race.
func TestFlushPendingRunsBesideTheFold(t *testing.T) {
	seed := commitmentPutCorpus(256)
	next := make([]commitment.BranchDelta, len(seed))
	for i, d := range seed {
		next[i] = commitment.BranchDelta{Key: d.Key, Data: []byte{byte(i), 3}, Prev: d.Data}
	}
	blockHash := common.Hash{9}
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	t.Cleanup(sd.Close)

	require.NoError(t, sd.PutCommitmentBranches(tx, [][]commitment.BranchDelta{runner.CloneDeltas(seed)}, 1, nil))
	block := &changeset.StateChangeSet{}
	sd.SavePastChangesetAccumulator(blockHash, 5, block)
	sd.SetChangesetAccumulator(&changeset.StateChangeSet{})
	sd.GetCommitmentContext().SetPendingUpdate(&commitment.PendingCommitmentUpdate{
		BlockNum: 5, BlockHash: blockHash, TxNum: 2, Deltas: slices.Collect(slices.Chunk(runner.CloneDeltas(next), 16)),
	})

	const folds = 200
	var wg sync.WaitGroup
	wg.Go(func() {
		assert.NoError(t, sd.FlushPendingUpdates(tx))
	})
	wg.Go(func() {
		for i := range folds {
			sd.LockChangesetAccumulator()
			unswap := sd.SwapChangesetAccumulatorLocked(block)
			sd.UnlockChangesetAccumulator()
			addr := make([]byte, 20)
			binary.BigEndian.PutUint64(addr, uint64(i)+1)
			assert.NoError(t, sd.DomainPut(kv.AccountsDomain, tx, addr, accounts.SerialiseV3(&accounts.Account{Nonce: uint64(i) + 1}), 3, nil))
			sd.LockChangesetAccumulator()
			unswap()
			sd.UnlockChangesetAccumulator()
		}
	})
	wg.Wait()
	require.Len(t, block.Diffs[kv.CommitmentDomain].GetDiffSet(), len(seed))
	require.Len(t, block.Diffs[kv.AccountsDomain].GetDiffSet(), folds)
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
