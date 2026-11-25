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

package v2

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/bits"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
)

func TestBranchData_MergeHexBranches2(t *testing.T) {
	t.Parallel()
	row, bm, enc := encodeCellRow(t, 16)
	require.NotEmpty(t, enc)
	t.Logf("enc [%d] %x\n", len(enc), enc)

	bmg := commitment.NewHexBranchMerger(8192)
	res, err := bmg.Merge(enc, enc)
	require.NoError(t, err)
	require.Equal(t, enc, res)

	tm, am, origins, err := decodeBranchCells(res)
	require.NoError(t, err)
	require.Equal(t, tm, am)
	require.Equal(t, bm, am)

	i := 0
	for _, c := range origins {
		if c == nil {
			continue
		}
		require.Equal(t, row[i].extLen, c.extLen)
		require.Equal(t, row[i].extension, c.extension)
		require.Equal(t, row[i].accountAddrLen, c.accountAddrLen)
		require.Equal(t, row[i].accountAddr, c.accountAddr)
		require.Equal(t, row[i].storageAddrLen, c.storageAddrLen)
		require.Equal(t, row[i].storageAddr, c.storageAddr)
		i++
	}
}

func TestBranchData_ChildCount(t *testing.T) {
	t.Parallel()

	require.Equal(t, 0, commitment.BranchData(nil).ChildCount())
	require.Equal(t, 0, commitment.BranchData{}.ChildCount())
	require.Equal(t, 0, commitment.BranchData{0xff, 0xff, 0x00}.ChildCount(), "buffer shorter than 4 bytes has no afterMap")

	for _, size := range []int{1, 2, 5, 16} {
		_, bm, enc := encodeCellRow(t, size)
		require.Equal(t, size, bits.OnesCount16(bm))
		require.Equal(t, size, enc.ChildCount(), "ChildCount must equal the number of afterMap children")
	}

	var buf commitment.BranchData = make([]byte, 4)
	binary.BigEndian.PutUint16(buf[0:], 0xffff)
	binary.BigEndian.PutUint16(buf[2:], 0b0000_0000_0000_0111)
	require.Equal(t, 3, buf.ChildCount())
}

func TestDecodeBranchWithLeafHashes(t *testing.T) {
	row, bm := generateCellRow(t, 16)

	for i := range row {
		if row[i].accountAddrLen > 0 {
			rand.Read(row[i].stateHash[:])
			row[i].stateHashLen = 32
		}
	}

	be := NewBranchEncoder(1024)
	cellData := generateCellEncodeDataRow(t, row, bm)
	enc, err := be.EncodeBranch(bm, bm, bm, &cellData)
	require.NoError(t, err)
	require.NotEmpty(t, enc)
}

func TestBranchData_ReplacePlainKeys(t *testing.T) {
	t.Parallel()

	_, _, enc := encodeCellRow(t, 16)

	original := bytes.Clone(enc)

	target := make([]byte, 0, len(enc))
	oldKeys := make([][]byte, 0)
	replaced, err := enc.ReplacePlainKeys(target, func(key []byte, isStorage bool) ([]byte, error) {
		oldKeys = append(oldKeys, key)
		if isStorage {
			return key[:8], nil
		}
		return key[:4], nil
	})
	require.NoError(t, err)
	require.Lessf(t, len(replaced), len(enc), "replaced expected to be shorter than original enc")

	keyI := 0
	replacedBack, err := replaced.ReplacePlainKeys(nil, func(key []byte, isStorage bool) ([]byte, error) {
		require.Equal(t, oldKeys[keyI][:4], key[:4])
		defer func() { keyI++ }()
		return oldKeys[keyI], nil
	})
	require.NoError(t, err)
	require.EqualValues(t, original, replacedBack)

	t.Run("merge replaced and original back", func(t *testing.T) {
		orig := bytes.Clone(original)

		merged, err := replaced.MergeHexBranches(original, nil)
		require.NoError(t, err)
		require.EqualValues(t, orig, merged)

		merged, err = merged.MergeHexBranches(replacedBack, nil)
		require.NoError(t, err)
		require.EqualValues(t, orig, merged)
	})
}

func TestBranchData_ReplacePlainKeys_WithEmpty(t *testing.T) {
	t.Parallel()

	_, _, enc := encodeCellRow(t, 16)

	original := bytes.Clone(enc)

	target := make([]byte, 0, len(enc))
	oldKeys := make([][]byte, 0)
	replaced, err := enc.ReplacePlainKeys(target, func(key []byte, isStorage bool) ([]byte, error) {
		oldKeys = append(oldKeys, key)
		if isStorage {
			return nil, nil
		}
		return nil, nil
	})
	require.NoError(t, err)
	require.Lenf(t, replaced, len(enc), "replaced expected to be equal to origin (since no replacements were made)")

	keyI := 0
	replacedBack, err := replaced.ReplacePlainKeys(nil, func(key []byte, isStorage bool) ([]byte, error) {
		require.Equal(t, oldKeys[keyI][:4], key[:4])
		defer func() { keyI++ }()
		return oldKeys[keyI], nil
	})
	require.NoError(t, err)
	require.EqualValues(t, original, replacedBack)

	t.Run("merge replaced and original back", func(t *testing.T) {
		orig := bytes.Clone(original)

		merged, err := replaced.MergeHexBranches(original, nil)
		require.NoError(t, err)
		require.EqualValues(t, orig, merged)

		merged, err = merged.MergeHexBranches(replacedBack, nil)
		require.NoError(t, err)
		require.EqualValues(t, orig, merged)
	})
}

func TestBranchData_ReplacePlainKeys_PartialChange(t *testing.T) {
	t.Parallel()

	_, _, enc := encodeCellRow(t, 16)

	original := bytes.Clone(enc)

	type keyRecord struct {
		key       []byte
		isStorage bool
	}
	var origKeys []keyRecord
	replaced, err := commitment.BranchData(bytes.Clone(enc)).ReplacePlainKeys(
		make([]byte, 0, len(enc)),
		func(key []byte, isStorage bool) ([]byte, error) {
			origKeys = append(origKeys, keyRecord{bytes.Clone(key), isStorage})
			if isStorage {
				return nil, nil
			}
			return key[:4], nil
		},
	)
	require.NoError(t, err)

	keyI := 0
	expandedBack, err := replaced.ReplacePlainKeys(nil, func(key []byte, isStorage bool) ([]byte, error) {
		rec := origKeys[keyI]
		keyI++
		if isStorage {
			require.True(t, rec.isStorage)
			return nil, nil
		}
		require.False(t, rec.isStorage)
		return rec.key, nil
	})
	require.NoError(t, err)
	require.EqualValues(t, original, expandedBack,
		"round-trip with partial key replacement should reproduce original")
}

type recordingCtx struct {
	puts []struct{ prefix, data, prev []byte }
}

func (r *recordingCtx) Branch(_ []byte) ([]byte, kv.Step, error) { return nil, 0, nil }

func (r *recordingCtx) PutBranch(prefix, data, prev []byte) error {
	r.puts = append(r.puts, struct{ prefix, data, prev []byte }{
		bytes.Clone(prefix), bytes.Clone(data), bytes.Clone(prev),
	})
	return nil
}

func (r *recordingCtx) Account(_ []byte) (*commitment.Update, error) { return nil, nil }

func (r *recordingCtx) Storage(_ []byte) (*commitment.Update, error) { return nil, nil }

func (r *recordingCtx) TxNum() uint64 { return 0 }

// CollectTombstone writes the empty-branch tombstone raw and carries prev through
// for the domain diff — it must NOT merge the tombstone against prev (a real branch
// can be shorter than its bitmap implies, and Merge would overrun it).
func TestCollectTombstone_WritesRawAndCarriesPrev(t *testing.T) {
	t.Parallel()
	prefix := []byte{0xab, 0xcd}
	row, bm := generateCellRow(t, 5)
	cells := generateCellEncodeDataRow(t, row, bm)

	be := NewBranchEncoder(1024)
	branch, err := be.EncodeBranch(bm, bm, bm, &cells)
	require.NoError(t, err)
	prev := bytes.Clone(branch)

	wantTomb, err := NewBranchEncoder(1024).EncodeBranch(0, bm, 0, nil)
	require.NoError(t, err)

	ctx := &recordingCtx{}
	require.NoError(t, be.CollectTombstone(ctx, prefix, bm, prev))
	require.Len(t, ctx.puts, 1)
	require.Equal(t, 0, commitment.BranchData(ctx.puts[0].data).ChildCount(), "tombstone is an empty branch")
	require.Equal(t, []byte(wantTomb), ctx.puts[0].data, "raw tombstone, not a merge of prev")
	require.Equal(t, prev, ctx.puts[0].prev, "prev is carried for the domain diff / unwind")
}

func TestCollectUpdate_HonoursSuppliedPrev(t *testing.T) {
	t.Parallel()
	prefix := []byte{0xab, 0xcd}
	row, bm := generateCellRow(t, 4)
	cells := generateCellEncodeDataRow(t, row, bm)

	ctxNew := &recordingCtx{}
	beNew := NewBranchEncoder(1024)
	require.NoError(t, beNew.CollectUpdate(ctxNew, prefix, bm, bm, bm, &cells, nil))
	require.Len(t, ctxNew.puts, 1)
	require.Empty(t, ctxNew.puts[0].prev)

	beSame := NewBranchEncoder(1024)
	encoded, err := beSame.EncodeBranch(bm, bm, bm, &cells)
	require.NoError(t, err)
	unchanged := bytes.Clone(encoded)

	ctxSame := &recordingCtx{}
	require.NoError(t, beSame.CollectUpdate(ctxSame, prefix, bm, bm, bm, &cells, unchanged))
	require.Empty(t, ctxSame.puts, "a supplied prev identical to the update suppresses the write, so prev is really being used")
}

func TestCollectDeferredUpdate_CarriesSuppliedPrev(t *testing.T) {
	t.Parallel()
	prefix := []byte{0x11, 0x22}
	row, bm := generateCellRow(t, 4)
	cells := generateCellEncodeDataRow(t, row, bm)
	prev := []byte{0x00, 0x0f, 0x00, 0x0f, 0xaa}

	ctx := &recordingCtx{}
	be := NewBranchEncoder(1024)
	be.setDeferUpdates(true)
	require.NoError(t, be.CollectDeferredUpdate(ctx, prefix, bm, bm, bm, &cells, prev))

	require.Len(t, be.deferred, 1)
	require.Equal(t, prev, []byte(be.deferred[0].Prev), "the record must carry the prev it was given")
	require.Empty(t, ctx.puts, "deferred collection writes nothing")
}

// TestCollectDeferredUpdate_PoolRecycleDoesNotCorruptEarlierApply pins the
// contract documented on GetDeferredUpdate: PutDeferredUpdate recycles the
// prefix/raw backing arrays for a later, unrelated update, so a PutBranch
// implementation that copies (as recordingCtx and every real implementation
// do) must see its own copy stay correct across that recycle.
// Not parallel: it swaps the global DeferredUpdatePool for a deterministic one, since
// sync.Pool may hand back a fresh object and leave the recycle unexercised.
func TestCollectDeferredUpdate_PoolRecycleDoesNotCorruptEarlierApply(t *testing.T) {
	seed := &commitment.DeferredBranchUpdate{}
	saved := commitment.DeferredUpdatePool
	commitment.DeferredUpdatePool = &sync.Pool{New: func() any { return seed }}
	t.Cleanup(func() { commitment.DeferredUpdatePool = saved })

	rowA, bmA := generateCellRow(t, 8)
	cellsA := generateCellEncodeDataRow(t, rowA, bmA)
	rowB, bmB := generateCellRow(t, 2)
	cellsB := generateCellEncodeDataRow(t, rowB, bmB)

	wantBE := NewBranchEncoder(1024)
	rawA, err := wantBE.EncodeBranch(bmA, bmA, bmA, &cellsA)
	require.NoError(t, err)
	wantA := bytes.Clone([]byte(rawA))
	rawB, err := wantBE.EncodeBranch(bmB, bmB, bmB, &cellsB)
	require.NoError(t, err)
	wantB := bytes.Clone([]byte(rawB))
	require.NotEqual(t, wantA, wantB, "test rows must be distinctive")
	require.Greater(t, len(wantA), len(wantB), "round A must be longer so a truncation bug on reuse is visible")

	ctx := &recordingCtx{}
	be := NewBranchEncoder(1024)
	be.setDeferUpdates(true)

	require.NoError(t, be.CollectDeferredUpdate(ctx, []byte{0xAA, 0xAA}, bmA, bmA, bmA, &cellsA, nil))
	require.NoError(t, be.ApplyDeferredUpdates(1, ctx.PutBranch))
	be.ClearDeferred() // recycles this round's DeferredBranchUpdate into the pool

	require.Len(t, ctx.puts, 1)
	require.Equal(t, wantA, ctx.puts[0].data)

	// A shorter, distinctive second round reuses the pool's backing arrays.
	require.NoError(t, be.CollectDeferredUpdate(ctx, []byte{0xBB, 0xBB}, bmB, bmB, bmB, &cellsB, nil))
	require.Same(t, seed, be.deferred[0], "second round must run on the recycled object, or it proves nothing")
	require.NoError(t, be.ApplyDeferredUpdates(1, ctx.PutBranch))
	be.ClearDeferred()

	require.Len(t, ctx.puts, 2)
	require.Equal(t, wantA, ctx.puts[0].data, "recycling the pool must not corrupt data already handed to PutBranch")
	require.Equal(t, wantB, ctx.puts[1].data, "reused buffer must not retain stale bytes from the earlier, longer round")
}

// The encoder sits inside a HexPatriciaHashed that Release() parks in a pool,
// so a reslice would leave a branch's buffers reachable from there.
func TestClearDeferredDropsUpdatesItReslicesPast(t *testing.T) {
	t.Parallel()

	row, bm := generateCellRow(t, 8)
	cells := generateCellEncodeDataRow(t, row, bm)

	ctx := &recordingCtx{}
	be := NewBranchEncoder(1024)
	be.setDeferUpdates(true)
	require.NoError(t, be.CollectDeferredUpdate(ctx, []byte{0xAA, 0xAA}, bm, bm, bm, &cells, nil))
	require.NoError(t, be.CollectDeferredUpdate(ctx, []byte{0xBB, 0xBB}, bm, bm, bm, &cells, nil))
	require.Len(t, be.deferred, 2)

	be.ClearDeferred()

	for i, upd := range be.deferred[:cap(be.deferred)] {
		require.Nil(t, upd, "deferred[%d] still pins a DeferredBranchUpdate after ClearDeferred", i)
	}
}

func TestCollectDeferredUpdate_CallerOwnedIgnoresCapacityLimit(t *testing.T) {
	t.Parallel()
	row, bm := generateCellRow(t, 4)
	cells := generateCellEncodeDataRow(t, row, bm)

	ctx := &recordingCtx{}
	be := NewBranchEncoder(1024)
	be.setDeferUpdates(true)
	be.callerOwnsDeferred = true
	be.maxDeferredUpdates = 2

	for i := range 5 {
		require.NoError(t, be.CollectDeferredUpdate(ctx, []byte{0xAA, byte(i)}, bm, bm, bm, &cells, nil))
	}

	require.Zero(t, len(ctx.puts), "caller owns the output, so nothing may reach the domain before it validates the root")
	require.Len(t, be.deferred, 5, "every record must still be pending")
}

func TestCollectDeferredUpdate_InlineFlushesAtCapacity(t *testing.T) {
	t.Parallel()
	row, bm := generateCellRow(t, 4)
	cells := generateCellEncodeDataRow(t, row, bm)

	ctx := &recordingCtx{}
	be := NewBranchEncoder(1024)
	be.setDeferUpdates(true)
	be.maxDeferredUpdates = 2

	for i := range 3 {
		require.NoError(t, be.CollectDeferredUpdate(ctx, []byte{0xAA, byte(i)}, bm, bm, bm, &cells, nil))
	}

	require.Len(t, ctx.puts, 2, "inline collection still bounds its buffer by writing the batch through")
	require.Len(t, be.deferred, 1)
}

func TestCollectDeferredUpdate_NewBranchCarriesEmptyPrev(t *testing.T) {
	t.Parallel()
	row, bm := generateCellRow(t, 4)
	cells := generateCellEncodeDataRow(t, row, bm)

	be := NewBranchEncoder(1024)
	be.setDeferUpdates(true)
	require.NoError(t, be.CollectDeferredUpdate(&recordingCtx{}, []byte{0x33, 0x44}, bm, bm, bm, &cells, nil))
	require.Len(t, be.deferred, 1)
	require.NotNil(t, be.deferred[0].Prev, "a new branch must carry an empty prev, or the domain reads the previous value again on apply")
	require.Empty(t, be.deferred[0].Prev)
	be.ClearDeferred()
}

func shardedFlushTestBranch(tb testing.TB, be *BranchEncoder, row []*cell, bm uint16) commitment.BranchData {
	tb.Helper()
	cellData := generateCellEncodeDataRow(tb, row, bm)
	enc, err := be.EncodeBranch(bm, bm, bm, &cellData)
	require.NoError(tb, err)
	return commitment.BranchData(bytes.Clone(enc))
}

type countingFlushCtx struct {
	commitment.PatriciaContext
	mu      *sync.Mutex
	written map[string][]byte
	prev    map[string][]byte
	calls   *atomic.Int64
}

func (c *countingFlushCtx) PutBranch(prefix, data, prevData []byte) error {
	c.calls.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, dup := c.written[string(prefix)]; dup {
		return fmt.Errorf("prefix %x written twice", prefix)
	}
	c.written[string(prefix)] = bytes.Clone(data)
	c.prev[string(prefix)] = bytes.Clone(prevData)
	return nil
}

func TestApplyDeferredUpdates_ShardedFlushMergesAndWritesEachPrefixOnce(t *testing.T) {
	row, bm, _ := encodeCellRow(t, 16)
	be := NewBranchEncoder(1024)
	raw := shardedFlushTestBranch(t, be, row, bm>>4)
	prev := shardedFlushTestBranch(t, be, row, bm)
	same := shardedFlushTestBranch(t, be, row, bm>>8)

	wantMerged, err := commitment.NewHexBranchMerger(1024).Merge(prev, raw)
	require.NoError(t, err)
	wantMerged = bytes.Clone(wantMerged)

	for _, workers := range []int{1, 4, 16} {
		t.Run("workers"+strconv.Itoa(workers), func(t *testing.T) {
			var mu sync.Mutex
			var calls, made atomic.Int64
			written, prevSeen := map[string][]byte{}, map[string][]byte{}
			factory := func(context.Context) (commitment.PatriciaContext, func()) {
				made.Add(1)
				return &countingFlushCtx{mu: &mu, written: written, prev: prevSeen, calls: &calls}, func() {}
			}

			pu := &commitment.ParallelUpdate{}
			pu.DeferredCombined = append(pu.DeferredCombined,
				commitment.GetDeferredUpdate([]byte{0x01}, raw, prev),
				commitment.GetDeferredUpdate([]byte{0x02}, raw, nil),
				commitment.GetDeferredUpdate([]byte{0x03}, same, same))
			for i := range (workers - 1) * deferredWritesPerWorker {
				pu.DeferredCombined = append(pu.DeferredCombined,
					commitment.GetDeferredUpdate([]byte{0x10, byte(i >> 8), byte(i)}, raw, prev))
			}
			wantWrites := int64(len(pu.DeferredCombined) - 1)

			p := NewParallelPatriciaHashed(factory, length.Addr, commitment.DefaultTrieConfig())
			p.SetNumWorkers(workers)
			require.NoError(t, p.applyDeferredUpdates(context.Background(), pu))

			require.Equal(t, wantWrites, calls.Load(), "every record but the unchanged one is written exactly once")
			require.Len(t, written, int(wantWrites))
			require.Equal(t, []byte(wantMerged), written[string([]byte{0x01})],
				"a sharded worker merges its own record against prev")
			require.Equal(t, []byte(prev), prevSeen[string([]byte{0x01})],
				"prev must survive the merge for the changeset undo record")
			require.Equal(t, []byte(raw), written[string([]byte{0x02})], "an empty prev encodes as raw")
			require.NotContains(t, written, string([]byte{0x03}), "prev==raw stays a skipped write")
			require.Equal(t, int64(workers), made.Load(), "one trie context per flush worker, none shared")
		})
	}
}
