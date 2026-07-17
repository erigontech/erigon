// Copyright 2024 The Erigon Authors
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

package commitment

import (
	"bytes"
	"context"
	"encoding/binary"
	"math/bits"
	"math/rand"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
)

// noopPatriciaContext is a mock PatriciaContext for testing warmup.
type noopPatriciaContext struct{}

func (n *noopPatriciaContext) Branch(prefix []byte) ([]byte, kv.Step, error) { return nil, 0, nil }
func (n *noopPatriciaContext) PutBranch(prefix, data, prevData []byte) error {
	return nil
}
func (n *noopPatriciaContext) Account(plainKey []byte) (*Update, error) { return nil, nil }
func (n *noopPatriciaContext) Storage(plainKey []byte) (*Update, error) { return nil, nil }
func (n *noopPatriciaContext) TxNum() uint64                            { return 0 }

func noopCtxFactory() (PatriciaContext, func()) {
	return &noopPatriciaContext{}, nil
}

// gatedPatriciaContext is a mock PatriciaContext with a controllable in-flight window:
// sleep+descend keep a worker re-reading its arena-backed key across batch boundaries,
// while entered/release gate a worker inside Branch for deterministic ordering.
type gatedPatriciaContext struct {
	sleep    time.Duration
	descend  bool
	entered  chan struct{}
	release  chan struct{}
	gateDone atomic.Bool
}

func (g *gatedPatriciaContext) Branch(prefix []byte) ([]byte, kv.Step, error) {
	// Gate only the first Branch call so a released worker can't wedge re-sending to entered.
	if (g.entered != nil || g.release != nil) && !g.gateDone.Swap(true) {
		if g.entered != nil {
			g.entered <- struct{}{}
		}
		if g.release != nil {
			<-g.release
		}
	}
	if g.sleep > 0 {
		time.Sleep(g.sleep)
	}
	if g.descend {
		// touch map + bitmap 0x0001 (child nibble 0) + fieldBits 0x00: warmupKey
		// descends on nibble 0, re-reading hashedKey at every level.
		return []byte{0, 0, 0, 1, 0, 0}, 0, nil
	}
	return []byte{0, 0, 0, 0}, 0, nil
}

func (g *gatedPatriciaContext) PutBranch(prefix, data, prevData []byte) error { return nil }
func (g *gatedPatriciaContext) Account(plainKey []byte) (*Update, error)      { return nil, nil }
func (g *gatedPatriciaContext) Storage(plainKey []byte) (*Update, error)      { return nil, nil }
func (g *gatedPatriciaContext) TxNum() uint64                                 { return 0 }

// slowCtxFactory makes the first worker a slow straggler that holds one key across many
// batch resets while the rest run fast, so the producer's arena reset races its in-flight reads.
func slowCtxFactory(stall time.Duration) TrieContextFactory {
	var n atomic.Int32
	return func() (PatriciaContext, func()) {
		if n.Add(1) == 1 {
			return &gatedPatriciaContext{sleep: stall, descend: true}, nil
		}
		return &gatedPatriciaContext{}, nil
	}
}

// gatedCtxFactory returns a factory whose contexts signal entered then block on
// release inside Branch, for deterministic single-worker ordering tests.
func gatedCtxFactory(entered, release chan struct{}) TrieContextFactory {
	return func() (PatriciaContext, func()) {
		return &gatedPatriciaContext{entered: entered, release: release}, nil
	}
}

// genNibbleKeys produces n unique keyLen-byte keys whose every byte is a valid nibble
// (0x00-0x0F), with the index encoded in the trailing nibbles so keys are distinct.
func genNibbleKeys(n, keyLen int) [][]byte {
	keys := make([][]byte, n)
	for i := range n {
		k := make([]byte, keyLen)
		v := i
		for j := keyLen - 1; j >= 0; j-- {
			k[j] = byte(v & 0x0F)
			v >>= 4
		}
		keys[i] = k
	}
	return keys
}

// TestHashSort_WarmupArenaNoRace reproduces the arena data race: at a batch boundary HashSort
// resets a buffer while warmup workers still read key slices aliasing it. -race is the signal.
func TestHashSort_WarmupArenaNoRace(t *testing.T) {
	t.Parallel()

	const numKeys = 20_000 // two batches: one in-loop arena reset mid-stream plus the final batch
	const keyLen = 64

	forEachMode(t, func(t *testing.T, mode Mode) {
		ut := NewUpdates(mode, t.TempDir(), keyHasherNoop)
		forceDirectSpill(ut) // these tests pin the arena/etl path
		for _, k := range genNibbleKeys(numKeys, keyLen) {
			ut.TouchPlainKey(string(k), []byte("v"), ut.TouchStorage)
		}
		require.EqualValues(t, numKeys, ut.Size())

		ctx := context.Background()
		// Large per-level stall keeps the straggler in-flight across the arena reset.
		warmuper := testWarmuper(ctx, slowCtxFactory(2*time.Millisecond), 4)
		warmuper.Start()

		visited := 0
		err := ut.HashSort(ctx, warmuper, func(hk, pk []byte, _ *Update) error {
			visited++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, numKeys, visited)
		require.NoError(t, warmuper.Wait())
	})
}

// TestHashSort_NilWarmuper exercises the nil-warmuper batch-boundary path (the else branch
// that resets the arena directly), crossing the in-loop reset for both modes.
func TestHashSort_NilWarmuper(t *testing.T) {
	t.Parallel()

	const numKeys = 20_000
	const keyLen = 64

	forEachMode(t, func(t *testing.T, mode Mode) {
		ut := NewUpdates(mode, t.TempDir(), keyHasherNoop)
		forceDirectSpill(ut) // these tests pin the arena/etl path
		for _, k := range genNibbleKeys(numKeys, keyLen) {
			ut.TouchPlainKey(string(k), []byte("v"), ut.TouchStorage)
		}
		require.EqualValues(t, numKeys, ut.Size())

		visited := 0
		err := ut.HashSort(context.Background(), nil, func(hk, pk []byte, _ *Update) error {
			visited++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, numKeys, visited)
	})
}

// TestHashSort_WarmupLap crosses ≥3 batch boundaries (K=2) so a ring slot is reused while a slow
// straggler still holds a key from that slot's previous generation; the producer must block in
// WaitBufferFree until it drains. -race is the signal.
func TestHashSort_WarmupLap(t *testing.T) {
	t.Parallel()

	const numKeys = 30_000 // three batch boundaries → gen reaches 3, so each ring slot is reused
	const keyLen = 64

	forEachMode(t, func(t *testing.T, mode Mode) {
		ut := NewUpdates(mode, t.TempDir(), keyHasherNoop)
		forceDirectSpill(ut) // these tests pin the arena/etl path
		for _, k := range genNibbleKeys(numKeys, keyLen) {
			ut.TouchPlainKey(string(k), []byte("v"), ut.TouchStorage)
		}
		require.EqualValues(t, numKeys, ut.Size())

		ctx := context.Background()
		warmuper := testWarmuper(ctx, slowCtxFactory(2*time.Millisecond), 4)
		warmuper.Start()

		visited := 0
		err := ut.HashSort(ctx, warmuper, func(hk, pk []byte, _ *Update) error {
			visited++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, numKeys, visited)
		// gen advances once per batch boundary; ≥3 means at least one ring slot was
		// reused (lapped) — the path WaitBufferFree guards.
		require.GreaterOrEqual(t, ut.gen, uint64(3))
		require.NoError(t, warmuper.Wait())
	})
}

// gatedStragglerFactory makes the first worker block inside Branch on release (holding its
// first key) while every other worker runs fast, so exactly one ring slot stays occupied.
func gatedStragglerFactory(entered, release chan struct{}) TrieContextFactory {
	var n atomic.Int32
	return func() (PatriciaContext, func()) {
		if n.Add(1) == 1 {
			return &gatedPatriciaContext{entered: entered, release: release}, nil
		}
		return &gatedPatriciaContext{}, nil
	}
}

// TestHashSort_WaitBufferFreeErrorKeepsArenaInvariant cancels the context during a boundary
// WaitBufferFree while a straggler pins the slot, asserting the curArena == gen % arenaRingSize
// invariant survives the error return.
func TestHashSort_WaitBufferFreeErrorKeepsArenaInvariant(t *testing.T) {
	t.Parallel()

	const numKeys = 30_000 // ≥3 batch boundaries so a ring slot is reused (lapped)
	const keyLen = 64
	const lapFnCall = 2 * hashSortBatchSize // fn calls for gen 0 + gen 1, completing right before boundary 2

	forEachMode(t, func(t *testing.T, mode Mode) {
		ut := NewUpdates(mode, t.TempDir(), keyHasherNoop)
		forceDirectSpill(ut) // these tests pin the arena/etl path
		for _, k := range genNibbleKeys(numKeys, keyLen) {
			ut.TouchPlainKey(string(k), []byte("v"), ut.TouchStorage)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		entered := make(chan struct{}, 1)
		release := make(chan struct{})
		warmuper := testWarmuper(ctx, gatedStragglerFactory(entered, release), 4)
		warmuper.Start()
		defer warmuper.CloseAndWait()
		defer close(release)

		// fn runs only on the producer goroutine, so this counter is race-free. Signaling at
		// lapFnCall (right before the gen++/WaitBufferFree block) makes the cancel land inside the wait.
		fnCalls := 0
		reachedLap := make(chan struct{})
		errCh := make(chan error, 1)
		go func() {
			errCh <- ut.HashSort(ctx, warmuper, func(hk, pk []byte, _ *Update) error {
				fnCalls++
				if fnCalls == lapFnCall {
					close(reachedLap)
				}
				return nil
			})
		}()

		<-entered // the straggler holds a gen-0 key, pinning slot 0
		require.GreaterOrEqual(t, warmuper.outstanding[0].Load(), int64(1))

		<-reachedLap // batch-2 fn-loop done; producer heads into WaitBufferFree(0), which slot 0 pins
		cancel()

		select {
		case err := <-errCh:
			require.Error(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("HashSort did not return after cancellation")
		}

		require.Equal(t, int(ut.gen%arenaRingSize), ut.curArena)
	})
}

// TestUpdates_ArenaAlloc verifies that sequential allocations within a ring buffer return
// non-overlapping sub-slices, and that an over-capacity request falls back to an independent
// allocation that leaves prior sub-slices intact.
func TestUpdates_ArenaAlloc(t *testing.T) {
	t.Parallel()

	ut := NewUpdates(ModeDirect, t.TempDir(), keyHasherNoop)
	ut.arenaEnsureCap(16)

	a := ut.arenaAlloc([]byte("aaaa"))
	b := ut.arenaAlloc([]byte("bbbb"))
	require.Equal(t, []byte("aaaa"), a)
	require.Equal(t, []byte("bbbb"), b)

	// Sub-slices are contiguous and non-overlapping within the same buffer.
	require.Equal(t, &ut.arenas[ut.curArena][0], &a[0])
	require.Equal(t, &ut.arenas[ut.curArena][4], &b[0])

	// Mutating the second slice must not touch the first.
	b[0] = 'X'
	require.Equal(t, []byte("aaaa"), a)

	// Over-capacity request falls back to an independent allocation; prior slices stay valid.
	big := ut.arenaAlloc(bytes.Repeat([]byte("z"), 32))
	require.Equal(t, bytes.Repeat([]byte("z"), 32), big)
	require.Equal(t, []byte("aaaa"), a)
	require.Equal(t, []byte("Xbbb"), b)
	// The fallback slice is not backed by the current ring buffer.
	require.NotEqual(t, &ut.arenas[ut.curArena][0], &big[0])
}

// TestWarmuper_WaitBufferFree_BlocksUntilStragglerDone verifies that WaitBufferFree
// blocks while a warm item for the slot's generation is still in-flight, and returns
// once that item completes (slot drains to zero).
func TestWarmuper_WaitBufferFree_BlocksUntilStragglerDone(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{})
	release := make(chan struct{})
	warmuper := testWarmuper(context.Background(), gatedCtxFactory(entered, release), 1)
	warmuper.Start()
	defer func() { require.NoError(t, warmuper.Wait()) }()

	warmuper.WarmKey([]byte{0, 1, 2, 3}, 0, 0)
	<-entered // worker is now inside Branch, key for gen 0 in-flight
	require.Equal(t, int64(1), warmuper.outstanding[0].Load())

	done := make(chan struct{})
	go func() {
		_ = warmuper.WaitBufferFree(0)
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("WaitBufferFree returned while a gen-0 item is still in-flight")
	case <-time.After(50 * time.Millisecond):
	}

	close(release) // let the worker finish

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("WaitBufferFree did not return after the straggler drained")
	}
	require.Equal(t, int64(0), warmuper.outstanding[0].Load())
}

// TestWarmuper_WaitBufferFree_UnblocksOnCancel verifies a producer parked in WaitBufferFree wakes
// and returns the context error when the warmuper is canceled while a counted item is stuck.
func TestWarmuper_WaitBufferFree_UnblocksOnCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	entered := make(chan struct{})
	release := make(chan struct{})
	warmuper := testWarmuper(ctx, gatedCtxFactory(entered, release), 1)
	warmuper.Start()
	defer warmuper.CloseAndWait()
	defer close(release)

	warmuper.WarmKey([]byte{0, 1, 2, 3}, 0, 0)
	<-entered // worker is inside Branch holding the gen-0 item; slot 0 counter is 1
	require.Equal(t, int64(1), warmuper.outstanding[0].Load())

	errCh := make(chan error, 1)
	go func() { errCh <- warmuper.WaitBufferFree(0) }()

	select {
	case <-errCh:
		t.Fatal("WaitBufferFree returned before cancellation while the slot is in-flight")
	case <-time.After(50 * time.Millisecond):
	}

	cancel()

	select {
	case err := <-errCh:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("WaitBufferFree did not return after the context was canceled")
	}
}

// TestWarmuper_WaitBufferFree_FastPath verifies WaitBufferFree returns immediately when
// the slot is already drained.
func TestWarmuper_WaitBufferFree_FastPath(t *testing.T) {
	t.Parallel()

	warmuper := testWarmuper(context.Background(), noopCtxFactory, 1)
	warmuper.Start()
	defer func() { require.NoError(t, warmuper.Wait()) }()

	done := make(chan struct{})
	go func() {
		_ = warmuper.WaitBufferFree(0)
		_ = warmuper.WaitBufferFree(1)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("WaitBufferFree did not fast-path return on an already-drained slot")
	}
}

func TestBranchData_MergeHexBranches2(t *testing.T) {
	t.Parallel()
	row, bm, enc := encodeCellRow(t, 16)
	require.NotEmpty(t, enc)
	t.Logf("enc [%d] %x\n", len(enc), enc)

	bmg := NewHexBranchMerger(8192)
	res, err := bmg.Merge(enc, enc)
	require.NoError(t, err)
	require.Equal(t, enc, res)

	tm, am, origins, err := res.decodeCells()
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

	require.Equal(t, 0, BranchData(nil).ChildCount())
	require.Equal(t, 0, BranchData{}.ChildCount())
	require.Equal(t, 0, BranchData{0xff, 0xff, 0x00}.ChildCount(), "buffer shorter than 4 bytes has no afterMap")

	for _, size := range []int{1, 2, 5, 16} {
		_, bm, enc := encodeCellRow(t, size)
		require.Equal(t, size, bits.OnesCount16(bm))
		require.Equal(t, size, enc.ChildCount(), "ChildCount must equal the number of afterMap children")
	}

	// ChildCount counts afterMap (bytes 2:4), not touchMap (bytes 0:2).
	var buf BranchData = make([]byte, 4)
	binary.BigEndian.PutUint16(buf[0:], 0xffff)
	binary.BigEndian.PutUint16(buf[2:], 0b0000_0000_0000_0111)
	require.Equal(t, 3, buf.ChildCount())
}

func TestBranchData_MergeHexBranchesEmptyBranches(t *testing.T) {
	t.Parallel()

	// Create a BranchMerger instance with sufficient capacity for testing.
	merger := NewHexBranchMerger(1024)

	// Test merging when one branch is empty.
	branch1 := BranchData{}
	branch2 := BranchData{0x02, 0x02, 0x03, 0x03, 0x0C, 0x02, 0x04, 0x0C}
	mergedBranch, err := merger.Merge(branch1, branch2)
	require.NoError(t, err)
	require.Equal(t, branch2, mergedBranch)

	// Test merging when both branches are empty.
	branch1 = BranchData{}
	branch2 = BranchData{}
	mergedBranch, err = merger.Merge(branch1, branch2)
	require.NoError(t, err)
	require.Equal(t, branch1, mergedBranch)
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

	original := common.Copy(enc)

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
		orig := common.Copy(original)

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

	original := common.Copy(enc)

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
		orig := common.Copy(original)

		merged, err := replaced.MergeHexBranches(original, nil)
		require.NoError(t, err)
		require.EqualValues(t, orig, merged)

		merged, err = merged.MergeHexBranches(replacedBack, nil)
		require.NoError(t, err)
		require.EqualValues(t, orig, merged)
	})
}

// TestBranchData_ReplacePlainKeys_PartialChange exercises the span-copy logic
// when only some keys change (account keys shortened, storage keys kept).
func TestBranchData_ReplacePlainKeys_PartialChange(t *testing.T) {
	t.Parallel()

	_, _, enc := encodeCellRow(t, 16)

	original := common.Copy(enc)

	// Collect original keys and shorten only account keys.
	type keyRecord struct {
		key       []byte
		isStorage bool
	}
	var origKeys []keyRecord
	replaced, err := BranchData(common.Copy(enc)).ReplacePlainKeys(
		make([]byte, 0, len(enc)),
		func(key []byte, isStorage bool) ([]byte, error) {
			origKeys = append(origKeys, keyRecord{common.Copy(key), isStorage})
			if isStorage {
				return nil, nil // keep original
			}
			return key[:4], nil // shorten account keys
		},
	)
	require.NoError(t, err)

	// Expand back: restore account keys, keep storage keys.
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

func TestNewUpdates(t *testing.T) {
	t.Parallel()

	t.Run("ModeUpdate", func(t *testing.T) {
		ut := NewUpdates(ModeUpdate, t.TempDir(), keyHasherNoop)

		require.NotNil(t, ut.tree)
		require.Nil(t, ut.keys)
		require.Equal(t, ModeUpdate, ut.mode)
	})

	t.Run("ModeDirect", func(t *testing.T) {
		ut := NewUpdates(ModeDirect, t.TempDir(), keyHasherNoop)

		require.NotNil(t, ut.keys)
		require.Equal(t, ModeDirect, ut.mode)
	})

}

func TestUpdates_TouchPlainKey(t *testing.T) {
	t.Parallel()

	utUpdate := NewUpdates(ModeUpdate, t.TempDir(), keyHasherNoop)
	utDirect := NewUpdates(ModeDirect, t.TempDir(), keyHasherNoop)

	type tc struct {
		key []byte
		val []byte
	}

	upds := []tc{
		{common.FromHex("c17fa85f22306d37cec90b0ec74c5623dbbac68f"), []byte("value1")},
		{common.FromHex("553bba1d92398a69fbc9f01593bbc51b58862366"), []byte("value0")},
		{common.FromHex("553bba1d92398a69fbc9f01593bbc51b58862366"), []byte("value8")},
		{common.FromHex("2452345febefe553bba1d92398a69fbc9f01593b"), []byte("value8")},
		{common.FromHex("ffffffffffff8a69fbc9f01593bbc51b58862366"), []byte("value8")},
		{common.FromHex("553bba1d92398a69fbc9f01593bbceeeeeeeee66"), []byte("value8")},
		{common.FromHex("553bba1d9239aaaaaaaaa01593bbc51b58862366"), []byte("value8")},
		{common.FromHex("553bba1d92398a69fbc9f01593bb777777777777"), []byte("value8")},
		{common.FromHex("5cccccccccccca69fbc9f01593bbc51b58862366"), []byte("value8")},
		{common.FromHex("553bba1d92398a69fbc9feeeeeeee51b58862366"), []byte("value8")},
		{common.FromHex("553bba1d9bbbbbbbbbbbbb1593bbc51b58862366"), []byte("value8")},
		{common.FromHex("553bba1d9ffffffffffff01593bbc51b5aaaaaaa"), []byte("value8")},
		{common.FromHex("97c780315e7820752006b7a918ce7ec023df263a87a715b64d5ab445e1782a760a974f8810551f81dfb7f1425f7d8358332af195"), []byte("value1")},
		{common.FromHex("97c780315e7820752006b7a918ce7ec023df263a87a715b64d5ab445e1782a760a974f881055fffffffff1425f7d8358332af195"), []byte("value1")},
		{common.FromHex("97c780315e7820752006b7a918ce7ec023df263a87a715b64d5ab445e1782a760a974f8810551f81dfb7eeeeeeeeeeeeeeeeee95"), []byte("value1")},
		{common.FromHex("97c780315e7820752006b7a918ce7ec023df263a87a715b64d5ab445e1782a760a974aaaaaaa1f81dfb7f1425f7d8358332af195"), []byte("value1")},
		{common.FromHex("97c780315e7820752006b7a918ce7ec023df263a87a715b64d5ab445e1782a760a974f8810551f81dfb7f1425f7d835838888885"), []byte("value1")},
	}
	for i := range upds {
		utUpdate.TouchPlainKey(string(upds[i].key), upds[i].val, utUpdate.TouchStorage)
		utDirect.TouchPlainKey(string(upds[i].key), upds[i].val, utDirect.TouchStorage)
	}

	uniqUpds := make(map[string]tc)
	for i := range upds {
		uniqUpds[string(upds[i].key)] = upds[i]
	}
	sortedUniqUpds := make([]tc, 0, len(uniqUpds))
	for _, v := range uniqUpds {
		sortedUniqUpds = append(sortedUniqUpds, v)
	}
	slices.SortFunc(sortedUniqUpds, func(a, b tc) int {
		return bytes.Compare(a.key, b.key)
	})

	sz := utUpdate.Size()
	require.EqualValues(t, len(uniqUpds), sz)

	sz = utDirect.Size()
	require.EqualValues(t, len(uniqUpds), sz)

	ctx := context.Background()
	warmuper := testWarmuper(ctx, noopCtxFactory, 2)
	warmuper.Start()

	i := 0
	// keyHasherNoop is used so ordering is going by plainKey
	err := utUpdate.HashSort(ctx, warmuper, func(hk, pk []byte, upd *Update) error {
		require.Equal(t, sortedUniqUpds[i].key, pk)
		require.Equal(t, sortedUniqUpds[i].val, upd.Storage[:upd.StorageLen])
		i++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, len(uniqUpds), i)

	err = warmuper.Wait()
	require.NoError(t, err)

	// Create a new warmuper for the second test
	warmuper2 := testWarmuper(ctx, noopCtxFactory, 2)
	warmuper2.Start()

	i = 0
	err = utDirect.HashSort(ctx, warmuper2, func(hk, pk []byte, _ *Update) error {
		require.Equal(t, sortedUniqUpds[i].key, pk)
		i++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, len(uniqUpds), i)

	err = warmuper2.Wait()
	require.NoError(t, err)
}

// recordingCtx captures Branch call count and PutBranch arguments for assertions.
type recordingCtx struct {
	branchCalls int
	puts        []struct{ prefix, data, prev []byte }
}

func (r *recordingCtx) Branch(_ []byte) ([]byte, kv.Step, error) {
	r.branchCalls++
	return nil, 0, nil
}
func (r *recordingCtx) PutBranch(prefix, data, prev []byte) error {
	r.puts = append(r.puts, struct{ prefix, data, prev []byte }{
		common.Copy(prefix), common.Copy(data), common.Copy(prev),
	})
	return nil
}
func (r *recordingCtx) Account(_ []byte) (*Update, error) { return nil, nil }
func (r *recordingCtx) Storage(_ []byte) (*Update, error) { return nil, nil }
func (r *recordingCtx) TxNum() uint64                     { return 0 }

func TestCollectUpdate_IsNewSkipsLookupAndMatchesNilPath(t *testing.T) {
	t.Parallel()
	prefix := []byte{0xab, 0xcd}
	row, bm := generateCellRow(t, 4)
	cells := generateCellEncodeDataRow(t, row, bm)

	// isNew=false: Branch is probed but returns nil (key doesn't exist yet)
	ctxA := &recordingCtx{}
	beA := NewBranchEncoder(1024)
	require.NoError(t, beA.CollectUpdate(ctxA, prefix, bm, bm, bm, &cells, false))
	require.Equal(t, 1, ctxA.branchCalls, "isNew=false must probe Branch")
	require.Len(t, ctxA.puts, 1)

	// isNew=true: Branch must not be called, but PutBranch output must be identical
	ctxB := &recordingCtx{}
	beB := NewBranchEncoder(1024)
	require.NoError(t, beB.CollectUpdate(ctxB, prefix, bm, bm, bm, &cells, true))
	require.Equal(t, 0, ctxB.branchCalls, "isNew=true must not probe Branch")
	require.Len(t, ctxB.puts, 1)

	require.Equal(t, ctxA.puts[0].data, ctxB.puts[0].data)
	require.Equal(t, ctxA.puts[0].prev, ctxB.puts[0].prev)
}

func TestCollectDeferredUpdate_IsNewSkipsLookupAndMatchesNilPath(t *testing.T) {
	t.Parallel()
	prefix := []byte{0x11, 0x22}
	row, bm := generateCellRow(t, 4)
	cells := generateCellEncodeDataRow(t, row, bm)

	// isNew=false: Branch is probed but returns nil
	ctxA := &recordingCtx{}
	beA := NewBranchEncoder(1024)
	beA.setDeferUpdates(true)
	require.NoError(t, beA.CollectDeferredUpdate(ctxA, prefix, bm, bm, bm, &cells, false))
	require.NoError(t, beA.ApplyDeferredUpdates(1, ctxA.PutBranch))
	require.Equal(t, 1, ctxA.branchCalls, "isNew=false must probe Branch")
	require.Len(t, ctxA.puts, 1)

	// isNew=true: Branch must not be called, deferred output must match
	ctxB := &recordingCtx{}
	beB := NewBranchEncoder(1024)
	beB.setDeferUpdates(true)
	require.NoError(t, beB.CollectDeferredUpdate(ctxB, prefix, bm, bm, bm, &cells, true))
	require.NoError(t, beB.ApplyDeferredUpdates(1, ctxB.PutBranch))
	require.Equal(t, 0, ctxB.branchCalls, "isNew=true must not probe Branch")
	require.Len(t, ctxB.puts, 1)

	require.Equal(t, ctxA.puts[0].data, ctxB.puts[0].data)
	require.Equal(t, ctxA.puts[0].prev, ctxB.puts[0].prev)
}

func TestUpdates_TouchStorageClearsDeleteOnRewrite(t *testing.T) {
	t.Parallel()

	updates := NewUpdates(ModeUpdate, t.TempDir(), keyHasherNoop)
	key := "storage-key"

	updates.TouchPlainKey(key, nil, updates.TouchStorage)
	updates.TouchPlainKey(key, []byte("value"), updates.TouchStorage)

	// Look up via treeIdx (the plainKey→KeyUpdate map). The btree's
	// comparator (keyUpdateLessFn) orders entries by hashedKey first with
	// plainKey as a tiebreaker, so scanning the tree with a pivot that has
	// only plainKey set returns nothing — treeIdx is the right access path
	// for plainKey lookups.
	entry, ok := updates.treeIdx[key]
	require.True(t, ok, "key should be present after TouchPlainKey rewrite")
	got := entry.update

	require.NotNil(t, got)
	require.Equal(t, StorageUpdate, got.Flags)
	require.False(t, got.Deleted())
	require.Equal(t, int8(len("value")), got.StorageLen)
	require.Equal(t, []byte("value"), got.Storage[:got.StorageLen])
}

func TestModeString(t *testing.T) {
	t.Parallel()

	require.Equal(t, "disabled", ModeDisabled.String())
	require.Equal(t, "direct", ModeDirect.String())
	require.Equal(t, "update", ModeUpdate.String())
	require.Equal(t, "parallel", ModeParallel.String())
	require.Equal(t, "unknown", Mode(99).String())
}

func TestUpdatesModeParallel_NewAllocates(t *testing.T) {
	t.Parallel()

	ut := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer ut.Close()

	require.Equal(t, ModeParallel, ut.mode)
	require.NotNil(t, ut.parallel, "parallel field must be allocated")
	require.NotNil(t, ut.parallel.trie, "parallel trie must be allocated")
	require.NotNil(t, ut.keys, "keys dedup map must be allocated")
	require.Nil(t, ut.tree)
	require.Nil(t, ut.treeIdx)
	require.Nil(t, ut.etl, "ModeParallel uses the prefix trie, not any ETL collector")
	require.True(t, ut.IsConcurrentCommitment(), "IsConcurrentCommitment must report true for ModeParallel")
	require.Equal(t, uint64(0), ut.Size())
}

func TestUpdatesModeParallel_TouchPlainKeyRoutes(t *testing.T) {
	t.Parallel()

	ut := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer ut.Close()

	keys := [][]byte{
		common.FromHex("c17fa85f22306d37cec90b0ec74c5623dbbac68f"),
		common.FromHex("553bba1d92398a69fbc9f01593bbc51b58862366"),
		common.FromHex("2452345febefe553bba1d92398a69fbc9f01593b"),
		common.FromHex("ffffffffffff8a69fbc9f01593bbc51b58862366"),
	}
	for _, k := range keys {
		ut.TouchPlainKey(string(k), []byte("v"), ut.TouchStorage)
	}

	require.Equal(t, uint64(len(keys)), ut.Size())
	require.NotNil(t, ut.parallel.trie.root)
	require.EqualValues(t, len(keys), ut.parallel.trie.root.subtreeCount,
		"every touched key must show up in the prefix trie")

	ut.TouchPlainKey(string(keys[0]), []byte("v2"), ut.TouchStorage)
	require.Equal(t, uint64(len(keys)), ut.Size())
	require.EqualValues(t, len(keys), ut.parallel.trie.root.subtreeCount,
		"duplicate TouchPlainKey must not double-count in the trie")
}

func TestUpdatesModeParallel_TouchHashedKey(t *testing.T) {
	t.Parallel()

	ut := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer ut.Close()

	hk1 := KeyToHexNibbleHash(common.FromHex("c17fa85f22306d37cec90b0ec74c5623dbbac68f"))
	hk2 := KeyToHexNibbleHash(common.FromHex("553bba1d92398a69fbc9f01593bbc51b58862366"))

	ut.TouchHashedKey(hk1)
	ut.TouchHashedKey(hk2)
	ut.TouchHashedKey(hk1)

	require.Equal(t, uint64(2), ut.Size())
	require.EqualValues(t, 2, ut.parallel.trie.root.subtreeCount)
}

func TestUpdatesModeParallel_Reset(t *testing.T) {
	t.Parallel()

	ut := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer ut.Close()

	keys := [][]byte{
		common.FromHex("c17fa85f22306d37cec90b0ec74c5623dbbac68f"),
		common.FromHex("553bba1d92398a69fbc9f01593bbc51b58862366"),
	}
	for _, k := range keys {
		ut.TouchPlainKey(string(k), []byte("v"), ut.TouchStorage)
	}
	require.Equal(t, uint64(2), ut.Size())
	require.EqualValues(t, 2, ut.parallel.trie.root.subtreeCount)

	ut.Reset()

	require.Equal(t, uint64(0), ut.Size())
	require.NotNil(t, ut.parallel, "Reset must not release parallel field")
	require.NotNil(t, ut.parallel.trie)
	require.NotNil(t, ut.parallel.trie.root, "trie root must be re-allocated after Reset")
	require.EqualValues(t, 0, ut.parallel.trie.root.subtreeCount, "trie counts cleared after Reset")
	require.EqualValues(t, 0, ut.parallel.trie.root.bitmap, "trie bitmap cleared after Reset")

	for _, k := range keys {
		ut.TouchPlainKey(string(k), []byte("v"), ut.TouchStorage)
	}
	require.Equal(t, uint64(2), ut.Size())
	require.EqualValues(t, 2, ut.parallel.trie.root.subtreeCount)
}

func TestUpdatesModeParallel_Close(t *testing.T) {
	t.Parallel()

	ut := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)

	ut.TouchPlainKey(string(common.FromHex("c17fa85f22306d37cec90b0ec74c5623dbbac68f")), []byte("v"), ut.TouchStorage)

	ut.Close()
	require.Nil(t, ut.parallel, "Close must release parallel field")
}

func TestUpdatesModeParallel_SetMode(t *testing.T) {
	t.Parallel()

	ut := NewUpdates(ModeDirect, t.TempDir(), KeyToHexNibbleHash)
	defer ut.Close()
	require.Nil(t, ut.parallel)

	ut.SetMode(ModeParallel)
	require.Equal(t, ModeParallel, ut.mode)
	require.NotNil(t, ut.parallel)
	require.Equal(t, uint64(0), ut.Size())

	prev := ut.parallel
	ut.SetMode(ModeParallel)
	require.Same(t, prev, ut.parallel)
}

func TestInitializeTrieAndUpdates_ParallelVariant(t *testing.T) {
	t.Parallel()

	cfg := DefaultTrieConfig()
	cfg.Variant = VariantParallelHexPatricia
	trie, upd := InitializeTrieAndUpdates(ModeDirect, t.TempDir(), cfg)
	defer upd.Close()
	defer trie.Release()

	require.IsType(t, (*ParallelPatriciaHashed)(nil), trie)
	require.Equal(t, VariantParallelHexPatricia, trie.Variant())
	// Parallel variant forces ModeParallel regardless of the mode argument.
	require.Equal(t, ModeParallel, upd.Mode())
	require.NotNil(t, upd.parallel)
	require.True(t, upd.IsConcurrentCommitment())
}

func TestInitializeTrieAndUpdates_HexVariantUnchanged(t *testing.T) {
	t.Parallel()

	cfg := DefaultTrieConfig()
	cfg.Variant = VariantHexPatriciaTrie
	trie, upd := InitializeTrieAndUpdates(ModeDirect, t.TempDir(), cfg)
	defer upd.Close()
	defer trie.Release()

	require.IsType(t, (*HexPatriciaHashed)(nil), trie)
	require.Equal(t, VariantHexPatriciaTrie, trie.Variant())
	require.Equal(t, ModeDirect, upd.Mode())
	require.Nil(t, upd.parallel)
}
