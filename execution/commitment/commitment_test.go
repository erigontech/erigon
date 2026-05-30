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
	"encoding/hex"
	"fmt"
	"math/bits"
	"math/rand"
	"sort"
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
	sleep   time.Duration
	descend bool
	entered chan struct{}
	release chan struct{}
}

func (g *gatedPatriciaContext) Branch(prefix []byte) ([]byte, kv.Step, error) {
	if g.entered != nil {
		g.entered <- struct{}{}
	}
	if g.release != nil {
		<-g.release
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
	for i := 0; i < n; i++ {
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

	for _, mode := range []Mode{ModeDirect, ModeUpdate} {
		name := "ModeDirect"
		if mode == ModeUpdate {
			name = "ModeUpdate"
		}
		t.Run(name, func(t *testing.T) {
			ut := NewUpdates(mode, t.TempDir(), keyHasherNoop)
			for _, k := range genNibbleKeys(numKeys, keyLen) {
				ut.TouchPlainKey(string(k), []byte("v"), ut.TouchStorage)
			}
			require.EqualValues(t, numKeys, ut.Size())

			ctx := context.Background()
			warmuper := NewWarmuper(ctx, WarmupConfig{
				Enabled: true,
				// Large per-level stall keeps the straggler in-flight across the arena reset.
				CtxFactory: slowCtxFactory(2 * time.Millisecond),
				NumWorkers: 4,
				MaxDepth:   64,
				LogPrefix:  "test",
			})
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
}

// TestHashSort_NilWarmuper exercises the nil-warmuper batch-boundary path (the else branch
// that resets the arena directly), crossing the in-loop reset for both modes.
func TestHashSort_NilWarmuper(t *testing.T) {
	t.Parallel()

	const numKeys = 20_000
	const keyLen = 64

	for _, mode := range []Mode{ModeDirect, ModeUpdate} {
		name := "ModeDirect"
		if mode == ModeUpdate {
			name = "ModeUpdate"
		}
		t.Run(name, func(t *testing.T) {
			ut := NewUpdates(mode, t.TempDir(), keyHasherNoop)
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
}

// TestHashSort_WarmupLap crosses ≥3 batch boundaries (K=2) so a ring slot is reused while a slow
// straggler still holds a key from that slot's previous generation; the producer must block in
// WaitBufferFree until it drains. -race is the signal.
func TestHashSort_WarmupLap(t *testing.T) {
	t.Parallel()

	const numKeys = 30_000 // three batch boundaries → gen reaches 3, so each ring slot is reused
	const keyLen = 64

	for _, mode := range []Mode{ModeDirect, ModeUpdate} {
		name := "ModeDirect"
		if mode == ModeUpdate {
			name = "ModeUpdate"
		}
		t.Run(name, func(t *testing.T) {
			ut := NewUpdates(mode, t.TempDir(), keyHasherNoop)
			for _, k := range genNibbleKeys(numKeys, keyLen) {
				ut.TouchPlainKey(string(k), []byte("v"), ut.TouchStorage)
			}
			require.EqualValues(t, numKeys, ut.Size())

			ctx := context.Background()
			warmuper := NewWarmuper(ctx, WarmupConfig{
				Enabled:    true,
				CtxFactory: slowCtxFactory(2 * time.Millisecond),
				NumWorkers: 4,
				MaxDepth:   64,
				LogPrefix:  "test",
			})
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

	for _, mode := range []Mode{ModeDirect, ModeUpdate} {
		name := "ModeDirect"
		if mode == ModeUpdate {
			name = "ModeUpdate"
		}
		t.Run(name, func(t *testing.T) {
			ut := NewUpdates(mode, t.TempDir(), keyHasherNoop)
			for _, k := range genNibbleKeys(numKeys, keyLen) {
				ut.TouchPlainKey(string(k), []byte("v"), ut.TouchStorage)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered := make(chan struct{}, 1)
			release := make(chan struct{})
			warmuper := NewWarmuper(ctx, WarmupConfig{
				Enabled:    true,
				CtxFactory: gatedStragglerFactory(entered, release),
				NumWorkers: 4,
				MaxDepth:   64,
				LogPrefix:  "test",
			})
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
	warmuper := NewWarmuper(context.Background(), WarmupConfig{
		Enabled:    true,
		CtxFactory: gatedCtxFactory(entered, release),
		NumWorkers: 1,
		MaxDepth:   64,
		LogPrefix:  "test",
	})
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
	warmuper := NewWarmuper(ctx, WarmupConfig{
		Enabled:    true,
		CtxFactory: gatedCtxFactory(entered, release),
		NumWorkers: 1,
		MaxDepth:   64,
		LogPrefix:  "test",
	})
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

	warmuper := NewWarmuper(context.Background(), WarmupConfig{
		Enabled:    true,
		CtxFactory: noopCtxFactory,
		NumWorkers: 1,
		MaxDepth:   64,
		LogPrefix:  "test",
	})
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

func generateCellRow(tb testing.TB, size int) (row []*cell, bitmap uint16) {
	tb.Helper()

	row = make([]*cell, size)
	var bm uint16
	for i := 0; i < len(row); i++ {
		row[i] = new(cell)
		row[i].hashLen = 32
		n, err := rand.Read(row[i].hash[:])
		require.NoError(tb, err)
		require.Equal(tb, int(row[i].hashLen), n)

		th := rand.Intn(120)
		switch {
		case th > 70:
			n, err = rand.Read(row[i].accountAddr[:])
			require.NoError(tb, err)
			row[i].accountAddrLen = int16(n)
		case th > 20 && th <= 70:
			n, err = rand.Read(row[i].storageAddr[:])
			require.NoError(tb, err)
			row[i].storageAddrLen = int16(n)
		case th <= 20:
			n, err = rand.Read(row[i].extension[:th])
			row[i].extLen = int16(n)
			require.NoError(tb, err)
			require.Equal(tb, th, n)
		}
		bm |= uint16(1 << i)
	}
	return row, bm
}

// generateCellEncodeDataRow converts a cell row (from generateCellRow) into a [16]cellEncodeData array.
func generateCellEncodeDataRow(tb testing.TB, row []*cell, bm uint16) [16]cellEncodeData {
	tb.Helper()
	var data [16]cellEncodeData
	for bitset := bm; bitset != 0; {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		if nibble < len(row) && row[nibble] != nil {
			data[nibble] = cellEncodeDataFromCell(row[nibble])
		}
		bitset ^= bit
	}
	return data
}

func TestBranchData_MergeHexBranches2(t *testing.T) {
	t.Parallel()
	row, bm := generateCellRow(t, 16)

	be := NewBranchEncoder(1024)
	cellData := generateCellEncodeDataRow(t, row, bm)
	enc, err := be.EncodeBranch(bm, bm, bm, &cellData)

	require.NoError(t, err)
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

// Additional tests for error cases, edge cases, and other scenarios can be added here.

func TestBranchData_MergeHexBranches3(t *testing.T) {
	t.Parallel()

	encs := "0405040b04080f0b080d030204050b0502090805050d01060e060d070f0903090c04070a0d0a000e090b060b0c040c0700020e0b0c060b0106020c0607050a0b0209070d06040808"
	enc, err := hex.DecodeString(encs)
	require.NoError(t, err)

	//tm, am, origins, err := BranchData(enc).decodeCells()
	require.NoError(t, err)
	t.Logf("%s", BranchData(enc).String())
	//require.EqualValues(t, tm, am)
	//_, _ = tm, am
}

func TestDecodeBranchWithLeafHashes(t *testing.T) {
	// enc := "00061614a8f8d73af90eee32dc9729ce8d5bb762f30d21a434a8f8d73af90eee32dc9729ce8d5bb762f30d21a49f49fdd48601f00df18ebc29b1264e27d09cf7cbd514fe8af173e534db038033203c7e2acaef5400189202e1a6a3b0b3d9add71fb52ad24ae35be6b6c85ca78bb51214ba7a3b7b095d3370c022ca655c790f0c0ead66f52025c143802ceb44bbe35e883927edb5933fc33416d4cc354dd88c7bcf1aad66a1"
	// unfoldBranchDataFromString(t, enc)

	row, bm := generateCellRow(t, 16)

	for i := 0; i < len(row); i++ {
		if row[i].accountAddrLen > 0 {
			rand.Read(row[i].stateHash[:])
			row[i].stateHashLen = 32
		}
	}

	be := NewBranchEncoder(1024)
	cellData := generateCellEncodeDataRow(t, row, bm)
	enc, err := be.EncodeBranch(bm, bm, bm, &cellData)
	require.NoError(t, err)

	fmt.Printf("%s\n", enc.String())

}

// helper to decode row of cells from string
func unfoldBranchDataFromString(tb testing.TB, encs string) (row []*cell, am uint16) {
	tb.Helper()

	//encs := "0405040b04080f0b080d030204050b0502090805050d01060e060d070f0903090c04070a0d0a000e090b060b0c040c0700020e0b0c060b0106020c0607050a0b0209070d06040808"
	//encs := "37ad10eb75ea0fc1c363db0dda0cd2250426ee2c72787155101ca0e50804349a94b649deadcc5cddc0d2fd9fb358c2edc4e7912d165f88877b1e48c69efacf418e923124506fbb2fd64823fd41cbc10427c423"
	enc, err := hex.DecodeString(encs)
	require.NoError(tb, err)

	tm, am, origins, err := BranchData(enc).decodeCells()
	require.NoError(tb, err)
	_, _ = tm, am

	tb.Logf("%s", BranchData(enc).String())
	//require.EqualValues(tb, tm, am)
	//for i, c := range origins {
	//	if c == nil {
	//		continue
	//	}
	//	fmt.Printf("i %d, c %#+v\n", i, c)
	//}
	return origins[:], am
}

func TestBranchData_ReplacePlainKeys(t *testing.T) {
	t.Parallel()

	row, bm := generateCellRow(t, 16)

	cells, am := unfoldBranchDataFromString(t, "86e586e5082035e72a782b51d9c98548467e3f868294d923cdbbdf4ce326c867bd972c4a2395090109203b51781a76dc87640aea038e3fdd8adca94049aaa436735b162881ec159f6fb408201aa2fa41b5fb019e8abf8fc32800805a2743cfa15373cf64ba16f4f70e683d8e0404a192d9050404f993d9050404e594d90508208642542ff3ce7d63b9703e85eb924ab3071aa39c25b1651c6dda4216387478f10404bd96d905")
	for i, c := range cells {
		if c == nil {
			continue
		}
		if c.accountAddrLen > 0 {
			offt, _ := binary.Uvarint(c.accountAddr[:c.accountAddrLen])
			t.Logf("%d apk %x, offt %d\n", i, c.accountAddr[:c.accountAddrLen], offt)
		}
		if c.storageAddrLen > 0 {
			offt, _ := binary.Uvarint(c.storageAddr[:c.storageAddrLen])
			t.Logf("%d spk %x offt %d\n", i, c.storageAddr[:c.storageAddrLen], offt)
		}

	}
	_ = cells
	_ = am

	be := NewBranchEncoder(1024)
	cellData := generateCellEncodeDataRow(t, row, bm)
	enc, err := be.EncodeBranch(bm, bm, bm, &cellData)
	require.NoError(t, err)

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

	row, bm := generateCellRow(t, 16)

	be := NewBranchEncoder(1024)
	cellData := generateCellEncodeDataRow(t, row, bm)
	enc, err := be.EncodeBranch(bm, bm, bm, &cellData)
	require.NoError(t, err)

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

	row, bm := generateCellRow(t, 16)
	be := NewBranchEncoder(1024)
	cellData := generateCellEncodeDataRow(t, row, bm)
	enc, err := be.EncodeBranch(bm, bm, bm, &cellData)
	require.NoError(t, err)

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
	for i := 0; i < len(upds); i++ {
		utUpdate.TouchPlainKey(string(upds[i].key), upds[i].val, utUpdate.TouchStorage)
		utDirect.TouchPlainKey(string(upds[i].key), upds[i].val, utDirect.TouchStorage)
	}

	uniqUpds := make(map[string]tc)
	for i := 0; i < len(upds); i++ {
		if _, exist := uniqUpds[string(upds[i].key)]; exist {
			fmt.Printf("deduped %x\n", upds[i].key)
		}
		uniqUpds[string(upds[i].key)] = upds[i]
	}
	sortedUniqUpds := make([]tc, 0, len(uniqUpds))
	for _, v := range uniqUpds {
		sortedUniqUpds = append(sortedUniqUpds, v)
	}
	sort.Slice(sortedUniqUpds, func(i, j int) bool {
		return bytes.Compare(sortedUniqUpds[i].key, sortedUniqUpds[j].key) < 0
	})

	sz := utUpdate.Size()
	require.EqualValues(t, len(uniqUpds), sz)

	sz = utDirect.Size()
	require.EqualValues(t, len(uniqUpds), sz)

	ctx := context.Background()
	cfg := WarmupConfig{
		Enabled:    true,
		CtxFactory: noopCtxFactory,
		NumWorkers: 2,
		MaxDepth:   64,
		LogPrefix:  "test",
	}
	warmuper := NewWarmuper(ctx, cfg)
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
	cfg2 := WarmupConfig{
		Enabled:    true,
		CtxFactory: noopCtxFactory,
		NumWorkers: 2,
		MaxDepth:   64,
		LogPrefix:  "test",
	}
	warmuper2 := NewWarmuper(ctx, cfg2)
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
