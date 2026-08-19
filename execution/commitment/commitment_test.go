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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
)

type noopPatriciaContext struct{}

func (n *noopPatriciaContext) Branch(prefix []byte) ([]byte, kv.Step, error) { return nil, 0, nil }
func (n *noopPatriciaContext) PutBranch(prefix, data, prevData []byte) error {
	return nil
}
func (n *noopPatriciaContext) Account(plainKey []byte) (*Update, error) { return nil, nil }
func (n *noopPatriciaContext) Storage(plainKey []byte) (*Update, error) { return nil, nil }
func (n *noopPatriciaContext) TxNum() uint64                            { return 0 }

func noopCtxFactory(context.Context) (PatriciaContext, func()) {
	return &noopPatriciaContext{}, nil
}

type gatedPatriciaContext struct {
	sleep    time.Duration
	descend  bool
	entered  chan struct{}
	release  chan struct{}
	gateDone atomic.Bool
}

func (g *gatedPatriciaContext) Branch(prefix []byte) ([]byte, kv.Step, error) {
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
		return []byte{0, 0, 0, 1, 0, 0}, 0, nil
	}
	return []byte{0, 0, 0, 0}, 0, nil
}

func (g *gatedPatriciaContext) PutBranch(prefix, data, prevData []byte) error { return nil }
func (g *gatedPatriciaContext) Account(plainKey []byte) (*Update, error)      { return nil, nil }
func (g *gatedPatriciaContext) Storage(plainKey []byte) (*Update, error)      { return nil, nil }
func (g *gatedPatriciaContext) TxNum() uint64                                 { return 0 }

func slowCtxFactory(stall time.Duration) TrieContextFactory {
	var n atomic.Int32
	return func(context.Context) (PatriciaContext, func()) {
		if n.Add(1) == 1 {
			return &gatedPatriciaContext{sleep: stall, descend: true}, nil
		}
		return &gatedPatriciaContext{}, nil
	}
}

func gatedCtxFactory(entered, release chan struct{}) TrieContextFactory {
	return func(context.Context) (PatriciaContext, func()) {
		return &gatedPatriciaContext{entered: entered, release: release}, nil
	}
}

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

func TestHashSort_WarmupArenaNoRace(t *testing.T) {
	t.Parallel()

	const numKeys = 20_000
	const keyLen = 64

	forEachMode(t, func(t *testing.T, mode Mode) {
		ut := NewUpdates(mode, t.TempDir(), keyHasherNoop)
		forceDirectSpill(ut)
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
		require.NoError(t, warmuper.Wait())
	})
}

func TestHashSort_NilWarmuper(t *testing.T) {
	t.Parallel()

	const numKeys = 20_000
	const keyLen = 64

	forEachMode(t, func(t *testing.T, mode Mode) {
		ut := NewUpdates(mode, t.TempDir(), keyHasherNoop)
		forceDirectSpill(ut)
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

func TestHashSort_WarmupLap(t *testing.T) {
	t.Parallel()

	const numKeys = 30_000
	const keyLen = 64

	forEachMode(t, func(t *testing.T, mode Mode) {
		ut := NewUpdates(mode, t.TempDir(), keyHasherNoop)
		forceDirectSpill(ut)
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
		require.GreaterOrEqual(t, ut.gen, uint64(3))
		require.NoError(t, warmuper.Wait())
	})
}

func gatedStragglerFactory(entered, release chan struct{}) TrieContextFactory {
	var n atomic.Int32
	return func(context.Context) (PatriciaContext, func()) {
		if n.Add(1) == 1 {
			return &gatedPatriciaContext{entered: entered, release: release}, nil
		}
		return &gatedPatriciaContext{}, nil
	}
}

func TestHashSort_WaitBufferFreeErrorKeepsArenaInvariant(t *testing.T) {
	t.Parallel()

	const numKeys = 30_000
	const keyLen = 64
	const lapFnCall = 2 * hashSortBatchSize

	forEachMode(t, func(t *testing.T, mode Mode) {
		ut := NewUpdates(mode, t.TempDir(), keyHasherNoop)
		forceDirectSpill(ut)
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

		<-entered
		require.GreaterOrEqual(t, warmuper.outstanding[0].Load(), int64(1))

		<-reachedLap
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

// TestUpdateDecodeRefusesOversizedStorage: the storage length is a varint on the
// wire but an int8 in the struct, so a value past the field's own width has to
// be refused at the bound check rather than wrap negative.
func TestUpdateDecodeRefusesOversizedStorage(t *testing.T) {
	t.Parallel()

	for _, storageLen := range []uint64{uint64(length.Hash) + 1, 200} {
		buf := []byte{byte(StorageUpdate)}
		buf = binary.AppendUvarint(buf, storageLen)
		buf = append(buf, bytes.Repeat([]byte{0xAA}, int(storageLen))...)

		var u Update
		_, err := u.Decode(buf, 0)
		require.Error(t, err, "storage len %d", storageLen)
	}
}

func TestUpdates_ArenaAlloc(t *testing.T) {
	t.Parallel()

	ut := NewUpdates(ModeDirect, t.TempDir(), keyHasherNoop)
	ut.arenaEnsureCap(16)

	a := ut.arenaAlloc([]byte("aaaa"))
	b := ut.arenaAlloc([]byte("bbbb"))
	require.Equal(t, []byte("aaaa"), a)
	require.Equal(t, []byte("bbbb"), b)

	require.Equal(t, &ut.arenas[ut.curArena][0], &a[0])
	require.Equal(t, &ut.arenas[ut.curArena][4], &b[0])

	b[0] = 'X'
	require.Equal(t, []byte("aaaa"), a)

	big := ut.arenaAlloc(bytes.Repeat([]byte("z"), 32))
	require.Equal(t, bytes.Repeat([]byte("z"), 32), big)
	require.Equal(t, []byte("aaaa"), a)
	require.Equal(t, []byte("Xbbb"), b)
	require.NotEqual(t, &ut.arenas[ut.curArena][0], &big[0])
}

func TestWarmuper_WaitBufferFree_BlocksUntilStragglerDone(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{})
	release := make(chan struct{})
	warmuper := testWarmuper(context.Background(), gatedCtxFactory(entered, release), 1)
	warmuper.Start()
	defer func() { require.NoError(t, warmuper.Wait()) }()

	warmuper.WarmKey([]byte{0, 1, 2, 3}, 0, 0)
	<-entered
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

	close(release)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("WaitBufferFree did not return after the straggler drained")
	}
	require.Equal(t, int64(0), warmuper.outstanding[0].Load())
}

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
	<-entered
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

	var buf BranchData = make([]byte, 4)
	binary.BigEndian.PutUint16(buf[0:], 0xffff)
	binary.BigEndian.PutUint16(buf[2:], 0b0000_0000_0000_0111)
	require.Equal(t, 3, buf.ChildCount())
}

func TestBranchData_IsComplete(t *testing.T) {
	t.Parallel()

	// Buffers shorter than the 4-byte touchMap+afterMap header are not complete
	// and must not panic. Empty values are deletion tombstones kept across merges.
	require.False(t, BranchData(nil).IsComplete())
	require.False(t, BranchData{}.IsComplete())
	require.False(t, BranchData{0x00}.IsComplete())
	require.False(t, BranchData{0xff, 0xff, 0x00}.IsComplete())

	complete := make(BranchData, 4)
	binary.BigEndian.PutUint16(complete[0:], 0xffff)
	binary.BigEndian.PutUint16(complete[2:], 0b0000_0000_0000_0111)
	require.True(t, complete.IsComplete())

	incomplete := make(BranchData, 4)
	binary.BigEndian.PutUint16(incomplete[0:], 0b0000_0000_0000_0001)
	binary.BigEndian.PutUint16(incomplete[2:], 0b0000_0000_0000_0011)
	require.False(t, incomplete.IsComplete())
}

func TestBranchData_MergeHexBranchesEmptyBranches(t *testing.T) {
	t.Parallel()

	merger := NewHexBranchMerger(1024)

	branch1 := BranchData{}
	branch2 := BranchData{0x02, 0x02, 0x03, 0x03, 0x0C, 0x02, 0x04, 0x0C}
	mergedBranch, err := merger.Merge(branch1, branch2)
	require.NoError(t, err)
	require.Equal(t, branch2, mergedBranch)

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
	replaced, err := BranchData(bytes.Clone(enc)).ReplacePlainKeys(
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
		bytes.Clone(prefix), bytes.Clone(data), bytes.Clone(prev),
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

	ctxA := &recordingCtx{}
	beA := NewBranchEncoder(1024)
	require.NoError(t, beA.CollectUpdate(ctxA, prefix, bm, bm, bm, &cells, false))
	require.Equal(t, 1, ctxA.branchCalls, "isNew=false must probe Branch")
	require.Len(t, ctxA.puts, 1)

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

	ctxA := &recordingCtx{}
	beA := NewBranchEncoder(1024)
	beA.setDeferUpdates(true)
	require.NoError(t, beA.CollectDeferredUpdate(ctxA, prefix, bm, bm, bm, &cells, false))
	require.NoError(t, beA.ApplyDeferredUpdates(1, ctxA.PutBranch))
	require.Equal(t, 1, ctxA.branchCalls, "isNew=false must probe Branch")
	require.Len(t, ctxA.puts, 1)

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

// TestCollectDeferredUpdate_PoolRecycleDoesNotCorruptEarlierApply pins the
// contract documented on getDeferredUpdate: putDeferredUpdate recycles the
// prefix/raw backing arrays for a later, unrelated update, so a PutBranch
// implementation that copies (as recordingCtx and every real implementation
// do) must see its own copy stay correct across that recycle.
// Not parallel: it swaps the global deferredUpdatePool for a deterministic one, since
// sync.Pool may hand back a fresh object and leave the recycle unexercised.
func TestCollectDeferredUpdate_PoolRecycleDoesNotCorruptEarlierApply(t *testing.T) {
	seed := &DeferredBranchUpdate{}
	saved := deferredUpdatePool
	deferredUpdatePool = &sync.Pool{New: func() any { return seed }}
	t.Cleanup(func() { deferredUpdatePool = saved })

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

	require.NoError(t, be.CollectDeferredUpdate(ctx, []byte{0xAA, 0xAA}, bmA, bmA, bmA, &cellsA, true))
	require.NoError(t, be.ApplyDeferredUpdates(1, ctx.PutBranch))
	be.ClearDeferred() // recycles this round's DeferredBranchUpdate into the pool

	require.Len(t, ctx.puts, 1)
	require.Equal(t, wantA, ctx.puts[0].data)

	// A shorter, distinctive second round reuses the pool's backing arrays.
	require.NoError(t, be.CollectDeferredUpdate(ctx, []byte{0xBB, 0xBB}, bmB, bmB, bmB, &cellsB, true))
	require.Same(t, seed, be.deferred[0], "second round must run on the recycled object, or it proves nothing")
	require.NoError(t, be.ApplyDeferredUpdates(1, ctx.PutBranch))
	be.ClearDeferred()

	require.Len(t, ctx.puts, 2)
	require.Equal(t, wantA, ctx.puts[0].data, "recycling the pool must not corrupt data already handed to PutBranch")
	require.Equal(t, wantB, ctx.puts[1].data, "reused buffer must not retain stale bytes from the earlier, longer round")
}

func TestUpdates_TouchStorageClearsDeleteOnRewrite(t *testing.T) {
	t.Parallel()

	updates := NewUpdates(ModeUpdate, t.TempDir(), keyHasherNoop)
	key := "storage-key"

	updates.TouchPlainKey(key, nil, updates.TouchStorage)
	updates.TouchPlainKey(key, []byte("value"), updates.TouchStorage)

	// treeIdx (plainKey→KeyUpdate) is required here: the btree orders by hashedKey first,
	// so scanning it with only plainKey set as pivot returns nothing.
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

// reuseBytes is what makes the pooled buffers reusable; sync.Pool itself guarantees
// nothing, so the reuse is pinned here rather than through a pool round-trip.
func TestReuseBytes(t *testing.T) {
	dst := make([]byte, 0, 8)
	got := reuseBytes(dst, []byte{1, 2, 3})
	require.Equal(t, []byte{1, 2, 3}, got)
	require.Same(t, &dst[:1][0], &got[0], "must write into dst's backing array")

	require.Nil(t, reuseBytes(dst, nil), "nil src must yield nil, as bytes.Clone does")
	require.Nil(t, reuseBytes(nil, nil))

	// nil-ness must depend only on src, never on whether dst carries capacity
	for _, d := range [][]byte{nil, make([]byte, 0, 8), make([]byte, 4)} {
		require.NotNil(t, reuseBytes(d, []byte{}), "empty src must stay non-nil for any dst")
		require.Empty(t, reuseBytes(d, []byte{}))
	}

	require.Equal(t, []byte{1, 2, 3, 4}, reuseBytes(make([]byte, 0, 1), []byte{1, 2, 3, 4}))
}

// prev is cloned rather than recycled precisely so its nil-ness follows the input; this
// fails if it is ever switched to a pooled buffer. putDeferredUpdate clears prev, so a
// warm buffer has to be seeded through the pool rather than by a Put/Get round trip.
func TestGetDeferredUpdate_WarmPoolPreservesNilPrev(t *testing.T) {
	seed := &DeferredBranchUpdate{prev: make([]byte, 0, 32)}
	saved := deferredUpdatePool
	deferredUpdatePool = &sync.Pool{New: func() any { return seed }}
	t.Cleanup(func() { deferredUpdatePool = saved })

	upd := getDeferredUpdate([]byte{1}, []byte{2, 3}, nil)
	defer putDeferredUpdate(upd)
	require.Same(t, seed, upd)
	require.Nil(t, upd.prev)
}

// Pins the production path, not the helper: sync.Pool may hand back a fresh object, so the
// pool is swapped for one that always yields a known object with known backing arrays.
func TestGetDeferredUpdate_WritesIntoPooledBacking(t *testing.T) {
	seed := &DeferredBranchUpdate{
		prefix: make([]byte, 0, 32),
		raw:    make([]byte, 0, 32),
		prev:   make([]byte, 0, 32),
	}
	prefixArr, rawArr := &seed.prefix[:1][0], &seed.raw[:1][0]

	saved := deferredUpdatePool
	deferredUpdatePool = &sync.Pool{New: func() any { return seed }}
	t.Cleanup(func() { deferredUpdatePool = saved })

	upd := getDeferredUpdate([]byte{1, 2}, []byte{3, 4, 5}, []byte{6})
	require.Same(t, seed, upd)
	require.Same(t, prefixArr, &upd.prefix[0], "prefix must be written into the pooled array")
	require.Same(t, rawArr, &upd.raw[0], "raw must be written into the pooled array")
}

func TestCapLen(t *testing.T) {
	t.Parallel()
	require.Nil(t, capLen(nil))
	big := make([]byte, 3, 64)
	require.Equal(t, 3, cap(capLen(big)))
	require.Equal(t, 0, cap(capLen(big[:0])))
}

// A callback must not see capacity left over from whichever update used the object before.
// Driven through ApplyDeferredBranchUpdates rather than capLen, so dropping the clip at
// either the serial or the parallel call site fails here.
func TestApplyDeferred_CallbackSeesInputDerivedCapacity(t *testing.T) {
	t.Parallel()

	upd := func(prefix, raw byte) *DeferredBranchUpdate {
		return &DeferredBranchUpdate{
			prefix: append(make([]byte, 0, 64), prefix),
			raw:    append(make(BranchData, 0, 64), raw),
			prev:   make([]byte, 0, 64),
		}
	}
	deferred := func(n int) []*DeferredBranchUpdate {
		out := make([]*DeferredBranchUpdate, n)
		for i := range out {
			out[i] = upd(byte(i), byte(i+1))
		}
		return out
	}

	// numWorkers == 1 takes the serial path; 5 updates over 2 workers takes the parallel one.
	for _, tc := range []struct {
		name             string
		updates, workers int
	}{
		{"serial", 2, 1},
		{"parallel", 5, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var seen int
			written, err := ApplyDeferredBranchUpdates(deferred(tc.updates), tc.workers,
				func(prefix, data, prevData []byte) error {
					seen++
					require.Equal(t, len(prefix), cap(prefix), "prefix carries leftover pool capacity")
					require.Equal(t, len(data), cap(data), "data carries leftover pool capacity")
					require.Equal(t, len(prevData), cap(prevData), "prevData carries leftover pool capacity")
					return nil
				})
			require.NoError(t, err)
			require.Equal(t, tc.updates, written)
			require.Equal(t, tc.updates, seen, "callback must run for every update")
		})
	}
}
