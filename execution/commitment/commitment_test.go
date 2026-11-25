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
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/common/empty"
	"github.com/holiman/uint256"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
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

func TestCollectedHashedKeysIncludeHashedTouches(t *testing.T) {
	u := NewUpdates(ModeCollect, t.TempDir(), KeyToHexNibbleHash)
	account := []byte("0123456789abcdefghij")
	other := []byte("jihgfedcba9876543210")
	u.TouchPlainKey(string(account), nil, func(*KeyUpdate, []byte) {})
	u.TouchPlainKeyDirect(string(account), &Update{Flags: NonceUpdate, Nonce: 1})
	u.TouchPlainKeyDirect(string(other), &Update{Flags: NonceUpdate, Nonce: 2})
	partial := []byte{0x3, 0x0, 0xa}
	u.TouchHashedKey(partial)
	u.TouchHashedKey(partial)

	want := [][]byte{partial, KeyToHexNibbleHash(account), KeyToHexNibbleHash(other)}
	slices.SortFunc(want, bytes.Compare)
	require.Equal(t, want, u.CollectedHashedKeys())
}

func TestTouchPlainKeyDirectCollectMergesRepeatedTouches(t *testing.T) {
	u := NewUpdates(ModeCollect, t.TempDir(), KeyToHexNibbleHash)
	account := "0123456789abcdefghij"
	first := &Update{Flags: BalanceUpdate, DeleteStorageSubtree: true}
	first.Balance.SetUint64(5)
	u.TouchPlainKeyDirect(account, first)
	u.TouchPlainKeyDirect(account, &Update{Flags: NonceUpdate, Nonce: 7})

	var got []*Update
	require.NoError(t, u.Drain(func(_ string, upd *Update) error {
		got = append(got, upd)
		return nil
	}))
	require.Len(t, got, 1)
	require.True(t, got[0].DeleteStorageSubtree)
	require.Equal(t, BalanceUpdate|NonceUpdate, got[0].Flags)
	require.Equal(t, uint64(5), got[0].Balance.Uint64())
	require.Equal(t, uint64(7), got[0].Nonce)
}

type gatedPatriciaContext struct {
	sleep       time.Duration
	descend     bool
	entered     chan struct{}
	release     chan struct{}
	startOthers chan struct{}
	gateDone    atomic.Bool
}

func (g *gatedPatriciaContext) Branch(prefix []byte) ([]byte, kv.Step, error) {
	if (g.entered != nil || g.release != nil) && !g.gateDone.Swap(true) {
		if g.startOthers != nil {
			close(g.startOthers)
		}
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
		warmuper.CloseAndWait()
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
		warmuper.CloseAndWait()
	})
}

func gatedStragglerFactory(entered, release chan struct{}) TrieContextFactory {
	var n atomic.Int32
	startOthers := make(chan struct{})
	return func(ctx context.Context) (PatriciaContext, func()) {
		if n.Add(1) == 1 {
			return &gatedPatriciaContext{entered: entered, release: release, startOthers: startOthers}, nil
		}
		select {
		case <-startOthers:
		case <-ctx.Done():
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
	defer warmuper.CloseAndWait()

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
	defer warmuper.CloseAndWait()

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

	warmuper.CloseAndWait()

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

	warmuper2.CloseAndWait()
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
	got := entry.Update

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
	require.NotNil(t, ut.Parallel, "parallel field must be allocated")
	require.NotNil(t, ut.Parallel.Trie, "parallel trie must be allocated")
	require.NotNil(t, ut.keys, "keys dedup map must be allocated")
	require.Nil(t, ut.tree)
	require.Nil(t, ut.treeIdx)
	require.Nil(t, ut.etl, "ModeParallel uses the prefix trie, not any ETL collector")
	require.Equal(t, ModeParallel, ut.Mode(), "collection must stay in ModeParallel")
	require.Equal(t, uint64(0), ut.Size())
}

func requireParallelCount(t *testing.T, ut *Updates, n int, msg string) {
	t.Helper()
	require.Equal(t, uint64(n), ut.Size())
	ut.Parallel.Build()
	require.EqualValues(t, n, ut.Parallel.Trie.Root.SubtreeCount, msg)
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

	requireParallelCount(t, ut, len(keys), "every touched key must show up in the prefix trie")

	ut.TouchPlainKey(string(keys[0]), []byte("v2"), ut.TouchStorage)
	requireParallelCount(t, ut, len(keys), "duplicate TouchPlainKey must not double-count in the trie")
}

func TestUpdatesModeParallel_RepeatTouchDoesNotRehash(t *testing.T) {
	t.Parallel()

	var hashCalls atomic.Int64
	hasher := func(key []byte) []byte {
		hashCalls.Add(1)
		return KeyToHexNibbleHash(key)
	}
	require.False(t, hasherReusesAddrPrefix(hasher), "wrapped hasher must not alias KeyToHexNibbleHash")

	ut := NewUpdates(ModeParallel, t.TempDir(), hasher)
	defer ut.Close()

	key := string(common.FromHex("c17fa85f22306d37cec90b0ec74c5623dbbac68f"))
	for range 3 {
		ut.TouchPlainKey(key, nil, func(c *KeyUpdate, val []byte) {})
	}

	require.EqualValues(t, 1, hashCalls.Load(), "a repeat touch must not rehash the key")
	requireParallelCount(t, ut, 1, "the single touched key must reach the trie")
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

	requireParallelCount(t, ut, 2, "a repeated hashed key must not double-count in the trie")
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
	requireParallelCount(t, ut, 2, "both touched keys must reach the trie")

	ut.Reset()

	require.Equal(t, uint64(0), ut.Size())
	require.NotNil(t, ut.Parallel, "Reset must not release parallel field")
	require.NotNil(t, ut.Parallel.Trie)
	require.NotNil(t, ut.Parallel.Trie.Root, "trie root must be re-allocated after Reset")
	require.EqualValues(t, 0, ut.Parallel.Trie.Root.SubtreeCount, "trie counts cleared after Reset")
	require.EqualValues(t, 0, ut.Parallel.Trie.Root.Bitmap, "trie bitmap cleared after Reset")

	for _, k := range keys {
		ut.TouchPlainKey(string(k), []byte("v"), ut.TouchStorage)
	}
	requireParallelCount(t, ut, 2, "a collection reused after Reset must refill the trie")
}

func TestUpdatesModeParallel_Close(t *testing.T) {
	t.Parallel()

	ut := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)

	ut.TouchPlainKey(string(common.FromHex("c17fa85f22306d37cec90b0ec74c5623dbbac68f")), []byte("v"), ut.TouchStorage)

	ut.Close()
	require.Nil(t, ut.Parallel, "Close must release parallel field")
}

func TestUpdatesModeParallel_SetMode(t *testing.T) {
	t.Parallel()

	ut := NewUpdates(ModeDirect, t.TempDir(), KeyToHexNibbleHash)
	defer ut.Close()
	require.Nil(t, ut.Parallel)

	ut.SetMode(ModeParallel)
	require.Equal(t, ModeParallel, ut.mode)
	require.NotNil(t, ut.Parallel)
	require.Equal(t, uint64(0), ut.Size())

	prev := ut.Parallel
	ut.SetMode(ModeParallel)
	require.Same(t, prev, ut.Parallel)
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
// fails if it is ever switched to a pooled buffer. PutDeferredUpdate clears prev, so a
// warm buffer has to be seeded through the pool rather than by a Put/Get round trip.
func TestGetDeferredUpdate_WarmPoolPreservesNilPrev(t *testing.T) {
	seed := &DeferredBranchUpdate{Prev: make([]byte, 0, 32)}
	saved := DeferredUpdatePool
	DeferredUpdatePool = &sync.Pool{New: func() any { return seed }}
	t.Cleanup(func() { DeferredUpdatePool = saved })

	upd := GetDeferredUpdate([]byte{1}, []byte{2, 3}, nil)
	defer PutDeferredUpdate(upd)
	require.Same(t, seed, upd)
	require.Nil(t, upd.Prev)
}

// Pins the production path, not the helper: sync.Pool may hand back a fresh object, so the
// pool is swapped for one that always yields a known object with known backing arrays.
func TestGetDeferredUpdate_WritesIntoPooledBacking(t *testing.T) {
	seed := &DeferredBranchUpdate{
		Prefix: make([]byte, 0, 32),
		Raw:    make([]byte, 0, 32),
		Prev:   make([]byte, 0, 32),
	}
	prefixArr, rawArr := &seed.Prefix[:1][0], &seed.Raw[:1][0]

	saved := DeferredUpdatePool
	DeferredUpdatePool = &sync.Pool{New: func() any { return seed }}
	t.Cleanup(func() { DeferredUpdatePool = saved })

	upd := GetDeferredUpdate([]byte{1, 2}, []byte{3, 4, 5}, []byte{6})
	require.Same(t, seed, upd)
	require.Same(t, prefixArr, &upd.Prefix[0], "prefix must be written into the pooled array")
	require.Same(t, rawArr, &upd.Raw[0], "raw must be written into the pooled array")
}

func TestCapLen(t *testing.T) {
	t.Parallel()
	require.Nil(t, CapLen(nil))
	big := make([]byte, 3, 64)
	require.Equal(t, 3, cap(CapLen(big)))
	require.Equal(t, 0, cap(CapLen(big[:0])))
}

// A callback must not see capacity left over from whichever update used the object before.
// Driven through ApplyDeferredBranchUpdates rather than CapLen, so dropping the clip at
// either the serial or the parallel call site fails here.
func TestApplyDeferred_CallbackSeesInputDerivedCapacity(t *testing.T) {
	t.Parallel()

	upd := func(prefix, raw byte) *DeferredBranchUpdate {
		return &DeferredBranchUpdate{
			Prefix: append(make([]byte, 0, 64), prefix),
			Raw:    append(make(BranchData, 0, 64), raw),
			Prev:   make([]byte, 0, 64),
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
				}, nil)
			require.NoError(t, err)
			require.Equal(t, tc.updates, written)
			require.Equal(t, tc.updates, seen, "callback must run for every update")
		})
	}
}

// More keys than one batch, so the mid-loop truncation runs too, not just the
// trailing one.
func TestHashSortLeavesNoBatchSlabEntriesBehind(t *testing.T) {
	t.Parallel()

	upd := NewUpdates(ModeUpdate, t.TempDir(), keyHasherNoop)
	for i := range hashSortBatchSize + 1 {
		upd.TouchPlainKey(strconv.Itoa(i), []byte("v"), upd.TouchStorage)
	}

	require.NoError(t, upd.HashSort(context.Background(), nil, func(hk, pk []byte, u *Update) error { return nil }))

	require.NotZero(t, cap(upd.batchSlab), "no batch ran, so the test proves nothing")
	for i, ku := range upd.batchSlab[:cap(upd.batchSlab)] {
		require.Nil(t, ku.Update, "batchSlab[%d] still pins an Update after HashSort", i)
		require.Nil(t, ku.HashedKey, "batchSlab[%d] still pins a hashed key after HashSort", i)
		require.Empty(t, ku.PlainKey, "batchSlab[%d] still pins a plain key after HashSort", i)
	}
}

// Reset recycles the updates into the pool, so it must detach the slice the way
// every other drain of DeferredCombined does.
func TestParallelUpdateResetDetachesDeferred(t *testing.T) {
	t.Parallel()

	pu := NewParallelUpdate()
	pu.AppendDeferred([]*DeferredBranchUpdate{
		GetDeferredUpdate([]byte{1}, make([]byte, 4096), nil),
		GetDeferredUpdate([]byte{2}, make([]byte, 4096), nil),
	})

	pu.Reset()

	require.Nil(t, pu.DeferredCombined, "Reset still pins the recycled updates")
}

func TestUpdate_EncodeDecode(t *testing.T) {
	t.Parallel()

	updates := []Update{
		{Flags: BalanceUpdate, Balance: *uint256.NewInt(123), CodeHash: empty.CodeHash},
		{Flags: BalanceUpdate | NonceUpdate, Balance: *uint256.NewInt(45639015), Nonce: 123, CodeHash: empty.CodeHash},
		{
			Flags: BalanceUpdate | NonceUpdate | CodeUpdate, Balance: *uint256.NewInt(45639015), Nonce: 123,
			CodeHash: common.Hash{
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
				0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
				0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
				0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20,
			},
		},
		{Flags: StorageUpdate, Storage: common.Hash{0x21, 0x22, 0x23, 0x24}, StorageLen: 4, CodeHash: empty.CodeHash},
		{Flags: DeleteUpdate, CodeHash: empty.CodeHash},
	}

	var numBuf [10]byte
	for i, update := range updates {
		encoded := update.Encode(nil, numBuf[:])

		decoded := Update{}
		n, err := decoded.Decode(encoded, 0)
		require.NoError(t, err, i)
		require.Equal(t, len(encoded), n, i)

		require.Equal(t, update.Flags, decoded.Flags, i)
		require.Equal(t, update.Balance, decoded.Balance, i)
		require.Equal(t, update.Nonce, decoded.Nonce, i)
		require.Equal(t, update.CodeHash, decoded.CodeHash, i)
		require.Equal(t, update.Storage, decoded.Storage, i)
		require.Equal(t, update.StorageLen, decoded.StorageLen, i)
	}
}

func TestUpdate_Merge(t *testing.T) {
	type tcase struct {
		a, b, e Update
	}

	updates := []tcase{
		{
			a: Update{Flags: BalanceUpdate, Balance: *uint256.NewInt(123), CodeHash: empty.CodeHash},
			b: Update{Flags: BalanceUpdate | NonceUpdate, Balance: *uint256.NewInt(45639015), Nonce: 123, CodeHash: empty.CodeHash},
			e: Update{Flags: BalanceUpdate | NonceUpdate, Balance: *uint256.NewInt(45639015), Nonce: 123, CodeHash: empty.CodeHash},
		},
		{
			a: Update{Flags: BalanceUpdate | NonceUpdate, Balance: *uint256.NewInt(45639015), Nonce: 123, CodeHash: empty.CodeHash},
			b: Update{Flags: BalanceUpdate | NonceUpdate | CodeUpdate, Balance: *uint256.NewInt(1000000), Nonce: 547, CodeHash: empty.CodeHash},
			e: Update{Flags: BalanceUpdate | NonceUpdate | CodeUpdate, Balance: *uint256.NewInt(1000000), Nonce: 547, CodeHash: empty.CodeHash},
		},
		{
			a: Update{Flags: BalanceUpdate | NonceUpdate | CodeUpdate, Balance: *uint256.NewInt(4568314), Nonce: 123, CodeHash: empty.CodeHash},
			b: Update{
				Flags: BalanceUpdate | NonceUpdate | CodeUpdate, Balance: *uint256.NewInt(45639015), Nonce: 124,
				CodeHash: common.Hash{
					0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
					0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
					0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
					0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20,
				},
			},
			e: Update{Flags: BalanceUpdate | NonceUpdate | CodeUpdate, Balance: *uint256.NewInt(45639015), Nonce: 124, CodeHash: common.Hash{
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
				0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
				0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
				0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20,
			}},
		},
		{
			a: Update{Flags: StorageUpdate, Storage: common.Hash{0x21, 0x22, 0x23, 0x24}, StorageLen: 4, CodeHash: empty.CodeHash},
			b: Update{Flags: DeleteUpdate, CodeHash: empty.CodeHash},
			e: Update{Flags: DeleteUpdate, CodeHash: empty.CodeHash},
		},
		{
			a: Update{Flags: DeleteUpdate, CodeHash: empty.CodeHash},
			b: Update{Flags: StorageUpdate, Storage: common.Hash{0x21, 0x22, 0x23, 0x24}, StorageLen: 4, CodeHash: empty.CodeHash},
			e: Update{Flags: StorageUpdate, Storage: common.Hash{0x21, 0x22, 0x23, 0x24}, StorageLen: 4, CodeHash: empty.CodeHash},
		},
	}

	var numBuf [10]byte
	for i, tc := range updates {
		tc.a.Merge(&tc.b)
		encA := tc.a.Encode(nil, numBuf[:])
		encE := tc.e.Encode(nil, numBuf[:])
		require.Equal(t, encE, encA, i)
	}
}

func TestParallelPatriciaHashedSkeletonParseTrieVariant(t *testing.T) {
	assert.Equal(t, VariantParallelHexPatricia, ParseTrieVariant("parallel"))
	assert.Equal(t, VariantHexPatriciaTrie, ParseTrieVariant("hex"))
	assert.Equal(t, VariantHexPatriciaTrie, ParseTrieVariant("nonsense"))
	assert.Equal(t, VariantCommitmentV3, ParseTrieVariant("v3"))
}
