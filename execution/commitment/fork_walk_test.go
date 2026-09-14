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

package commitment

import (
	"bytes"
	"context"
	"encoding/hex"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

type cancelOnAccountContext struct {
	noopPatriciaContext
	cancel context.CancelFunc
}

func (c *cancelOnAccountContext) Account([]byte) (*Update, error) {
	c.cancel()
	return &Update{Flags: DeleteUpdate}, nil
}

func TestForkGrain_Auto(t *testing.T) {
	require.EqualValues(t, minForkGrain, forkGrainFor(0, 16))
	require.EqualValues(t, minForkGrain, forkGrainFor(8_192, 16))
	require.EqualValues(t, 15_625, forkGrainFor(1_000_000, 16))
	require.EqualValues(t, 62_500, forkGrainFor(1_000_000, 4))
}

func TestForkWalk_CancelledContextStopsBeforeNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	w := NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig())
	w.ResetContext(&noopPatriciaContext{})
	defer w.Release()

	fw := forkWalk{grain: ForkGrainNever}
	node := &prefixNode{plainKey: make([]byte, length.Addr), update: &Update{Flags: BalanceUpdate}}
	err := fw.walk(ctx, &walker{trie: w}, node, make([]byte, 64))

	require.ErrorIs(t, err, context.Canceled)
}

func TestForkWalk_CancelledDuringNodeStopsBeforeChildren(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	w := NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig())
	w.ResetContext(&cancelOnAccountContext{cancel: cancel})
	defer w.Release()

	fw := forkWalk{grain: ForkGrainNever}
	node := &prefixNode{plainKey: make([]byte, length.Addr)}
	err := fw.walk(ctx, &walker{trie: w}, node, make([]byte, 64))

	require.ErrorIs(t, err, context.Canceled)
}

type forkCorpus struct {
	name     string
	k1, k2   [][]byte
	u1, u2   []Update
	minForks uint64
}

type survivorKind uint8

const (
	survivorEOA survivorKind = iota
	survivorBareRoot
	survivorExtRoot
)

func keyedSurvivorCorpus(kind survivorKind) (k1 [][]byte, u1 []Update, k2 [][]byte, u2 []Update) {
	rnd := rand.New(rand.NewSource(515151))
	ub := NewUpdateBuilder()
	var doomed [][]byte
	for _, tail := range []byte{0x0, 0x4, 0x8} {
		a := findAddressForHexPrefix([]byte{0x1, 0x2, 0x3, tail}, int(tail)+301)
		doomed = append(doomed, a)
		ub.Balance(addrHex(a), uint64(tail)+7)
	}

	surv := findAddressForHexPrefix([]byte{0x1, 0x2, 0x3, 0xf}, 359)
	sa := addrHex(surv)
	ub.Balance(sa, 4242)
	var locs []string
	switch kind {
	case survivorBareRoot:
		locs = append(slotLocsForHexPrefix([]byte{0x2}, 1, 12_000), slotLocsForHexPrefix([]byte{0x9}, 1, 12_000)...)
	case survivorExtRoot:
		locs = slotLocsForHexPrefix([]byte{0xa, 0xb}, 2, 640_000)
	}
	for i, loc := range locs {
		ub.Storage(sa, loc, hex.EncodeToString([]byte{byte(i) + 1}))
	}

	var touch [][]byte
	for _, p := range [][]byte{{0x1, 0x2, 0x7}, {0x1, 0x2, 0xc}, {0x1, 0x7}, {0x1, 0xc}} {
		for seed := range 2 {
			a := findAddressForHexPrefix(p, seed*17+401)
			ub.Balance(addrHex(a), uint64(seed)+53)
			if seed == 0 && (len(p) == 2 || p[2] == 0x7) {
				touch = append(touch, a)
			}
		}
	}
	for range 400 {
		addRandomAccount(ub, rnd, 0)
	}
	k1, u1 = ub.Build()

	next := NewUpdateBuilder()
	for _, a := range doomed {
		next.Delete(addrHex(a))
	}
	for i, a := range touch {
		next.Balance(addrHex(a), uint64(i)+7001)
	}
	k2, u2 = next.Build()
	return k1, u1, k2, u2
}

func forkExtremeCorpora() []forkCorpus {
	mk1, mu1 := buildMixedCorpus(20260904, 1_200)
	mk2, mu2 := buildMixedCorpus(20260905, 400)

	wk1, wu1, wk2, wu2 := buildSubsetTouchedWhale(20260906, nibs(0, 1, 2, 3), nibs(1, 3), 60, 200)
	fk, fu := buildMixedCorpus(4242, 150)
	wk1 = append(append([][]byte{}, fk...), wk1...)
	wu1 = append(append([]Update{}, fu...), wu1...)

	sk1, su1, sk2, su2 := whaleSurvivorCorpus(false)

	dk1, du1 := buildMixedCorpus(20260907, 900)
	dk2, du2 := emptyRegionAllDeleteRound(20260908)

	ak1, au1 := buildMixedCorpus(20260909, 900)
	ak2, au2 := emptyPrefixAllDeleteRound(ak1)

	bk1, bu1, bk2, bu2 := keyedSurvivorCorpus(survivorBareRoot)
	xk1, xu1, xk2, xu2 := keyedSurvivorCorpus(survivorExtRoot)
	ek1, eu1, ek2, eu2 := keyedSurvivorCorpus(survivorEOA)
	ok1, ou1, ok2, ou2 := absentDeleteCorpus(false)
	rk1, ru1, rk2, ru2 := absentDeleteCorpus(true)

	return []forkCorpus{
		{"mixed", mk1, mk2, mu1, mu2, 2},
		{"subsetTouchedWhale", wk1, wk2, wu1, wu2, 2},
		{"survivorCollapse", sk1, sk2, su1, su2, 2},
		{"emptyRegionAllDeletes", dk1, dk2, du1, du2, 1},
		{"emptyPrefixAllDeletes", ak1, ak2, au1, au2, 1},
		{"keyedSurvivorBareRoot", bk1, bk2, bu1, bu2, 2},
		{"keyedSurvivorExtRoot", xk1, xk2, xu1, xu2, 2},
		{"keyedSurvivorEOA", ek1, ek2, eu1, eu2, 2},
		{"absentDeleteBesideUpdate", ok1, ok2, ou1, ou2, 1},
		{"absentDeleteInFreshRegion", rk1, rk2, ru1, ru2, 1},
	}
}

func emptyPrefixAllDeleteRound(onDiskPlain [][]byte) ([][]byte, []Update) {
	hashed := make([][]byte, len(onDiskPlain))
	for i := range onDiskPlain {
		hashed[i] = KeyToHexNibbleHash(onDiskPlain[i])
	}
	_, addrs := findEmptyPrefixWithAddresses(hashed, 3, 20260910)
	ub := NewUpdateBuilder()
	for _, a := range addrs {
		ub.Delete(hex.EncodeToString(a))
	}
	return ub.Build()
}

func emptyRegionAllDeleteRound(seed int64) ([][]byte, []Update) {
	rnd := rand.New(rand.NewSource(seed))
	addr := make([]byte, length.Addr)
	rnd.Read(addr)
	a := hex.EncodeToString(addr)
	ub := NewUpdateBuilder()
	ub.Balance(a, 7331)
	for range 8 {
		loc := make([]byte, length.Hash)
		rnd.Read(loc)
		ub.DeleteStorage(a, hex.EncodeToString(loc))
	}
	return ub.Build()
}

func TestForkWalk_GrainExtremesMatchSequential(t *testing.T) {
	knobs := []struct {
		name  string
		grain uint32
	}{
		{"everySplitPoint", 1},
		{"serialControl", ForkGrainNever},
	}
	for _, c := range forkExtremeCorpora() {
		for _, kn := range knobs {
			t.Run(c.name+"/"+kn.name, func(t *testing.T) {
				seqRoot, seqMs := incrementalRoot(t, modeSeq, 0, c.k1, c.u1, c.k2, c.u2)

				parMs := NewMockState(t)
				parMs.SetConcurrentCommitment(true)
				_, blob, _ := parallelBatchForks(t, parMs, 4, kn.grain, c.k1, c.u1, nil)
				parRoot, _, forks := parallelBatchForks(t, parMs, 4, kn.grain, c.k2, c.u2, blob)

				if !bytes.Equal(seqRoot, parRoot) {
					branchDiff(t, seqMs, parMs)
				}
				require.Equal(t, seqRoot, parRoot, "grain %d root != sequential", kn.grain)
				requireBranchParity(t, seqMs, parMs)
				if kn.grain == ForkGrainNever {
					require.Zero(t, forks, "the serial control must not fork")
				} else {
					require.GreaterOrEqual(t, forks, c.minForks, "grain 1 must reach every split point of this corpus")
				}
			})
		}
	}
}

func nestedWaiterCorpus() ([][]byte, []Update) {
	rnd := rand.New(rand.NewSource(20260907))
	ub := NewUpdateBuilder()
	for _, spec := range []struct {
		prefix []byte
		slots  int
	}{
		{[]byte{0, 0}, 100},
		{[]byte{0, 1}, 25},
		{[]byte{1}, 25},
	} {
		a := hex.EncodeToString(findAddressForHexPrefix(spec.prefix, 20260907))
		ub.Balance(a, rnd.Uint64()+1)
		for range spec.slots {
			addRandomSlot(ub, rnd, a)
		}
	}
	return ub.Build()
}

func TestForkWalk_NestedWaitsDoNotSuppressDescendantForks(t *testing.T) {
	const grain = 26
	keys, upds := nestedWaiterCorpus()
	seqRoot, seqMs := sequentialRoot(t, keys, upds)

	var counts []uint64
	for _, workers := range []int{2, 16} {
		ms := NewMockState(t)
		ms.SetConcurrentCommitment(true)
		root, _, forks := parallelBatchForks(t, ms, workers, grain, keys, upds, nil)
		require.Equal(t, seqRoot, root, "workers=%d root != sequential", workers)
		requireBranchParity(t, seqMs, ms)
		t.Logf("workers=%d forks=%d", workers, forks)
		counts = append(counts, forks)
	}
	require.Equal(t, uint64(3), counts[0], "root, its 0-child and the whale account must all fork at 2 workers")
	require.Equal(t, counts[0], counts[1], "fork count must not depend on worker count")
}

type panicOnAccountContext struct {
	noopPatriciaContext
}

func (*panicOnAccountContext) Account([]byte) (*Update, error) { panic("injected account panic") }

func TestForkWalk_PanicInChildKeepsLeaseOnTheRunner(t *testing.T) {
	ctx := context.Background()
	pool := newCtxLeasePool(ctx, func(context.Context) (PatriciaContext, func()) {
		return &panicOnAccountContext{}, nil
	}, 1)
	defer pool.close()

	fw := &forkWalk{
		leases:        pool,
		accountKeyLen: length.Addr,
		cfg:           DefaultTrieConfig(),
		metrics:       NewMetrics(""),
		grain:         ForkGrainNever,
	}
	base := NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig())
	defer base.Release()

	child := &prefixNode{plainKey: make([]byte, length.Addr)}
	node := &prefixNode{bitmap: 1, children: []*prefixNode{child}}

	l, err := pool.acquire(ctx)
	require.NoError(t, err)
	held := &walker{lease: l, bindsCtx: true}

	var cells [16]cell
	var deferred [16][]*DeferredBranchUpdate
	func() {
		defer func() {
			require.Equal(t, "injected account panic", recover(), "the child must panic inside the walk")
		}()
		_ = fw.runChild(ctx, base, held, node, 0, 0, make([]byte, 63), &cells, &[16]bool{}, &[16]bool{}, &deferred)
	}()

	require.Nil(t, held.trie, "checkin must return the child trie even when the walk panics")
	require.Same(t, l, held.lease,
		"a panic must leave the lease on the runner's walker; its deferred release is the only thing that returns it")

	require.Empty(t, pool.free, "the lease is still held")
	pool.release(held.lease)
	require.Len(t, pool.free, 1, "exactly one lease returns to the pool, so a panic neither leaks nor double-releases")
}

func TestForkWalk_KeyedSurvivorCorpusShape(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind survivorKind
	}{
		{"bareRoot", survivorBareRoot},
		{"extRoot", survivorExtRoot},
		{"eoa", survivorEOA},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k1, u1, k2, u2 := keyedSurvivorCorpus(tc.kind)

			ms := NewMockState(t)
			trie := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
			defer trie.Release()
			processBatch(t, ms, trie, k1, u1)

			prefix := []byte{0x1, 0x2, 0x3}
			base := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
			defer base.Release()
			positioned, err := unfoldToRow(context.Background(), base, prefix)
			require.NoError(t, err)
			require.Truef(t, positioned, "corpus must put a branch row under %x", prefix)

			row := base.activeRows - 1
			surv := base.grid[row][0xf]
			require.Positivef(t, surv.accountAddrLen, "survivor at %x/f must be an account", prefix)
			switch tc.kind {
			case survivorEOA:
				require.Zero(t, surv.hashLen, "an EOA survivor carries no storage root hash")
			case survivorExtRoot:
				require.Positive(t, surv.hashLen, "survivor must carry a storage root hash")
				require.Positive(t, surv.extLen, "survivor's storage root must be an extension")
			case survivorBareRoot:
				require.Positive(t, surv.hashLen, "survivor must carry a storage root hash")
				require.Zero(t, surv.extLen, "survivor's storage root must be a bare branch hash")
			}
			for _, nib := range []byte{0x0, 0x4, 0x8} {
				require.NotZerof(t, base.afterMap[row]&(uint16(1)<<nib), "doomed sibling %x must be present before round 2", nib)
			}

			processBatch(t, ms, trie, k2, u2)
			rec := ms.cm[string(nibbles.HexToCompact(prefix))]
			require.Lenf(t, rec, 4, "round 2 must leave only a header at %x", prefix)
			require.Zerof(t, rec[2]|rec[3], "round 2 must empty the row at %x so its single survivor is promoted", prefix)
		})
	}
}

func absentDeleteCorpus(freshRegion bool) (k1 [][]byte, u1 []Update, k2 [][]byte, u2 []Update) {
	const seed = 20260914
	a0 := addrHex(findAddressForHexPrefix([]byte{0x0}, seed))
	a1 := addrHex(findAddressForHexPrefix([]byte{0x1}, seed))
	ub1 := NewUpdateBuilder()
	ub1.Balance(a0, 1)
	ub1.Balance(a1, 2)
	k1, u1 = ub1.Build()

	ub2 := NewUpdateBuilder()
	if freshRegion {
		ub2.Balance(addrHex(findAddressForHexPrefix([]byte{0x2, 0xa}, seed)), 3)
		ub2.Balance(addrHex(findAddressForHexPrefix([]byte{0x2, 0xb}, seed)), 4)
		ub2.Delete(addrHex(findAddressForHexPrefix([]byte{0x2, 0xc}, seed)))
	} else {
		ub2.Balance(a0, 3)
		ub2.Delete(addrHex(findAddressForHexPrefix([]byte{0x5}, seed)))
	}
	k2, u2 = ub2.Build()
	return k1, u1, k2, u2
}

func TestForkWalk_SplitsExcludesTheNodesOwnKey(t *testing.T) {
	pu := newParallelUpdate()
	pu.Insert(nibs(0x01, 0x02), []byte("pk-D"), nil)
	pu.Insert(nibs(0x01, 0x02, 0x03), []byte("pk-A"), nil)
	pu.Insert(nibs(0x01, 0x02, 0x04), []byte("pk-B"), nil)
	node := pu.trie.root.children[0]
	require.NotNil(t, node.plainKey)
	require.Equal(t, uint32(3), node.subtreeCount)
	require.True(t, (&forkWalk{grain: 1}).splits(node))
	require.False(t, (&forkWalk{grain: 2}).splits(node), "the non-largest child holds one key, below a grain of 2")
}

func TestForkWalk_AbsentDeletesBesideSingleSurvivor(t *testing.T) {
	a := addrHex(findAddressForHexPrefix([]byte{2, 2, 0}, 7))
	k, u := NewUpdateBuilder().Balance(a, 1).
		Storage(a, "0000000000000000000000000000000000000000000000000000000000000001", "01").
		Storage(a, "0000000000000000000000000000000000000000000000000000000000000002", "01").
		Delete(addrHex(findAddressForHexPrefix([]byte{2, 2, 2}, 8))).
		Delete(addrHex(findAddressForHexPrefix([]byte{2, 2, 3}, 9))).
		Delete(addrHex(findAddressForHexPrefix([]byte{2, 4}, 10))).
		Balance(addrHex(findAddressForHexPrefix([]byte{0, 0}, 11)), 2).
		Balance(addrHex(findAddressForHexPrefix([]byte{0, 1}, 12)), 3).
		Build()
	seqRoot, _ := sequentialRoot(t, k, u)
	parMs := NewMockState(t)
	parMs.SetConcurrentCommitment(true)
	parRoot, _, _ := parallelBatchForks(t, parMs, 1, 2, k, u, nil)
	require.Equal(t, seqRoot, parRoot)
}

func TestForkWalk_LeafDeleteUnderSplitRowKeepsBranchParity(t *testing.T) {
	x := addrHex(findAddressForHexPrefix([]byte{2, 2}, 1))
	k0, u0 := NewUpdateBuilder().Balance(x, 1).
		Balance(addrHex(findAddressForHexPrefix([]byte{0, 0}, 2)), 2).
		Balance(addrHex(findAddressForHexPrefix([]byte{0, 1}, 3)), 3).
		Build()
	k1, u1 := NewUpdateBuilder().Delete(x).
		Balance(addrHex(findAddressForHexPrefix([]byte{2, 4}, 4)), 4).
		Balance(addrHex(findAddressForHexPrefix([]byte{2, 0xa}, 5)), 5).
		Balance(addrHex(findAddressForHexPrefix([]byte{2, 0xc}, 6)), 6).
		Build()
	seqRoot, seqMs := incrementalRoot(t, modeSeq, 0, k0, u0, k1, u1)
	parMs := NewMockState(t)
	parMs.SetConcurrentCommitment(true)
	_, blob, _ := parallelBatchForks(t, parMs, 1, 1, k0, u0, nil)
	parRoot, _, _ := parallelBatchForks(t, parMs, 1, 1, k1, u1, blob)
	require.Equal(t, seqRoot, parRoot)
	requireBranchParity(t, seqMs, parMs)
}
