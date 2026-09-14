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
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

type splitKV struct {
	hk  []byte
	pk  []byte
	upd *Update
}

func splitKVs(pk [][]byte, upds []Update) []splitKV {
	out := make([]splitKV, len(pk))
	for i := range pk {
		out[i] = splitKV{hk: KeyToHexNibbleHash(pk[i]), pk: pk[i], upd: &upds[i]}
	}
	slices.SortFunc(out, func(a, b splitKV) int { return bytes.Compare(a.hk, b.hk) })
	return out
}

func cloneMockState(t *testing.T, src *MockState) *MockState {
	t.Helper()
	dst := NewMockState(t)
	for k, v := range src.sm {
		dst.sm[k] = bytes.Clone(v)
	}
	for k, v := range src.cm {
		dst.cm[k] = bytes.Clone(v)
	}
	return dst
}

func hasBranchAt(ms *MockState, prefix []byte) bool {
	_, ok := ms.cm[string(nibbles.HexToCompact(prefix))]
	return ok
}

type splitPosition uint8

const (
	positionOff splitPosition = iota
	positionOpen
	positionKeep
)

func splitFoldFrom(t *testing.T, ms *MockState, prefix []byte, pre, under []splitKV, mode splitPosition) ([]byte, bool) {
	t.Helper()
	ctx := context.Background()
	w := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer w.Release()
	w.SetLeaveDeferredForCaller(true)

	for _, k := range pre {
		require.NoError(t, w.followAndUpdate(k.hk, k.pk, k.upd))
	}
	positioned := false
	if mode != positionOff {
		var err error
		positioned, err = unfoldToRow(ctx, w, prefix)
		require.NoError(t, err)
		if !positioned && mode == positionOpen {
			openEmptyRow(w, prefix)
		}
		if positioned || mode == positionOpen {
			require.EqualValues(t, len(prefix)+1, w.depths[w.activeRows-1],
				"deepest row must sit at depth len(prefix)+1 for prefix %x", prefix)
			require.Equal(t, prefix, append([]byte(nil), w.currentKey[:w.currentKeyLen]...),
				"currentKey must be the fork prefix")
		}
	}
	for _, k := range under {
		require.NoError(t, w.followAndUpdate(k.hk, k.pk, k.upd))
	}
	_, err := foldSplitRow(ctx, w)
	require.NoError(t, err)
	root, err := w.RootHash()
	require.NoError(t, err)
	_, err = ApplyDeferredBranchUpdates(w.TakeDeferredUpdates(), 1, ms.PutBranch, nil)
	require.NoError(t, err)
	return bytes.Clone(root), positioned
}

func splitAroundPrefix(t *testing.T, all []splitKV, prefix []byte) (pre, under []splitKV) {
	t.Helper()
	for _, k := range all {
		if len(k.hk) > len(prefix) && bytes.HasPrefix(k.hk, prefix) {
			under = append(under, k)
		} else {
			pre = append(pre, k)
		}
	}
	require.NotEmpty(t, under, "no touched key under prefix %x", prefix)
	if len(pre) > 0 {
		require.Negative(t, bytes.Compare(pre[len(pre)-1].hk, under[0].hk),
			"keys outside prefix %x must all sort before the ones under it", prefix)
	}
	return pre, under
}

func requireUnfoldToRowTransparent(t *testing.T, base *MockState, prefix []byte, pk [][]byte, upds []Update) bool {
	t.Helper()
	all := splitKVs(pk, upds)
	pre, under := splitAroundPrefix(t, all, prefix)

	msSeq := cloneMockState(t, base)
	msPos := cloneMockState(t, base)
	require.NoError(t, msSeq.applyPlainUpdates(pk, upds))
	require.NoError(t, msPos.applyPlainUpdates(pk, upds))

	seqRoot, _ := splitFoldFrom(t, msSeq, nil, pre, under, positionOff)
	posRoot, positioned := splitFoldFrom(t, msPos, prefix, pre, under, positionOpen)
	require.Equalf(t, seqRoot, posRoot, "root through a walker positioned at %x != unpositioned walker", prefix)
	requireBranchParity(t, msSeq, msPos)
	return positioned
}

func unfoldRowKeysUnder(keys []splitKV, prefix []byte) []splitKV {
	var out []splitKV
	for _, k := range keys {
		if len(k.hk) > len(prefix) && bytes.HasPrefix(k.hk, prefix) {
			out = append(out, k)
		}
	}
	return out
}

func unfoldRowDistinctChildren(keys []splitKV, prefix []byte) int {
	seen := map[byte]struct{}{}
	for _, k := range unfoldRowKeysUnder(keys, prefix) {
		seen[k.hk[len(prefix)]] = struct{}{}
	}
	return len(seen)
}

type unfoldRowFixture struct {
	ms      *MockState
	round1  [][]byte
	hashed1 [][]byte
	whale   []byte
	single  []byte
	bare    []byte
	extAcc  []byte
}

func randomSlot(rnd *rand.Rand) []byte {
	loc := make([]byte, length.Hash)
	rnd.Read(loc)
	return loc
}

func newUnfoldRowFixture(t *testing.T) *unfoldRowFixture {
	t.Helper()
	rnd := rand.New(rand.NewSource(20260904))
	ub := NewUpdateBuilder()
	mk := func() []byte {
		a := make([]byte, length.Addr)
		rnd.Read(a)
		return a
	}

	whale := mk()
	ub.Balance(hex.EncodeToString(whale), 11)
	for range 400 {
		addRandomSlot(ub, rnd, hex.EncodeToString(whale))
	}
	single := mk()
	ub.Balance(hex.EncodeToString(single), 12)
	addRandomSlot(ub, rnd, hex.EncodeToString(single))
	bare := mk()
	ub.Balance(hex.EncodeToString(bare), 13)

	pairs := make([][]byte, 64)
	for i := range pairs {
		a := mk()
		pairs[i] = a
		ub.Balance(hex.EncodeToString(a), uint64(20+i))
		addRandomSlot(ub, rnd, hex.EncodeToString(a))
		addRandomSlot(ub, rnd, hex.EncodeToString(a))
	}
	for range 3_000 {
		addRandomAccount(ub, rnd, 0)
	}
	pk1, u1 := ub.Build()

	ms := NewMockState(t)
	seq := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer seq.Release()
	processBatch(t, ms, seq, pk1, u1)

	var extAcc []byte
	for _, a := range pairs {
		if !hasBranchAt(ms, KeyToHexNibbleHash(a)[:64]) {
			extAcc = a
			break
		}
	}
	require.NotNil(t, extAcc, "no 2-slot account whose storage root sits behind an extension")

	hashed := make([][]byte, len(pk1))
	for i := range pk1 {
		hashed[i] = KeyToHexNibbleHash(pk1[i])
	}
	slices.SortFunc(hashed, bytes.Compare)
	return &unfoldRowFixture{ms: ms, round1: pk1, hashed1: hashed, whale: whale, single: single, bare: bare, extAcc: extAcc}
}

func (f *unfoldRowFixture) onDiskUnder(prefix []byte) int {
	n := 0
	for _, hk := range f.hashed1 {
		if bytes.HasPrefix(hk, prefix) {
			n++
		}
	}
	return n
}

func (f *unfoldRowFixture) findPrefix(keys []splitKV, plen int, accept func(prefix []byte, onDisk int) bool) []byte {
	for i := 0; i < len(keys); {
		p := keys[i].hk[:plen]
		j := i
		for j < len(keys) && bytes.HasPrefix(keys[j].hk, p) {
			j++
		}
		if unfoldRowDistinctChildren(keys[i:j], p) >= 2 && accept(p, f.onDiskUnder(p)) {
			return bytes.Clone(p)
		}
		i = j
	}
	return nil
}

func (f *unfoldRowFixture) seamRound(t *testing.T, addr []byte, newSlots int) ([][]byte, []Update) {
	t.Helper()
	rnd := rand.New(rand.NewSource(int64(addr[0])*7919 + 4242))
	a := hex.EncodeToString(addr)
	ub := NewUpdateBuilder()
	ub.Balance(a, 999_000)
	for range newSlots {
		ub.Storage(a, hex.EncodeToString(randomSlot(rnd)), "c0ffee")
	}
	pk, upds := ub.Build()
	require.GreaterOrEqualf(t, unfoldRowDistinctChildren(splitKVs(pk, upds), KeyToHexNibbleHash(addr)[:64]), 2,
		"seam round for %x must touch two distinct storage nibbles", addr)
	return pk, upds
}

func unfoldRowSubset(pk [][]byte, upds []Update, prefix []byte) ([][]byte, []Update) {
	var (
		outK [][]byte
		outU []Update
	)
	for i := range pk {
		hk := KeyToHexNibbleHash(pk[i])
		if bytes.HasPrefix(hk, prefix) || bytes.HasPrefix(prefix, hk) {
			outK = append(outK, pk[i])
			outU = append(outU, upds[i])
		}
	}
	return outK, outU
}

type unfoldRowCase struct {
	name           string
	prefix         []byte
	pk             [][]byte
	upds           []Update
	wantPositioned bool
}

func (f *unfoldRowFixture) cases(t *testing.T) []unfoldRowCase {
	t.Helper()
	rnd := rand.New(rand.NewSource(555_111))
	ub := NewUpdateBuilder()
	for range 1_200 {
		addRandomAccount(ub, rnd, 0)
	}
	for i := 0; i < len(f.round1); i += 4 {
		if len(f.round1[i]) == length.Addr {
			ub.Balance(hex.EncodeToString(f.round1[i]), rnd.Uint64()+1)
		}
	}
	pkAcc, uAcc := ub.Build()
	acc := splitKVs(pkAcc, uAcc)

	branchPrefix := f.findPrefix(acc, 3, func(p []byte, _ int) bool { return hasBranchAt(f.ms, p) })
	require.NotNil(t, branchPrefix, "no 3-nibble prefix with a stored branch and two touched children")
	leafPrefix := f.findPrefix(acc, 3, func(p []byte, onDisk int) bool { return onDisk == 1 && !hasBranchAt(f.ms, p) })
	require.NotNil(t, leafPrefix, "no 3-nibble prefix holding exactly one on-disk key")
	extPrefix := f.findPrefix(acc, 3, func(p []byte, onDisk int) bool { return onDisk >= 2 && !hasBranchAt(f.ms, p) })
	require.NotNil(t, extPrefix, "no 3-nibble prefix whose on-disk branch sits below it")
	emptyPrefix := f.findPrefix(acc, 3, func(p []byte, onDisk int) bool { return onDisk == 0 })
	require.NotNil(t, emptyPrefix, "no 3-nibble prefix empty on disk with two touched children")

	whalePk, whaleU := f.seamRound(t, f.whale, 6)
	singlePk, singleU := f.seamRound(t, f.single, 4)
	extPk, extU := f.seamRound(t, f.extAcc, 4)

	cases := []unfoldRowCase{
		{"account/branch", branchPrefix, nil, nil, true},
		{"account/leaf", leafPrefix, nil, nil, true},
		{"account/extension", extPrefix, nil, nil, true},
		{"account/empty", emptyPrefix, nil, nil, false},
		{"seam/branchRoot", KeyToHexNibbleHash(f.whale)[:64], whalePk, whaleU, true},
		{"seam/singleSlot", KeyToHexNibbleHash(f.single)[:64], singlePk, singleU, true},
		{"seam/extensionRoot", KeyToHexNibbleHash(f.extAcc)[:64], extPk, extU, true},
	}
	for i := range cases {
		if cases[i].pk == nil {
			cases[i].pk, cases[i].upds = unfoldRowSubset(pkAcc, uAcc, cases[i].prefix)
		}
	}
	return cases
}

func TestUnfoldToRow_PositionsAtSplitPointRow(t *testing.T) {
	f := newUnfoldRowFixture(t)
	for _, tc := range f.cases(t) {
		t.Run(tc.name, func(t *testing.T) {
			positioned := requireUnfoldToRowTransparent(t, f.ms, tc.prefix, tc.pk, tc.upds)
			require.Equalf(t, tc.wantPositioned, positioned, "prefix %x", tc.prefix)
		})
	}
}

func splitForkAt(t *testing.T, ms *MockState, prefix []byte, pre, under []splitKV) []byte {
	t.Helper()
	ctx := context.Background()
	base := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer base.Release()
	base.SetLeaveDeferredForCaller(true)

	for _, k := range pre {
		require.NoError(t, base.followAndUpdate(k.hk, k.pk, k.upd))
	}
	positioned, err := unfoldToRow(ctx, base, prefix)
	require.NoError(t, err)
	if !positioned {
		openEmptyRow(base, prefix)
	}
	require.Greaterf(t, base.activeRows, 1, "fork at %x must sit below at least one other row", prefix)

	var (
		cells    [16]cell
		present  uint16
		deferred []*DeferredBranchUpdate
	)
	d := len(prefix)
	for i := 0; i < len(under); {
		nib := under[i].hk[d]
		j := i
		for j < len(under) && under[j].hk[d] == nib {
			j++
		}
		w := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
		w.SetLeaveDeferredForCaller(true)
		w.mountTo(base, int(nib))
		for _, k := range under[i:j] {
			require.NoError(t, w.followAndUpdate(k.hk, k.pk, k.upd))
		}
		c, err := w.foldMounted(ctx, int(nib))
		require.NoError(t, err)
		cells[nib] = c
		present |= uint16(1) << nib
		deferred = append(deferred, w.TakeDeferredUpdates()...)
		w.Release()
		i = j
	}
	stitchSplitCells(base, &cells, present)
	if _, err := foldSplitRow(ctx, base); err != nil {
		require.NoError(t, err)
	}
	root, err := base.RootHash()
	require.NoError(t, err)
	deferred = append(deferred, base.TakeDeferredUpdates()...)
	_, err = ApplyDeferredBranchUpdates(deferred, 1, ms.PutBranch, nil)
	require.NoError(t, err)
	return bytes.Clone(root)
}

func TestSplitPoint_ForkOnWalkerRows(t *testing.T) {
	f := newUnfoldRowFixture(t)
	for _, tc := range f.cases(t) {
		t.Run(tc.name, func(t *testing.T) {
			all := splitKVs(tc.pk, tc.upds)
			pre, under := splitAroundPrefix(t, all, tc.prefix)

			msSeq := cloneMockState(t, f.ms)
			msFork := cloneMockState(t, f.ms)
			require.NoError(t, msSeq.applyPlainUpdates(tc.pk, tc.upds))
			require.NoError(t, msFork.applyPlainUpdates(tc.pk, tc.upds))

			seqRoot, _ := splitFoldFrom(t, msSeq, nil, pre, under, positionOff)
			forkRoot := splitForkAt(t, msFork, tc.prefix, pre, under)
			require.Equalf(t, seqRoot, forkRoot, "fork at %x != sequential root", tc.prefix)
			requireBranchParity(t, msSeq, msFork)
		})
	}
}

func TestUnfoldToRow_SeamWithoutStorageStaysAboveTheRow(t *testing.T) {
	f := newUnfoldRowFixture(t)
	w := NewHexPatriciaHashed(length.Addr, f.ms, DefaultTrieConfig())
	defer w.Release()

	prefix := KeyToHexNibbleHash(f.bare)[:64]
	positioned, err := unfoldToRow(context.Background(), w, prefix)
	require.NoError(t, err)
	require.False(t, positioned, "an account without storage has no row at depth 65")
	require.EqualValues(t, 64, w.depths[w.activeRows-1], "positioning must stop on the account row")
}

func findEmptyPrefixWithAddresses(onDisk [][]byte, plen int, seed int64) ([]byte, [][]byte) {
	occupied := map[string]struct{}{}
	for _, hk := range onDisk {
		if len(hk) >= plen {
			occupied[string(hk[:plen])] = struct{}{}
		}
	}
	rnd := rand.New(rand.NewSource(seed))
	groups := map[string][][]byte{}
	for range 6_000 {
		a := make([]byte, length.Addr)
		rnd.Read(a)
		hk := KeyToHexNibbleHash(a)
		groups[string(hk[:plen])] = append(groups[string(hk[:plen])], a)
	}
	keys := make([]string, 0, len(groups))
	for p := range groups {
		keys = append(keys, p)
	}
	slices.Sort(keys)
	for _, p := range keys {
		g := groups[p]
		if len(g) < 2 {
			continue
		}
		if _, taken := occupied[p]; taken {
			continue
		}
		nibs := map[byte]struct{}{}
		for _, a := range g {
			nibs[KeyToHexNibbleHash(a)[plen]] = struct{}{}
		}
		if len(nibs) >= 2 {
			return []byte(p), g
		}
	}
	return nil, nil
}

func TestUnfoldToRow_AllDeleteSubtreeMustNotOpenTheRow(t *testing.T) {
	f := newUnfoldRowFixture(t)

	prefix, addrs := findEmptyPrefixWithAddresses(f.hashed1, 3, 6_060_842)
	require.NotNil(t, prefix, "no empty 3-nibble prefix with two candidate addresses")

	ub := NewUpdateBuilder()
	for _, a := range addrs {
		ub.Delete(hex.EncodeToString(a))
	}
	pk, upds := ub.Build()
	all := splitKVs(pk, upds)
	require.GreaterOrEqual(t, unfoldRowDistinctChildren(all, prefix), 2, "the delete round must span two nibbles")
	pre, under := splitAroundPrefix(t, all, prefix)
	require.Empty(t, pre)

	msSeq := cloneMockState(t, f.ms)
	msKeep := cloneMockState(t, f.ms)
	msOpen := cloneMockState(t, f.ms)
	for _, ms := range []*MockState{msSeq, msKeep, msOpen} {
		require.NoError(t, ms.applyPlainUpdates(pk, upds))
	}

	seqRoot, _ := splitFoldFrom(t, msSeq, nil, pre, under, positionOff)
	keepRoot, positioned := splitFoldFrom(t, msKeep, prefix, pre, under, positionKeep)
	require.False(t, positioned, "an empty region has no row to position on")
	require.Equal(t, seqRoot, keepRoot, "leaving the row closed must match the serial walker")
	requireBranchParity(t, msSeq, msKeep)

	openRoot, _ := splitFoldFrom(t, msOpen, prefix, pre, under, positionOpen)
	require.NotEqual(t, seqRoot, openRoot,
		"opening a row for an all-delete subtree marks an empty cell present; that is what the guard prevents")
}

func freshStorageWalker(t *testing.T, upds []splitKV) *HexPatriciaHashed {
	t.Helper()
	w := NewHexPatriciaHashed(length.Addr, NewMockState(t), DefaultTrieConfig())
	t.Cleanup(w.Release)
	for _, k := range upds {
		require.NoError(t, w.followAndUpdate(k.hk, k.pk, k.upd))
	}
	return w
}

func TestUnfoldToRow_CompressedPathCostsOneRow(t *testing.T) {
	rnd := rand.New(rand.NewSource(20260907))
	addr := make([]byte, length.Addr)
	rnd.Read(addr)
	ub := NewUpdateBuilder()
	ub.Balance(hex.EncodeToString(addr), 7)
	addRandomSlot(ub, rnd, hex.EncodeToString(addr))
	addRandomSlot(ub, rnd, hex.EncodeToString(addr))
	pk, upds := ub.Build()

	prefix := KeyToHexNibbleHash(addr)[:64]
	pre, under := splitAroundPrefix(t, splitKVs(pk, upds), prefix)
	require.Len(t, pre, 1, "only the account update sits outside its own storage prefix")

	w := freshStorageWalker(t, pre)
	positioned, err := unfoldToRow(context.Background(), w, prefix)
	require.NoError(t, err)
	require.False(t, positioned, "a fresh account has no storage subtree to position on")
	require.EqualValues(t, 1, w.activeRows,
		"the account's compressed 64-nibble path must cost one row, not one per nibble")
	require.EqualValues(t, 64, w.depths[0], "that row is the account boundary")

	openEmptyRow(w, prefix)
	require.EqualValues(t, 2, w.activeRows, "boundary row plus the storage split row")
	for _, k := range under {
		require.NoError(t, w.followAndUpdate(k.hk, k.pk, k.upd))
	}
	require.EqualValues(t, 65, w.depths[w.activeRows-1], "storage stays under the positioned row")
}

func TestUnfoldToRow_ProbeMatchingALongerPathStopsAtTheTarget(t *testing.T) {
	const plen = 17
	rnd := rand.New(rand.NewSource(20260907))
	var addr []byte
	for range 1_000 {
		c := make([]byte, length.Addr)
		rnd.Read(c)
		if KeyToHexNibbleHash(c)[plen] == 0 {
			addr = c
			break
		}
	}
	require.NotNil(t, addr, "no address whose hashed nibble %d is zero, so the probe cannot fully match", plen)

	ub := NewUpdateBuilder()
	ub.Balance(hex.EncodeToString(addr), 7)
	pk, upds := ub.Build()

	prefix := KeyToHexNibbleHash(addr)[:plen]
	w := freshStorageWalker(t, splitKVs(pk, upds))
	positioned, err := unfoldToRow(context.Background(), w, prefix)
	require.NoError(t, err)
	require.True(t, positioned, "a fully matching probe must still land on the requested row")
	require.EqualValues(t, plen+1, w.depths[w.activeRows-1], "descent must not overshoot the target depth")
}
