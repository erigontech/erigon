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

type splitRound struct {
	ms       *MockState
	splits   map[string]bool
	seams    map[string]cell
	captured map[string]cell
	deferred []*DeferredBranchUpdate
	workers  int
}

func newSplitRound(ms *MockState) *splitRound {
	return &splitRound{ms: ms, splits: map[string]bool{}, seams: map[string]cell{}, captured: map[string]cell{}}
}

func (r *splitRound) newTrie() *HexPatriciaHashed {
	w := NewHexPatriciaHashed(length.Addr, r.ms, DefaultTrieConfig())
	w.SetLeaveDeferredForCaller(true)
	return w
}

func (r *splitRound) take(w *HexPatriciaHashed) {
	r.deferred = append(r.deferred, w.TakeDeferredUpdates()...)
}

func (r *splitRound) flush(t *testing.T) {
	t.Helper()
	_, err := ApplyDeferredBranchUpdates(r.deferred, 1, r.ms.PutBranch, nil)
	require.NoError(t, err)
	r.deferred = nil
}

func (r *splitRound) foldLeaf(base *HexPatriciaHashed, nib int, group []splitKV) (cell, error) {
	w := r.newTrie()
	defer func() {
		r.take(w)
		w.Release()
	}()
	r.workers++
	w.mountTo(base, nib)
	for i := 0; i < len(group); i++ {
		k := group[i]
		if err := w.followAndUpdate(k.hk, k.pk, k.upd); err != nil {
			return cell{}, err
		}
		sr, ok := r.seams[string(k.hk)]
		if !ok {
			continue
		}
		setAccountStorageRoot(w, k.hk, sr)
		for i+1 < len(group) && bytes.HasPrefix(group[i+1].hk, k.hk) {
			i++
		}
	}
	return w.foldMounted(context.Background(), nib)
}

func (r *splitRound) foldChildren(base *HexPatriciaHashed, prefix []byte, keys []splitKV) ([16]cell, uint16, error) {
	var (
		cells  [16]cell
		bitmap uint16
	)
	d := len(prefix)
	for i := 0; i < len(keys); {
		nib := keys[i].hk[d]
		j := i
		for j < len(keys) && keys[j].hk[d] == nib {
			j++
		}
		childPrefix := make([]byte, 0, d+1)
		childPrefix = append(childPrefix, prefix...)
		childPrefix = append(childPrefix, nib)

		var (
			c   cell
			err error
		)
		if r.splits[string(childPrefix)] {
			c, err = r.foldAt(childPrefix, keys[i:j])
		} else {
			c, err = r.foldLeaf(base, int(nib), keys[i:j])
		}
		if err != nil {
			return cells, bitmap, err
		}
		cells[nib] = c
		bitmap |= uint16(1) << nib
		i = j
	}
	return cells, bitmap, nil
}

func (r *splitRound) foldAt(prefix []byte, keys []splitKV) (cell, error) {
	base := r.newTrie()
	defer func() {
		r.take(base)
		base.Release()
	}()
	if err := unfoldStorageBase(base, prefix); err != nil {
		return cell{}, err
	}
	cells, bitmap, err := r.foldChildren(base, prefix, keys)
	if err != nil {
		return cell{}, err
	}
	present := presentFlags(bitmap)
	stitchSplitCells(base, &cells, &present)
	out, err := aggregateMountedStorageRoot(base, &cells, bitmap)
	if err != nil {
		return cell{}, err
	}
	r.captured[string(prefix)] = out
	return out, nil
}

func (r *splitRound) foldRoot(keys []splitKV) ([]byte, error) {
	ctx := context.Background()
	base := r.newTrie()
	defer func() {
		r.take(base)
		base.Release()
	}()
	if err := unfoldRootWall(ctx, base); err != nil {
		return nil, err
	}
	seedRootBase(base)
	cells, bitmap, err := r.foldChildren(base, nil, keys)
	if err != nil {
		return nil, err
	}
	present := presentFlags(bitmap)
	stitchSplitCells(base, &cells, &present)
	if base.activeRows == 0 {
		base.activeRows = 1
	}
	for base.activeRows > 0 {
		if err := base.fold(); err != nil {
			return nil, err
		}
	}
	return base.RootHash()
}

func presentFlags(bitmap uint16) [16]bool {
	var present [16]bool
	for nib := range 16 {
		present[nib] = bitmap&(uint16(1)<<nib) != 0
	}
	return present
}

func splitWhaleCorpus(slots, background int) (addr []byte, pk [][]byte, upds []Update) {
	rnd := rand.New(rand.NewSource(20260903))
	addr = make([]byte, length.Addr)
	rnd.Read(addr)
	ub := NewUpdateBuilder()
	ub.Balance(hex.EncodeToString(addr), 12345)
	for range slots {
		addRandomSlot(ub, rnd, hex.EncodeToString(addr))
	}
	for range background {
		addRandomAccount(ub, rnd, 0)
	}
	pk, upds = ub.Build()
	return addr, pk, upds
}

func splitWhaleTouch(addr []byte, pk [][]byte, every int) ([][]byte, []Update) {
	a := hex.EncodeToString(addr)
	ub := NewUpdateBuilder()
	ub.Balance(a, 987654)
	n := 0
	for _, k := range pk {
		if len(k) != length.Addr+length.Hash || !bytes.HasPrefix(k, addr) {
			continue
		}
		if n%every == 0 {
			ub.Storage(a, hex.EncodeToString(k[length.Addr:]), "beef01")
		}
		n++
	}
	return ub.Build()
}

func TestSplitPoint_StoragePlaneInteriorSplit(t *testing.T) {
	addr, pk1, u1 := splitWhaleCorpus(400, 2_000)

	msSeq := NewMockState(t)
	seq := NewHexPatriciaHashed(length.Addr, msSeq, DefaultTrieConfig())
	defer seq.Release()
	processBatch(t, msSeq, seq, pk1, u1)

	msSplit := cloneMockState(t, msSeq)
	msCell := cloneMockState(t, msSeq)

	pk2, u2 := splitWhaleTouch(addr, pk1, 3)
	seqRoot := processBatch(t, msSeq, seq, pk2, u2)

	require.NoError(t, msSplit.applyPlainUpdates(pk2, u2))
	require.NoError(t, msCell.applyPlainUpdates(pk2, u2))

	accHash := KeyToHexNibbleHash(addr)
	accPrefix := accHash[:64]

	all := splitKVs(pk2, u2)
	var storage []splitKV
	for _, k := range all {
		if len(k.hk) == 128 && bytes.HasPrefix(k.hk, accPrefix) {
			storage = append(storage, k)
		}
	}
	require.NotEmpty(t, storage, "round 2 must touch whale storage")

	byNib := map[byte][]splitKV{}
	for _, k := range storage {
		byNib[k.hk[64]] = append(byNib[k.hk[64]], k)
	}
	splitNib := -1
	best := 0
	for nib := range 16 {
		g := byNib[byte(nib)]
		prefix65 := append(bytes.Clone(accPrefix), byte(nib))
		if len(g) > best && hasBranchAt(msSplit, prefix65) {
			splitNib, best = nib, len(g)
		}
	}
	require.GreaterOrEqual(t, splitNib, 0, "no touched storage nibble has an on-disk branch below the storage root")
	prefix65 := append(bytes.Clone(accPrefix), byte(splitNib))

	r := newSplitRound(msSplit)
	r.splits[string(prefix65)] = true
	sr, err := r.foldAt(accPrefix, storage)
	require.NoError(t, err)
	c65, ok := r.captured[string(prefix65)]
	require.True(t, ok, "the interior split at depth 65 did not run")

	r.seams[string(accHash)] = sr
	splitRoot, err := r.foldRoot(all)
	require.NoError(t, err)

	rc := newSplitRound(msCell)
	base := rc.newTrie()
	defer base.Release()
	require.NoError(t, unfoldStorageBase(base, accPrefix))
	c64, err := rc.foldLeaf(base, splitNib, byNib[byte(splitNib)])
	require.NoError(t, err)

	require.Equalf(t, c64.hash[:c64.hashLen], c65.hash[:c65.hashLen],
		"interior split at depth 65 (prefix %x) must yield the same cell hash as the depth-64 path for nibble %x", prefix65, splitNib)
	require.Equalf(t, c64.extension[:c64.extLen], c65.extension[:c65.extLen],
		"interior split at depth 65 (prefix %x) must yield the same cell extension as the depth-64 path", prefix65)
	require.Equal(t, seqRoot, splitRoot, "round root through a depth-65 split != sequential root")
}

func TestSplitPoint_AccountPlaneInteriorSplit(t *testing.T) {
	pk1, u1 := buildMixedCorpus(20260903, 8_000)

	msSeq := NewMockState(t)
	seq := NewHexPatriciaHashed(length.Addr, msSeq, DefaultTrieConfig())
	defer seq.Release()
	processBatch(t, msSeq, seq, pk1, u1)

	msSplit := cloneMockState(t, msSeq)

	pk2, u2 := buildMixedCorpus(771177, 1_500)
	seqRoot := processBatch(t, msSeq, seq, pk2, u2)
	require.NoError(t, msSplit.applyPlainUpdates(pk2, u2))

	keys := splitKVs(pk2, u2)
	var chosen []byte
	for i := 0; i < len(keys); {
		j := i
		for j < len(keys) && bytes.HasPrefix(keys[j].hk, keys[i].hk[:3]) {
			j++
		}
		p := bytes.Clone(keys[i].hk[:3])
		distinct := map[byte]struct{}{}
		for _, k := range keys[i:j] {
			distinct[k.hk[3]] = struct{}{}
		}
		if len(distinct) >= 2 && hasBranchAt(msSplit, p[:1]) && hasBranchAt(msSplit, p[:2]) && hasBranchAt(msSplit, p) {
			chosen = p
			break
		}
		i = j
	}
	require.NotNil(t, chosen, "no 3-nibble prefix with an on-disk branch and 2+ touched children")

	r := newSplitRound(msSplit)
	r.splits[string(chosen[:1])] = true
	r.splits[string(chosen[:2])] = true
	r.splits[string(chosen)] = true
	splitRoot, err := r.foldRoot(keys)
	require.NoError(t, err)
	r.flush(t)

	require.Equalf(t, seqRoot, splitRoot, "round root split at the root and again at %x != sequential root", chosen)
	requireBranchParity(t, msSeq, msSplit)
}

func splitCollapseCorpus() (survivors, doomed [][]byte, pk [][]byte, upds []Update) {
	rnd := rand.New(rand.NewSource(9090))
	ub := NewUpdateBuilder()
	for _, tail := range []byte{0x0, 0x5, 0xa} {
		a := findAddressForHexPrefix([]byte{0x1, 0x2, 0x3, tail}, int(tail)+11)
		doomed = append(doomed, a)
		ub.Balance(hex.EncodeToString(a), uint64(tail)+7)
	}
	for seed := range 6 {
		a := findAddressForHexPrefix([]byte{0x1, 0x2, 0x3, 0xf}, seed+77)
		survivors = append(survivors, a)
		ub.Balance(hex.EncodeToString(a), uint64(seed)+31)
	}
	for range 400 {
		addRandomAccount(ub, rnd, 0)
	}
	pk, upds = ub.Build()
	return survivors, doomed, pk, upds
}

func TestSplitPoint_InteriorSingleSurvivorCollapse(t *testing.T) {
	_, doomed, pk1, u1 := splitCollapseCorpus()

	ms := NewMockState(t)
	seq := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer seq.Release()
	processBatch(t, ms, seq, pk1, u1)

	prefix := []byte{0x1, 0x2, 0x3}
	require.True(t, hasBranchAt(ms, prefix), "corpus must produce an on-disk branch at 123")

	ub := NewUpdateBuilder()
	for _, a := range doomed {
		ub.Delete(hex.EncodeToString(a))
	}
	pk2, u2 := ub.Build()
	require.NoError(t, ms.applyPlainUpdates(pk2, u2))

	before := snapshotBranches(ms)
	r := newSplitRound(ms)
	base := r.newTrie()
	defer base.Release()
	require.NoError(t, unfoldStorageBase(base, prefix))

	survNib := byte(0xf)
	child := base.grid[0][survNib]
	require.Positive(t, child.hashLen, "the survivor child must be a branch reference")

	cells, bitmap, err := r.foldChildren(base, prefix, splitKVs(pk2, u2))
	require.NoError(t, err)
	require.Zero(t, bitmap&(uint16(1)<<survNib), "the survivor must not be touched")

	present := presentFlags(bitmap)
	stitchSplitCells(base, &cells, &present)
	got, err := aggregateMountedStorageRoot(base, &cells, bitmap)
	require.NoError(t, err)

	want := append([]byte{survNib}, child.extension[:child.extLen]...)
	require.Equal(t, want, got.extension[:got.extLen],
		"a single-survivor collapse must return an extension cell of survivor nibble + child extension")
	require.Equal(t, child.hash[:child.hashLen], got.hash[:got.hashLen], "the collapsed cell must carry the survivor's hash")

	r.take(base)
	require.Empty(t, r.deferred, "a single-survivor collapse must rewrite no branch")

	var written []string
	for k, v := range ms.cm {
		if !bytes.Equal(before[k], v) {
			written = append(written, k)
		}
	}
	require.Lenf(t, written, 1, "a single-survivor collapse must emit exactly one branch record, got %x", written)
	require.Equalf(t, string(nibbles.HexToCompact(prefix)), written[0],
		"the one branch record must sit at the collapsed prefix %x", prefix)
	_, afterMap, _, _ := BranchData(ms.cm[written[0]]).decodeCells()
	require.Zero(t, afterMap, "the record at the collapsed prefix must be a delete (afterMap 0)")
}

func TestSplitPoint_RefusesPrefixWithoutBranch(t *testing.T) {
	pk1, u1 := buildMixedCorpus(31337, 2_000)

	ms := NewMockState(t)
	seq := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer seq.Release()
	processBatch(t, ms, seq, pk1, u1)

	var absent []byte
	for n0 := range 16 {
		for n1 := range 16 {
			p := []byte{byte(n0), byte(n1), 0xd, 0xe, 0xa, 0xd}
			if !hasBranchAt(ms, p) {
				absent = p
				break
			}
		}
		if absent != nil {
			break
		}
	}
	require.NotNil(t, absent, "need a prefix with no stored branch")

	r := newSplitRound(ms)
	keys := splitKVs(pk1, u1)
	var under []splitKV
	for _, k := range keys {
		if bytes.HasPrefix(k.hk, absent) {
			under = append(under, k)
		}
	}
	_, err := r.foldAt(absent, under)
	require.ErrorIs(t, err, errStorageBaseNotBranch)
	require.Zero(t, r.workers, "positioning must refuse before any worker is created")
}
