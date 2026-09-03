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

	"github.com/erigontech/erigon/common"
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
	replay   map[string][]splitKV
	captured map[string]cell
	deferred []*DeferredBranchUpdate
	workers  int
}

func newSplitRound(ms *MockState) *splitRound {
	return &splitRound{
		ms:       ms,
		splits:   map[string]bool{},
		seams:    map[string]cell{},
		replay:   map[string][]splitKV{},
		captured: map[string]cell{},
	}
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
	if err := unfoldSplitBase(base, prefix); err != nil {
		return cell{}, err
	}
	later := r.replay[string(prefix)]
	cells, bitmap, err := r.foldChildren(base, prefix, withoutKeys(keys, later))
	if err != nil {
		return cell{}, err
	}
	stitchSplitCells(base, &cells, bitmap)
	if len(later) > 0 {
		r.take(base)
		if _, err := ApplyDeferredBranchUpdates(r.deferred, 1, r.ms.PutBranch, nil); err != nil {
			return cell{}, err
		}
		r.deferred = nil
	}
	for i := range later {
		if err := base.followAndUpdate(later[i].hk, later[i].pk, later[i].upd); err != nil {
			return cell{}, err
		}
	}
	for base.activeRows > 1 {
		if err := base.fold(); err != nil {
			return cell{}, err
		}
	}
	out, err := foldSplitRow(context.Background(), base, foldToCell)
	if err != nil {
		return cell{}, err
	}
	r.captured[string(prefix)] = out
	return out, nil
}

func withoutKeys(keys, drop []splitKV) []splitKV {
	if len(drop) == 0 {
		return keys
	}
	skip := make(map[string]struct{}, len(drop))
	for _, k := range drop {
		skip[string(k.hk)] = struct{}{}
	}
	out := make([]splitKV, 0, len(keys))
	for _, k := range keys {
		if _, ok := skip[string(k.hk)]; ok {
			continue
		}
		out = append(out, k)
	}
	return out
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
	stitchSplitCells(base, &cells, bitmap)
	if _, err := foldSplitRow(ctx, base, foldToRoot); err != nil {
		return nil, err
	}
	return base.RootHash()
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
	r.flush(t)

	rc := newSplitRound(msCell)
	base := rc.newTrie()
	defer base.Release()
	require.NoError(t, unfoldSplitBase(base, accPrefix))
	c64, err := rc.foldLeaf(base, splitNib, byNib[byte(splitNib)])
	require.NoError(t, err)

	require.Equalf(t, c64.hash[:c64.hashLen], c65.hash[:c65.hashLen],
		"interior split at depth 65 (prefix %x) must yield the same cell hash as the depth-64 path for nibble %x", prefix65, splitNib)
	require.Equalf(t, c64.extension[:c64.extLen], c65.extension[:c65.extLen],
		"interior split at depth 65 (prefix %x) must yield the same cell extension as the depth-64 path", prefix65)
	require.Equal(t, seqRoot, splitRoot, "round root through a depth-65 split != sequential root")
	requireBranchParity(t, msSeq, msSplit)
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

func splitCollapseCorpus() (doomed [][]byte, pk [][]byte, upds []Update) {
	rnd := rand.New(rand.NewSource(9090))
	ub := NewUpdateBuilder()
	for _, tail := range []byte{0x0, 0x5, 0xa} {
		a := findAddressForHexPrefix([]byte{0x1, 0x2, 0x3, tail}, int(tail)+11)
		doomed = append(doomed, a)
		ub.Balance(hex.EncodeToString(a), uint64(tail)+7)
	}
	for seed := range 6 {
		a := findAddressForHexPrefix([]byte{0x1, 0x2, 0x3, 0xf}, seed+77)
		ub.Balance(hex.EncodeToString(a), uint64(seed)+31)
	}
	for range 400 {
		addRandomAccount(ub, rnd, 0)
	}
	pk, upds = ub.Build()
	return doomed, pk, upds
}

func TestSplitPoint_InteriorSingleSurvivorCollapse(t *testing.T) {
	doomed, pk1, u1 := splitCollapseCorpus()

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
	require.NoError(t, unfoldSplitBase(base, prefix))

	survNib := byte(0xf)
	child := base.grid[0][survNib]
	require.Positive(t, child.hashLen, "the survivor child must be a branch reference")
	require.Zero(t, child.accountAddrLen+child.storageAddrLen, "this case pins the keyless survivor; the keyed one has its own test")

	cells, bitmap, err := r.foldChildren(base, prefix, splitKVs(pk2, u2))
	require.NoError(t, err)
	require.Zero(t, bitmap&(uint16(1)<<survNib), "the survivor must not be touched")

	stitchSplitCells(base, &cells, bitmap)
	got, err := foldSplitRow(context.Background(), base, foldToCell)
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
	require.ErrorIs(t, err, errSplitNotBranch)
	require.Zero(t, r.workers, "positioning must refuse before any worker is created")
}

func splitCollapseParentCorpus() (doomed [][]byte, keep [][]byte, pk [][]byte, upds []Update) {
	rnd := rand.New(rand.NewSource(4242))
	ub := NewUpdateBuilder()
	for _, tail := range []byte{0x0, 0x5, 0xa} {
		a := findAddressForHexPrefix([]byte{0x1, 0x2, 0x3, tail}, int(tail)+11)
		doomed = append(doomed, a)
		ub.Balance(hex.EncodeToString(a), uint64(tail)+7)
	}
	for seed := range 6 {
		a := findAddressForHexPrefix([]byte{0x1, 0x2, 0x3, 0xf}, seed+77)
		ub.Balance(hex.EncodeToString(a), uint64(seed)+31)
	}
	for _, p := range [][]byte{{0x1, 0x2, 0x7}, {0x1, 0x2, 0xc}, {0x1, 0x7}, {0x1, 0xc}} {
		for seed := range 2 {
			a := findAddressForHexPrefix(p, seed*13+101)
			ub.Balance(hex.EncodeToString(a), uint64(seed)+53)
			if seed == 0 && (p[len(p)-1] == 0x7) {
				keep = append(keep, a)
			}
		}
	}
	for range 400 {
		addRandomAccount(ub, rnd, 0)
	}
	pk, upds = ub.Build()
	return doomed, keep, pk, upds
}

func TestSplitPoint_CollapsedCellFoldsInParentSplit(t *testing.T) {
	doomed, keep, pk1, u1 := splitCollapseParentCorpus()
	require.Len(t, keep, 2, "corpus must keep one touchable account under 127 and one under 17")

	msSeq := NewMockState(t)
	seq := NewHexPatriciaHashed(length.Addr, msSeq, DefaultTrieConfig())
	defer seq.Release()
	processBatch(t, msSeq, seq, pk1, u1)

	prefix := []byte{0x1, 0x2, 0x3}
	for _, p := range [][]byte{prefix[:1], prefix[:2], prefix} {
		require.Truef(t, hasBranchAt(msSeq, p), "corpus must produce an on-disk branch at %x", p)
	}

	msSplit := cloneMockState(t, msSeq)

	ub := NewUpdateBuilder()
	for _, a := range doomed {
		ub.Delete(hex.EncodeToString(a))
	}
	for i, a := range keep {
		ub.Balance(hex.EncodeToString(a), uint64(i)+9001)
	}
	pk2, u2 := ub.Build()
	seqRoot := processBatch(t, msSeq, seq, pk2, u2)
	require.NoError(t, msSplit.applyPlainUpdates(pk2, u2))

	r := newSplitRound(msSplit)
	r.splits[string(prefix[:1])] = true
	r.splits[string(prefix[:2])] = true
	r.splits[string(prefix)] = true
	splitRoot, err := r.foldRoot(splitKVs(pk2, u2))
	require.NoError(t, err)
	r.flush(t)

	collapsed, ok := r.captured[string(prefix)]
	require.True(t, ok, "the interior split at 123 did not run")
	require.Positive(t, collapsed.extLen, "the split at 123 must collapse to a single survivor")
	require.EqualValues(t, 0xf, collapsed.extension[0], "the survivor nibble must lead the extension")
	require.Equalf(t, seqRoot, splitRoot, "root with a collapse at %x folded through parent splits at %x and %x != sequential", prefix, prefix[:2], prefix[:1])
	requireBranchParity(t, msSeq, msSplit)

	require.Equalf(t, collapsed.extension[:collapsed.extLen], collapsed.hashedExtension[:collapsed.hashedExtLen],
		"a collapsed account-plane cell must carry a hashed extension matching its extension; extLen=%d hashedExtLen=%d",
		collapsed.extLen, collapsed.hashedExtLen)
}

func splitStorageCollapseCorpus() (addr []byte, doomed []string, survivors []string, pk [][]byte, upds []Update) {
	rnd := rand.New(rand.NewSource(70707))
	addr = make([]byte, length.Addr)
	rnd.Read(addr)
	a := hex.EncodeToString(addr)

	ub := NewUpdateBuilder()
	ub.Balance(a, 1234)
	for i, tail := range []byte{0x0, 0x4, 0x8} {
		loc := slotLocsForHexPrefix([]byte{0x1, 0x2, tail}, 1, 5000*(i+1))[0]
		doomed = append(doomed, loc)
		ub.Storage(a, loc, "11")
	}
	survivors = slotLocsForHexPrefix([]byte{0x1, 0x2, 0xf}, 4, 900_000)
	for _, loc := range survivors {
		ub.Storage(a, loc, "22")
	}
	for _, tail := range []byte{0x5, 0x9} {
		for _, loc := range slotLocsForHexPrefix([]byte{0x1, tail}, 2, 31_000) {
			ub.Storage(a, loc, "33")
		}
	}
	for _, nib := range []byte{0x3, 0x7, 0xb} {
		for _, loc := range storageLocsForNibble(nib, 2, 77_000) {
			ub.Storage(a, loc, "44")
		}
	}
	for range 2_000 {
		addRandomAccount(ub, rnd, 0)
	}
	pk, upds = ub.Build()
	return addr, doomed, survivors, pk, upds
}

func TestSplitPoint_CollapsedStorageCellReUnfolded(t *testing.T) {
	addr, doomed, survivors, pk1, u1 := splitStorageCollapseCorpus()
	require.Len(t, survivors, 4, "the survivor nibble must hold a sub-branch, not a single slot")

	msSeq := NewMockState(t)
	seq := NewHexPatriciaHashed(length.Addr, msSeq, DefaultTrieConfig())
	defer seq.Release()
	processBatch(t, msSeq, seq, pk1, u1)

	accHash := KeyToHexNibbleHash(addr)
	accPrefix := accHash[:64]
	prefix65 := append(bytes.Clone(accPrefix), 0x1)
	prefix66 := append(bytes.Clone(prefix65), 0x2)
	for _, p := range [][]byte{accPrefix, prefix65, prefix66} {
		require.Truef(t, hasBranchAt(msSeq, p), "corpus must produce an on-disk branch at %x", p)
	}

	msSplit := cloneMockState(t, msSeq)

	a := hex.EncodeToString(addr)
	ub := NewUpdateBuilder()
	ub.Balance(a, 5678)
	for _, loc := range doomed {
		ub.DeleteStorage(a, loc)
	}
	ub.Storage(a, survivors[0], "99")
	pk2, u2 := ub.Build()
	seqRoot := processBatch(t, msSeq, seq, pk2, u2)
	require.NoError(t, msSplit.applyPlainUpdates(pk2, u2))

	all := splitKVs(pk2, u2)
	var storage []splitKV
	var reUnfold []splitKV
	laterHK := KeyToHexNibbleHash(storageKey(addr, common.Hex2Bytes(survivors[0])))
	for _, k := range all {
		if len(k.hk) != 128 || !bytes.HasPrefix(k.hk, accPrefix) {
			continue
		}
		storage = append(storage, k)
		if bytes.Equal(k.hk, laterHK) {
			reUnfold = append(reUnfold, k)
		}
	}
	require.Len(t, reUnfold, 1, "the surviving slot must be touched so the base descends through the collapsed cell")

	r := newSplitRound(msSplit)
	r.splits[string(prefix65)] = true
	r.splits[string(prefix66)] = true
	r.replay[string(prefix65)] = reUnfold

	sr, err := r.foldAt(accPrefix, storage)
	require.NoError(t, err)

	collapsed, ok := r.captured[string(prefix66)]
	require.True(t, ok, "the interior split at depth 66 did not run")
	require.Positive(t, collapsed.extLen, "the split at depth 66 must collapse to a single survivor")
	require.EqualValues(t, 0xf, collapsed.extension[0], "the survivor nibble must lead the extension")
	require.Zero(t, collapsed.accountAddrLen+collapsed.storageAddrLen, "the survivor must be keyless, not a single-slot leaf")

	r.seams[string(accHash)] = sr
	splitRoot, err := r.foldRoot(all)
	require.NoError(t, err)
	r.flush(t)

	require.Equalf(t, seqRoot, splitRoot,
		"a collapsed cell at %x re-unfolded by a later key in the same round != sequential root", prefix66)
	requireBranchParity(t, msSeq, msSplit)
}

func requireBranchParityExceptTouchMapAt(t *testing.T, seq, got *MockState, prefix []byte) {
	t.Helper()
	key := string(nibbles.HexToCompact(prefix))
	sb, sok := seq.cm[key]
	pb, pok := got.cm[key]
	require.Truef(t, sok && pok, "both engines must store a record at %x", prefix)
	require.Equalf(t, sb[2:], pb[2:],
		"record at %x must match serial in afterMap and cells; only its touch bitmap may differ, because the collapse writes the deletions as a separate delete record before the row is rebuilt on re-entry", prefix)
	masked := cloneMockState(t, seq)
	masked.cm[key] = pb
	requireBranchParity(t, masked, got)
}

func decodedRowCell(t *testing.T, ms *MockState, prefix []byte, nib byte) cell {
	t.Helper()
	base := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer base.Release()
	require.NoError(t, unfoldSplitBase(base, prefix))
	return base.grid[0][nib]
}

type survivorKind uint8

const (
	survivorBareRoot survivorKind = iota
	survivorExtRoot
	survivorEOA
)

func splitKeyedSurvivorCorpus(kind survivorKind) (doomed [][]byte, touch [][]byte, surv []byte, pk [][]byte, upds []Update) {
	rnd := rand.New(rand.NewSource(515151))
	ub := NewUpdateBuilder()
	for _, tail := range []byte{0x0, 0x4, 0x8} {
		a := findAddressForHexPrefix([]byte{0x1, 0x2, 0x3, tail}, int(tail)+301)
		doomed = append(doomed, a)
		ub.Balance(hex.EncodeToString(a), uint64(tail)+7)
	}

	surv = findAddressForHexPrefix([]byte{0x1, 0x2, 0x3, 0xf}, 359)
	sa := hex.EncodeToString(surv)
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

	for _, p := range [][]byte{{0x1, 0x2, 0x7}, {0x1, 0x2, 0xc}, {0x1, 0x7}, {0x1, 0xc}} {
		for seed := range 2 {
			a := findAddressForHexPrefix(p, seed*17+401)
			ub.Balance(hex.EncodeToString(a), uint64(seed)+53)
			if seed == 0 && (len(p) == 2 || p[2] == 0x7) {
				touch = append(touch, a)
			}
		}
	}
	for range 400 {
		addRandomAccount(ub, rnd, 0)
	}
	pk, upds = ub.Build()
	return doomed, touch, surv, pk, upds
}

func TestSplitPoint_KeyedSurvivorCollapse(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind survivorKind
	}{
		{"storage-root-hash", survivorBareRoot},
		{"storage-root-extension", survivorExtRoot},
		{"eoa", survivorEOA},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doomed, touch, surv, pk1, u1 := splitKeyedSurvivorCorpus(tc.kind)

			msSeq := NewMockState(t)
			seq := NewHexPatriciaHashed(length.Addr, msSeq, DefaultTrieConfig())
			defer seq.Release()
			processBatch(t, msSeq, seq, pk1, u1)

			prefix := []byte{0x1, 0x2, 0x3}
			for _, p := range [][]byte{prefix[:1], prefix[:2], prefix} {
				require.Truef(t, hasBranchAt(msSeq, p), "corpus must produce an on-disk branch at %x", p)
			}

			msSplit := cloneMockState(t, msSeq)

			survCell := decodedRowCell(t, msSplit, prefix, 0xf)
			require.Positivef(t, survCell.accountAddrLen, "the survivor decoded from the record at %x must be an account", prefix)
			switch tc.kind {
			case survivorEOA:
				require.Zero(t, survCell.hashLen, "an EOA survivor carries no storage root hash")
			case survivorExtRoot:
				require.Positive(t, survCell.hashLen, "the survivor account must carry a storage root hash")
				require.Positive(t, survCell.extLen, "the survivor's storage root must be an extension")
			default:
				require.Positive(t, survCell.hashLen, "the survivor account must carry a storage root hash")
				require.Zero(t, survCell.extLen, "the survivor's storage root must be a bare branch hash")
			}
			require.Positive(t, survCell.hashedExtLen, "the decoded survivor must carry its hashed path from the row depth")

			survHK := KeyToHexNibbleHash(surv)
			var later []byte
			for x := byte(0); x < 16; x++ {
				if x == 0xf || x == survHK[len(prefix)+1] {
					continue
				}
				later = findAddressForHexPrefix(append(append([]byte{}, prefix...), x), 373)
				break
			}
			laterHK := KeyToHexNibbleHash(later)
			require.Equal(t, prefix, laterHK[:len(prefix)], "the later key must descend into the promoted survivor's slot")
			require.NotEqual(t, survHK[len(prefix)+1], laterHK[len(prefix)],
				"the later key's next nibble must differ from the survivor's, so a stale hashed path misroutes instead of colliding")

			ub := NewUpdateBuilder()
			for _, a := range doomed {
				ub.Delete(hex.EncodeToString(a))
			}
			for i, a := range touch {
				ub.Balance(hex.EncodeToString(a), uint64(i)+7001)
			}
			ub.Balance(hex.EncodeToString(later), 7777)
			pk2, u2 := ub.Build()
			seqRoot := processBatch(t, msSeq, seq, pk2, u2)
			require.NoError(t, msSplit.applyPlainUpdates(pk2, u2))

			all := splitKVs(pk2, u2)
			var reUnfold []splitKV
			for _, k := range all {
				if bytes.Equal(k.hk, laterHK) {
					reUnfold = append(reUnfold, k)
				}
			}
			require.Len(t, reUnfold, 1, "the later key must be touched so the parent base descends through the promoted survivor")

			r := newSplitRound(msSplit)
			r.splits[string(prefix[:1])] = true
			r.splits[string(prefix[:2])] = true
			r.splits[string(prefix)] = true
			r.replay[string(prefix[:2])] = reUnfold
			splitRoot, err := r.foldRoot(all)
			require.NoError(t, err)
			r.flush(t)

			collapsed, ok := r.captured[string(prefix)]
			require.True(t, ok, "the interior split at 123 did not run")
			require.Equalf(t, survCell.accountAddrLen, collapsed.accountAddrLen,
				"a collapse onto a keyed survivor must promote the account, not wrap its storage root in an extension cell; got accountAddrLen=%d extLen=%d",
				collapsed.accountAddrLen, collapsed.extLen)

			require.Equalf(t, seqRoot, splitRoot, "root with a re-entry through the promoted keyed survivor at %x != sequential", prefix)
			requireBranchParityExceptTouchMapAt(t, msSeq, msSplit, prefix)

			require.Equal(t, survCell.hashedExtLen+1, collapsed.hashedExtLen,
				"the promoted survivor's hashed path must be measured from the parent row, one nibble longer")
			require.EqualValues(t, 0xf, collapsed.hashedExtension[0], "the promoted survivor's hashed path must start with its nibble in the collapsed row")
			require.Equal(t, survCell.hashedExtension[:survCell.hashedExtLen], collapsed.hashedExtension[1:collapsed.hashedExtLen],
				"the promoted survivor's hashed path must continue with the path it had in the collapsed row")
		})
	}
}

func splitMultiChildReentryCorpus() (splitTouch []byte, later []byte, touch [][]byte, pk [][]byte, upds []Update) {
	rnd := rand.New(rand.NewSource(818181))
	ub := NewUpdateBuilder()
	for seed := range 2 {
		a := findAddressForHexPrefix([]byte{0x1, 0x2, 0x3, 0x0}, seed*23+601)
		ub.Balance(hex.EncodeToString(a), uint64(seed)+11)
		if seed == 0 {
			splitTouch = a
		}
	}
	later = findAddressForHexPrefix([]byte{0x1, 0x2, 0x3, 0x1}, 631)
	ub.Balance(hex.EncodeToString(later), 71)

	for _, p := range [][]byte{{0x1, 0x2, 0x7}, {0x1, 0x2, 0xc}, {0x1, 0x7}, {0x1, 0xc}} {
		for seed := range 2 {
			a := findAddressForHexPrefix(p, seed*29+661)
			ub.Balance(hex.EncodeToString(a), uint64(seed)+37)
			if seed == 0 && (len(p) == 2 || p[2] == 0x7) {
				touch = append(touch, a)
			}
		}
	}
	for range 400 {
		addRandomAccount(ub, rnd, 0)
	}
	pk, upds = ub.Build()
	return splitTouch, later, touch, pk, upds
}

func TestSplitPoint_MultiChildSplitCellReUnfolded(t *testing.T) {
	splitTouch, later, touch, pk1, u1 := splitMultiChildReentryCorpus()

	msSeq := NewMockState(t)
	seq := NewHexPatriciaHashed(length.Addr, msSeq, DefaultTrieConfig())
	defer seq.Release()
	processBatch(t, msSeq, seq, pk1, u1)

	prefix := []byte{0x1, 0x2, 0x3}
	for _, p := range [][]byte{prefix[:1], prefix[:2], prefix} {
		require.Truef(t, hasBranchAt(msSeq, p), "corpus must produce an on-disk branch at %x", p)
	}
	laterHK := KeyToHexNibbleHash(later)
	require.EqualValues(t, prefix[0], laterHK[len(prefix)],
		"the later key's continuation must share a nibble with the split prefix so a stale hashed extension misroutes it")

	msSplit := cloneMockState(t, msSeq)

	ub := NewUpdateBuilder()
	ub.Balance(hex.EncodeToString(splitTouch), 8001)
	ub.Balance(hex.EncodeToString(later), 8002)
	for i, a := range touch {
		ub.Balance(hex.EncodeToString(a), uint64(i)+8100)
	}
	pk2, u2 := ub.Build()
	seqRoot := processBatch(t, msSeq, seq, pk2, u2)
	require.NoError(t, msSplit.applyPlainUpdates(pk2, u2))

	all := splitKVs(pk2, u2)
	var reUnfold []splitKV
	for _, k := range all {
		if bytes.Equal(k.hk, laterHK) {
			reUnfold = append(reUnfold, k)
		}
	}
	require.Len(t, reUnfold, 1, "the later key must be touched so the base descends through the split cell")

	r := newSplitRound(msSplit)
	r.splits[string(prefix[:1])] = true
	r.splits[string(prefix[:2])] = true
	r.splits[string(prefix)] = true
	r.replay[string(prefix[:2])] = reUnfold
	splitRoot, err := r.foldRoot(all)
	require.NoError(t, err)
	r.flush(t)

	split, ok := r.captured[string(prefix)]
	require.True(t, ok, "the interior split at 123 did not run")
	require.Zero(t, split.extLen, "a multi-child split returns a bare branch cell")

	require.Equalf(t, seqRoot, splitRoot, "root with a re-entry into the multi-child split cell at %x != sequential", prefix)
	requireBranchParity(t, msSeq, msSplit)

	require.Zerof(t, split.hashedExtLen,
		"a multi-child split cell must not carry the absolute split prefix as a hashed extension; got %x", split.hashedExtension[:split.hashedExtLen])
}
