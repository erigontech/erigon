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

package pbt

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
	"lukechampine.com/blake3"

	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/internal/commitmenttest/commitmentflags"
)

func TestFoldLeafRoot(t *testing.T) {
	key := []byte{eip8297.AccountZone, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, eip8297.BasicDataLeafKey}
	value := [eip8297.ValueLength]byte{1}
	record := Record{Form: LeafRoot, Cells: [16]Cell{{Kind: LeafCell, Key: key, Value: value}}}

	got, err := Fold(GlobalRootKey(), &record)
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot([]eip8297.Entry{{Key: key, Value: value[:]}}), got)
}

func TestFoldRootForms(t *testing.T) {
	keyA := foldTestKey(0x00, 0)
	keyB := foldTestStorageKey(1)
	keyC := foldTestKey(0x00, 2)
	keyD := foldTestKey(0x01, 3)
	cases := []struct {
		name    string
		entries []eip8297.Entry
	}{
		{name: "empty"},
		{name: "leaf root", entries: []eip8297.Entry{{Key: keyA, Value: foldTestValue(1)}}},
		{name: "row root", entries: []eip8297.Entry{{Key: keyA, Value: foldTestValue(1)}, {Key: keyB, Value: foldTestValue(2)}}},
		{name: "extension root", entries: []eip8297.Entry{{Key: keyC, Value: foldTestValue(2)}, {Key: keyD, Value: foldTestValue(3)}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			built := buildReferenceRows(tc.entries, nil, eip8297.Bitpath{})
			got, err := Fold(built.rootKey, &built.root)
			require.NoError(t, err)
			require.Equal(t, eip8297.StateRoot(tc.entries), got)
		})
	}
}

func TestFoldSplitsInsideWindow(t *testing.T) {
	for _, offset := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("bit %d", offset), func(t *testing.T) {
			left := make([]byte, eip8297.AccountKeyLength)
			right := make([]byte, eip8297.AccountKeyLength)
			right[1] = 1 << uint(7-offset)
			entries := []eip8297.Entry{{Key: left, Value: foldTestValue(1)}, {Key: right, Value: foldTestValue(2)}}
			built := buildReferenceRows(entries, nil, eip8297.Bitpath{})
			got, err := Fold(built.rootKey, &built.root)
			require.NoError(t, err)
			require.Equal(t, eip8297.StateRoot(entries), got)
		})
	}
}

func TestFoldTopSplitAwayFromWindowOffsetZero(t *testing.T) {
	entries := []eip8297.Entry{
		{Key: foldTestKey(0x00, 1), Value: foldTestValue(1)},
		{Key: foldTestKey(0x00, 2), Value: foldTestValue(2)},
	}
	built := buildReferenceRows(entries, nil, eip8297.Bitpath{})
	require.Equal(t, ExtRoot, built.root.Form)
	got, err := Fold(built.rootKey, &built.root)
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot(entries), got)
}

func TestFoldAdjacentKeyLengths(t *testing.T) {
	account := foldTestKey(0x00, 1)
	storage := make([]byte, eip8297.StorageKeyLength)
	storage[0] = eip8297.StorageZone
	storage[len(storage)-1] = 1
	entries := []eip8297.Entry{{Key: account, Value: foldTestValue(1)}, {Key: storage, Value: foldTestValue(2)}}
	built := buildReferenceRows(entries, nil, eip8297.Bitpath{})
	got, err := Fold(built.rootKey, &built.root)
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot(entries), got)
}

func TestFoldInRowPrefixStartsAfterParentSplit(t *testing.T) {
	testFoldThreeLeafBranch(t)
}

func TestFoldInRowPrefixEndsAtWindowBoundary(t *testing.T) {
	testFoldThreeLeafBranch(t)
}

func TestFoldBucketRootForms(t *testing.T) {
	rootBytes := bytes.Repeat([]byte{0x5a}, 33)
	rootBytes[0] = eip8297.StorageZone
	rootPath := eip8297.PathFromBits(rootBytes, 264)
	keys := []struct {
		name string
		key  []byte
	}{
		{name: "leaf", key: bucketKey(rootPath, 0x00)},
		{name: "row", key: bucketKey(rootPath, 0x80)},
		{name: "extension", key: bucketKey(rootPath, 0x10)},
	}
	for _, tc := range keys {
		t.Run(tc.name, func(t *testing.T) {
			entries := []eip8297.Entry{{Key: bucketKey(rootPath, 0x00), Value: foldTestValue(1)}}
			if tc.name != "leaf" {
				entries = append(entries, eip8297.Entry{Key: tc.key, Value: foldTestValue(2)})
			}
			built := buildReferenceRows(entries, nil, rootPath)
			got, err := Fold(built.rootKey, &built.root)
			require.NoError(t, err)
			want := eip8297.MerkelizeWith(referenceSubtreeForEntries(entries, &rootPath), nil)
			require.Equal(t, want, got)
		})
	}
	built := buildReferenceRows(nil, nil, rootPath)
	got, err := Fold(built.rootKey, &built.root)
	require.NoError(t, err)
	require.Equal(t, eip8297.EmptyTreeHash, got)
}

func TestFoldVectorsThroughStaticBuilder(t *testing.T) {
	commitmentflags.Restore(t)
	require.NoError(t, eip8297.SetHashSuite(eip8297.HashBlake3))
	raw, err := os.ReadFile("../../testdata/binary_trie_vectors.json")
	require.NoError(t, err)
	var vectors struct {
		TrieRoots []struct {
			Name    string `json:"name"`
			Entries []struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			} `json:"entries"`
			Root string `json:"root"`
		} `json:"trie_roots"`
	}
	require.NoError(t, json.Unmarshal(raw, &vectors))
	require.NotEmpty(t, vectors.TrieRoots)
	for _, vector := range vectors.TrieRoots {
		t.Run(vector.Name, func(t *testing.T) {
			entries := make([]eip8297.Entry, 0, len(vector.Entries))
			for _, entry := range vector.Entries {
				key, err := hex.DecodeString(entry.Key[2:])
				require.NoError(t, err)
				value, err := hex.DecodeString(entry.Value[2:])
				require.NoError(t, err)
				entries = append(entries, eip8297.Entry{Key: key, Value: value})
			}
			built := buildReferenceRows(entries, blake3Hash, eip8297.Bitpath{})
			got, err := Fold(built.rootKey, &built.root)
			require.NoError(t, err)
			require.Equal(t, vector.Root, "0x"+fmt.Sprintf("%x", got[:]))
		})
	}
}

func testFoldThreeLeafBranch(t *testing.T) {
	t.Helper()
	entries := []eip8297.Entry{
		{Key: foldTestKey(0x00, 1), Value: foldTestValue(1)},
		{Key: foldTestStorageKey(2), Value: foldTestValue(2)},
		{Key: foldTestStorageKey(3), Value: foldTestValue(3)},
	}
	built := buildReferenceRows(entries, nil, eip8297.Bitpath{})
	got, err := Fold(built.rootKey, &built.root)
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot(entries), got)
}

func TestFoldRowsMatchReferenceSubtrees(t *testing.T) {
	entries := []eip8297.Entry{
		{Key: foldTestKey(0x00, 1), Value: foldTestValue(1)},
		{Key: foldTestKey(0x00, 2), Value: foldTestValue(2)},
	}
	built := buildReferenceRows(entries, nil, eip8297.Bitpath{})
	for key, record := range built.rows {
		result, err := FoldRow([]byte(key), &record)
		require.NoError(t, err)
		branch, ok := built.referenceNodes[key]
		require.True(t, ok)
		expected := referenceFoldResult(branch, built.referenceSplits[key], nil)
		require.Equal(t, expected, result)
	}
}

func TestFoldEveryGeneratedRowMatchesReference(t *testing.T) {
	entries := randomEntries(0xC011A, 64)
	built := buildReferenceRows(entries, nil, eip8297.Bitpath{})
	require.Greater(t, len(built.rows), 1)
	for key, record := range built.rows {
		got, err := FoldRow([]byte(key), &record)
		require.NoError(t, err)
		want := referenceFoldResult(built.referenceNodes[key], built.referenceSplits[key], nil)
		require.Equal(t, want, got, "row %x", key)
	}
}

func TestFoldPropertyMatchesReference(t *testing.T) {
	commitmentflags.Restore(t)
	seeds := []int64{1, 17, 0x8297, 0xDEADBEEF}
	randomSeed := time.Now().UnixNano()
	seeds = append(seeds, randomSeed)
	for _, suite := range []string{eip8297.HashKeccak, eip8297.HashBlake3} {
		require.NoError(t, eip8297.SetHashSuite(suite))
		for _, seed := range seeds {
			for _, count := range []int{1, 7, 64, 1024, 10000} {
				entries := randomEntries(seed, count)
				built := buildReferenceRows(entries, eip8297.SelectedHash(), eip8297.Bitpath{})
				foldReferenceRows(t, &built, eip8297.SelectedHash())
				got, err := Fold(built.rootKey, &built.root)
				require.NoErrorf(t, err, "suite=%s seed=%d count=%d", suite, seed, count)
				want := eip8297.StateRootWithHash(entries, eip8297.SelectedHash())
				require.Equalf(t, want, got, "suite=%s seed=%d count=%d", suite, seed, count)
			}
		}
	}
}

func foldReferenceRows(t *testing.T, built *referenceRows, sum eip8297.HashFn) {
	t.Helper()
	keys := make([]string, 0, len(built.rows))
	for key := range built.rows {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b string) int {
		return int(recordPath([]byte(b)).BitLen - recordPath([]byte(a)).BitLen)
	})
	type parentLink struct {
		key  string
		slot int
	}
	parents := make(map[string][]parentLink, len(keys))
	for parentKey := range built.rows {
		parent := built.rows[parentKey]
		parentPath := recordPath([]byte(parentKey))
		for slot := range parent.Cells {
			cell := &parent.Cells[slot]
			if cell.Kind != BranchCell {
				continue
			}
			split := branchSplit(&rowNode{path: parentPath}, &rowCell{Cell: cell, Kind: cell.Kind})
			childPath, err := rowChildPath(&rowNode{path: parentPath}, slot, cell.Prefix, split)
			require.NoError(t, err)
			childKey := string(rootKey(childPath))
			parents[childKey] = append(parents[childKey], parentLink{key: parentKey, slot: slot})
		}
	}
	results := make(map[string]FoldResult, len(keys))
	for _, key := range keys {
		record := built.rows[key]
		got, err := FoldRow([]byte(key), &record)
		require.NoError(t, err)
		want := referenceFoldResult(built.referenceNodes[key], built.referenceSplits[key], sum)
		require.Equalf(t, want, got, "row %x", key)
		results[key] = got
		for _, link := range parents[key] {
			parent := built.rows[link.key]
			parent.Cells[link.slot].Left, parent.Cells[link.slot].Right = got.Left, got.Right
			built.rows[link.key] = parent
		}
	}
	if built.root.Form == RowRoot {
		built.root = built.rows[string(GlobalRootKey())]
		return
	}
	if built.root.Form != ExtRoot {
		return
	}
	path := built.root.SelfExt.Slice(0, (built.root.SelfExt.BitLen/4)*4)
	if result, ok := results[string(rootKey(path))]; ok {
		built.root.Left, built.root.Right = result.Left, result.Right
	}
}

type referenceRows struct {
	root            Record
	rootKey         []byte
	rows            map[string]Record
	referenceNodes  map[string]eip8297.Node
	referenceSplits map[string]int16
}

func buildReferenceRows(entries []eip8297.Entry, sum eip8297.HashFn, rootPath eip8297.Bitpath) referenceRows {
	var tree eip8297.Tree
	ordered := slices.Clone(entries)
	slices.SortFunc(ordered, func(a, b eip8297.Entry) int { return bytes.Compare(a.Key, b.Key) })
	for _, entry := range ordered {
		tree.Insert(entry.Key, entry.Value)
	}
	result := referenceRows{
		rootKey:         rootKey(rootPath),
		rows:            make(map[string]Record),
		referenceNodes:  make(map[string]eip8297.Node),
		referenceSplits: make(map[string]int16),
	}
	if tree.Root == nil {
		return result
	}
	node := referenceSubtree(tree.Root, 0, &rootPath)
	if leaf, ok := node.(*eip8297.Leaf); ok {
		result.root.Form = LeafRoot
		result.root.Cells[0] = Cell{Kind: LeafCell, Key: slices.Clone(leaf.Key), Value: arrayValue(leaf.Value)}
		return result
	}
	branch := node.(*eip8297.Branch)
	depth := rootPath.BitLen
	split := depth + int16(len(branch.Prefix))
	if split < depth+4 {
		rowPath := rootPath
		result.root.Form = RowRoot
		result.addRow(node, depth, sum)
		result.root = result.rows[string(rootKey(rowPath))]
		return result
	}
	result.root.Form = ExtRoot
	result.root.SelfExt = bitpathFromBits(branch.Prefix)
	result.root.Left = eip8297.MerkelizeWith(branch.Left, sum)
	result.root.Right = eip8297.MerkelizeWith(branch.Right, sum)
	result.addRow(node, depth, sum)
	return result
}

func referenceSubtreeForEntries(entries []eip8297.Entry, path *eip8297.Bitpath) eip8297.Node {
	var tree eip8297.Tree
	for _, entry := range entries {
		tree.Insert(entry.Key, entry.Value)
	}
	return referenceSubtree(tree.Root, 0, path)
}

func (r *referenceRows) addRow(node eip8297.Node, depth int16, sum eip8297.HashFn) {
	branch := node.(*eip8297.Branch)
	split := depth + int16(len(branch.Prefix))
	windowStart := (split / 4) * 4
	firstPath := firstLeafPath(node)
	rowPath := firstPath.Slice(0, windowStart)
	record := collectRow(node, depth, rowPath, sum, r)
	key := rootKey(rowPath)
	if _, exists := r.rows[string(key)]; !exists {
		r.rows[string(key)] = record
		r.referenceNodes[string(key)] = node
		r.referenceSplits[string(key)] = split
	}
}

func collectRow(node eip8297.Node, depth int16, rowPath eip8297.Bitpath, sum eip8297.HashFn, rows *referenceRows) Record {
	record := Record{Form: RowRoot}
	windowStart := rowPath.BitLen
	windowEnd := windowStart + 4
	collectWindow(node, depth, windowStart, windowEnd, &record, sum, rows)
	return record
}

func collectWindow(node eip8297.Node, depth, windowStart, windowEnd int16, record *Record, sum eip8297.HashFn, rows *referenceRows) {
	switch current := node.(type) {
	case *eip8297.Leaf:
		path := eip8297.PathFromBits(current.Key, int16(len(current.Key)*8))
		slot := int(slotFromPath(&path, windowStart))
		record.Cells[slot] = Cell{Kind: LeafCell, Key: slices.Clone(current.Key), Value: arrayValue(current.Value)}
	case *eip8297.Branch:
		split := depth + int16(len(current.Prefix))
		if split < windowEnd {
			collectWindow(current.Left, split+1, windowStart, windowEnd, record, sum, rows)
			collectWindow(current.Right, split+1, windowStart, windowEnd, record, sum, rows)
			return
		}
		path := firstLeafPath(node)
		slot := int(slotFromPath(&path, windowStart))
		prefix := path.Slice(windowEnd, split)
		record.Cells[slot] = Cell{
			Kind:   BranchCell,
			Prefix: prefix,
			Left:   eip8297.MerkelizeWith(current.Left, sum),
			Right:  eip8297.MerkelizeWith(current.Right, sum),
		}
		rows.addRow(node, depth, sum)
	}
}

func referenceSubtree(node eip8297.Node, depth int16, path *eip8297.Bitpath) eip8297.Node {
	if depth == path.BitLen {
		return node
	}
	if _, ok := node.(*eip8297.Leaf); ok {
		return node
	}
	branch := node.(*eip8297.Branch)
	split := depth + int16(len(branch.Prefix))
	if path.BitLen <= split {
		offset := path.BitLen - depth
		return &eip8297.Branch{Prefix: slices.Clone(branch.Prefix[offset:]), Left: branch.Left, Right: branch.Right}
	}
	if path.Bit(split) == 0 {
		return referenceSubtree(branch.Left, split+1, path)
	}
	return referenceSubtree(branch.Right, split+1, path)
}

func firstLeafPath(node eip8297.Node) eip8297.Bitpath {
	var leaf *eip8297.Leaf
	var find func(eip8297.Node)
	find = func(current eip8297.Node) {
		if leaf != nil {
			return
		}
		switch value := current.(type) {
		case *eip8297.Leaf:
			leaf = value
		case *eip8297.Branch:
			find(value.Left)
			find(value.Right)
		}
	}
	find(node)
	return eip8297.PathFromBits(leaf.Key, int16(len(leaf.Key)*8))
}

func slotFromPath(path *eip8297.Bitpath, start int16) uint64 {
	var slot uint64
	for i := range 4 {
		slot |= path.Bit(start+int16(i)) << uint(3-i)
	}
	return slot
}

func rootKey(path eip8297.Bitpath) []byte {
	if path.BitLen == 0 {
		return GlobalRootKey()
	}
	return eip8297.AppendBitPath(nil, &path)
}

func bucketKey(path eip8297.Bitpath, firstSuffix byte) []byte {
	key := make([]byte, eip8297.StorageKeyLength)
	packed := path.AppendPackedBits(nil)
	copy(key, packed)
	key[33] = firstSuffix
	key[65] = 1
	return key
}

func arrayValue(value []byte) [eip8297.ValueLength]byte {
	var out [eip8297.ValueLength]byte
	copy(out[:], value)
	return out
}

func bitpathFromBits(bits []byte) eip8297.Bitpath {
	var path eip8297.Bitpath
	for _, bit := range bits {
		path.AppendBit(uint64(bit))
	}
	return path
}

func foldTestKey(first, seed byte) []byte {
	key := make([]byte, eip8297.AccountKeyLength)
	key[0] = first
	key[len(key)-1] = seed
	return key
}

func foldTestStorageKey(seed byte) []byte {
	key := make([]byte, eip8297.StorageKeyLength)
	key[0] = eip8297.StorageZone
	key[len(key)-1] = seed
	return key
}

func foldTestValue(seed byte) []byte {
	value := make([]byte, eip8297.ValueLength)
	value[len(value)-1] = seed
	return value
}

func blake3Hash(data []byte) common.Hash {
	return common.Hash(blake3.Sum256(data))
}

func randomEntries(seed int64, count int) []eip8297.Entry {
	rng := rand.New(rand.NewSource(seed))
	entries := make([]eip8297.Entry, 0, count)
	seen := make(map[string]struct{}, count)
	for len(entries) < count {
		zone := len(entries) % 3
		keyLen := eip8297.AccountKeyLength
		zoneByte := byte(eip8297.AccountZone)
		if zone == 1 {
			zoneByte = eip8297.CodeZone
		} else if zone == 2 {
			keyLen = eip8297.StorageKeyLength
			zoneByte = eip8297.StorageZone
		}
		key := make([]byte, keyLen)
		_, _ = rng.Read(key)
		key[0] = zoneByte
		if _, exists := seen[string(key)]; exists {
			continue
		}
		seen[string(key)] = struct{}{}
		value := make([]byte, eip8297.ValueLength)
		_, _ = rng.Read(value)
		entries = append(entries, eip8297.Entry{Key: key, Value: value})
	}
	return entries
}

func recordPath(key []byte) eip8297.Bitpath {
	if bytes.Equal(key, GlobalRootKey()) {
		return eip8297.Bitpath{}
	}
	path, err := eip8297.DecodeBitPath(key)
	if err != nil {
		panic(err)
	}
	return path
}

func referenceFoldResult(node eip8297.Node, split int16, sum eip8297.HashFn) FoldResult {
	branch := node.(*eip8297.Branch)
	return FoldResult{Split: split, Left: eip8297.MerkelizeWith(branch.Left, sum), Right: eip8297.MerkelizeWith(branch.Right, sum)}
}
