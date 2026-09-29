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
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/eip8297/witness"
)

type pbinOracleNode struct {
	path []byte
	blob []byte
	hash common.Hash
}

func TestPBinWitnessResolverRowsMatchModel(t *testing.T) {
	pbinUseBlake3(t)
	entries := append(pbinResolverEntries(), randomEntries(0x8297, 96)...)
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
	ops := make([]Op, len(entries))
	for i, entry := range entries {
		copyValue := [eip8297.ValueLength]byte{}
		copy(copyValue[:], entry.Value)
		ops[i] = Op{Key: entry.Key, Value: copyValue}
	}
	ctx := newTrieTestContext()
	engineRoot, err := NewTrie(ctx).Process(ops)
	require.NoError(t, err)
	wantNodes, wantRoot := pbinOracleNodes(t, entries)
	require.Equal(t, wantRoot, engineRoot)
	resolver := NewPBinWitnessResolver(ctx)
	resolvedRoot, err := resolver.RootHash()
	require.NoError(t, err)
	require.Equal(t, wantRoot, resolvedRoot)
	for _, want := range wantNodes {
		got, err := resolver.Resolve(want.path)
		require.NoErrorf(t, err, "path %x", want.path)
		require.Equal(t, want.blob, got, "blob comparison at %x", want.path)
	}
	for _, path := range pbinResolverAbsentPaths() {
		got, err := resolver.Resolve(path)
		require.NoError(t, err)
		require.Nil(t, got, "empty path %x returned a neighbour", path)
	}
}

func TestPBinWitnessResolverUsesParentRows(t *testing.T) {
	pbinUseBlake3(t)
	entries := pbinResolverEntries()[:8]
	parent := newTrieTestContext()
	_, err := NewTrie(parent).Process(entriesToOps(entries))
	require.NoError(t, err)
	latest := newTrieTestContext()
	latest.records = cloneRecords(parent.records)
	rewrite := entries[0]
	rewrite.Value = testTrieValueBytes(0x88)
	_, err = NewTrie(latest).Process(entriesToOps([]eip8297.Entry{rewrite}))
	require.NoError(t, err)
	history := &pbinResolverHistoryContext{latest: latest, parent: parent.records}
	wantNodes, wantRoot := pbinOracleNodes(t, entries)
	resolver := NewPBinWitnessResolver(history)
	gotRoot, err := resolver.RootHash()
	require.NoError(t, err)
	require.Equal(t, wantRoot, gotRoot)
	got, err := resolver.Resolve(nil)
	require.NoError(t, err)
	require.Equal(t, wantNodes[0].blob, got, "parent root blob")
	require.NotEqual(t, latest.records[string(GlobalRootKey())], got)
}

func TestPBinWitnessResolverRejectsBadRows(t *testing.T) {
	pbinUseBlake3(t)
	entries := pbinResolverEntries()[:8]
	t.Run("missing row", func(t *testing.T) {
		ctx := newTrieTestContext()
		_, err := NewTrie(ctx).Process(entriesToOps(entries))
		require.NoError(t, err)
		wantNodes, _ := pbinOracleNodes(t, entries)
		var removed string
		for key := range ctx.records {
			if key != string(GlobalRootKey()) {
				removed = key
				delete(ctx.records, key)
				break
			}
		}
		require.NotEmpty(t, removed)
		resolver := NewPBinWitnessResolver(ctx)
		_, err = resolver.Resolve(wantNodes[len(wantNodes)-1].path)
		require.ErrorContains(t, err, "row")
	})
	t.Run("corrupt row", func(t *testing.T) {
		ctx := newTrieTestContext()
		_, err := NewTrie(ctx).Process(entriesToOps(entries))
		require.NoError(t, err)
		ctx.records[string(GlobalRootKey())] = []byte{0xff}
		_, err = NewPBinWitnessResolver(ctx).RootHash()
		require.Error(t, err)
	})
	t.Run("group depth", func(t *testing.T) {
		key := eip8297.TreeKeyAccount([]byte{0x99}, 0)
		group := witness.PBinGroup{Position: 1, Stem: bytes.Clone(key[:len(key)-1]), Subs: []byte{0, 1}, Values: [][]byte{testTrieValueBytes(1), testTrieValueBytes(2)}}
		blob, err := witness.PBinEncodeGroup(group)
		require.NoError(t, err)
		require.ErrorContains(t, pbinValidateWitnessBlob(nil, blob), "group position")
	})
}

func TestPBinWitnessResolverRejectsCorruptGroup(t *testing.T) {
	pbinUseBlake3(t)
	var codeHash common.Hash
	entries := []eip8297.Entry{
		{Key: eip8297.TreeKeyCodeChunk(codeHash, 0), Value: pbinResolverValue(1, 1)},
		{Key: eip8297.TreeKeyCodeChunk(codeHash, 1), Value: pbinResolverValue(1, 2)},
	}
	t.Run("root", func(t *testing.T) {
		ctx := newTrieTestContext()
		_, err := NewTrie(ctx).Process(entriesToOps(entries))
		require.NoError(t, err)
		pbinCorruptLeaf(t, ctx, entries[1].Key, 6)
		_, err = NewPBinWitnessResolver(ctx).Resolve(nil)
		require.ErrorContains(t, err, "pointer")
	})
	t.Run("below branch", func(t *testing.T) {
		all := append(pbinResolverEntries(), randomEntries(0x8297, 96)...)
		sort.Slice(all, func(i, j int) bool { return bytes.Compare(all[i].Key, all[j].Key) < 0 })
		ctx := newTrieTestContext()
		_, err := NewTrie(ctx).Process(entriesToOps(all))
		require.NoError(t, err)
		wantNodes, _ := pbinOracleNodes(t, all)
		var target []byte
		for _, node := range wantNodes {
			decoded, decodeErr := witness.PBinDecodeBlob(node.blob)
			require.NoError(t, decodeErr)
			if decoded.Group != nil {
				target = node.path
				break
			}
		}
		require.NotEmpty(t, target)
		_, err = NewPBinWitnessResolver(ctx).Resolve(target)
		require.NoError(t, err)
		var corruptKey []byte
		for _, node := range wantNodes {
			decoded, decodeErr := witness.PBinDecodeBlob(node.blob)
			require.NoError(t, decodeErr)
			if decoded.Group != nil && bytes.Equal(node.path, target) {
				corruptKey = append(append([]byte{}, decoded.Group.Stem...), decoded.Group.Subs[0])
				break
			}
		}
		require.NotEmpty(t, corruptKey)
		pbinCorruptLeaf(t, ctx, corruptKey, 6)
		_, err = NewPBinWitnessResolver(ctx).Resolve(target)
		require.Error(t, err)
	})
}

func TestPBinWitnessResolverRootReadBound(t *testing.T) {
	pbinUseBlake3(t)
	for _, count := range []int{10000, 100000} {
		t.Run(fmt.Sprintf("%d", count), func(t *testing.T) {
			entries := pbinLargeEntries(int64(0x8297+count), count)
			sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
			ctx := newTrieTestContext()
			_, err := NewTrie(ctx).Process(entriesToOps(entries))
			require.NoError(t, err)
			resolver := NewPBinWitnessResolver(ctx)
			reads := 0
			read := resolver.read
			resolver.read = func(key []byte) ([]byte, error) {
				reads++
				return read(key)
			}
			_, err = resolver.RootHash()
			require.NoError(t, err)
			require.LessOrEqual(t, reads, 2, "root resolution reads the whole tree")
			path := pbinLargeNodePath(t, ctx)
			reads = 0
			_, err = resolver.Resolve(witness.PBinPath(&path))
			require.NoError(t, err)
			require.LessOrEqual(t, reads, int(path.BitLen/4)+17, "single-node resolution reads unrelated rows")
		})
	}
}

func pbinLargeNodePath(t *testing.T, ctx *trieTestContext) eip8297.Bitpath {
	t.Helper()
	data := ctx.records[string(GlobalRootKey())]
	record, err := DecodeRecord(GlobalRootKey(), data)
	require.NoError(t, err)
	if record.Form == ExtRoot {
		path := record.SelfExt
		path.AppendBit(0)
		return path
	}
	slots := occupiedSlots(&record)
	result, err := FoldRow(GlobalRootKey(), &record)
	require.NoError(t, err)
	path := rowPrefix(&eip8297.Bitpath{}, slots[0], 0, result.Split)
	path.AppendBit(0)
	return path
}

func pbinLargeEntries(seed int64, count int) []eip8297.Entry {
	entries := randomEntries(seed, count)
	for i := range entries {
		seed := byte(i)
		switch entries[i].Key[0] {
		case eip8297.AccountZone:
			switch entries[i].Key[len(entries[i].Key)-1] {
			case eip8297.BasicDataLeafKey:
				entries[i].Value = testTrieValueBytes(seed)
			case eip8297.DelegationLeafKey:
				delegation := append([]byte{}, eip8297.DelegationMarker[:]...)
				delegation = append(delegation, make([]byte, eip8297.DelegationCodeLength-len(delegation))...)
				encoded := eip8297.EncodeDelegation(delegation)
				entries[i].Value = encoded[:]
			default:
				entries[i].Value = pbinResolverValue(seed, seed)
			}
		default:
			entries[i].Value = pbinResolverValue(seed, seed)
		}
	}
	return entries
}

func pbinCorruptLeaf(t *testing.T, ctx *trieTestContext, key []byte, suffix byte) {
	t.Helper()
	for recordKey, raw := range ctx.records {
		record, err := DecodeRecord([]byte(recordKey), raw)
		require.NoError(t, err)
		for slot := range record.Cells {
			cell := &record.Cells[slot]
			if cell.Kind != LeafCell || !bytes.Equal(cell.Key, key) {
				continue
			}
			cell.Value[len(cell.Value)-1] = suffix
			ctx.records[recordKey], err = EncodeRecord([]byte(recordKey), &record)
			require.NoError(t, err)
			return
		}
	}
	t.Fatalf("leaf %x not found", key)
}

func pbinUseBlake3(t *testing.T) {
	previous := eip8297.HashSuiteName()
	t.Cleanup(func() { require.NoError(t, eip8297.SetHashSuite(previous)) })
	require.NoError(t, eip8297.SetHashSuite(eip8297.HashBlake3))
}

func pbinResolverEntries() []eip8297.Entry {
	entries := make([]eip8297.Entry, 0, 500)
	for group, count := range []int{1, 2, 17, 64, 128, 256} {
		address := []byte{byte(group + 1)}
		for sub := range count {
			value := pbinResolverValue(byte(group), byte(sub))
			if sub == eip8297.BasicDataLeafKey {
				value = testTrieValueBytes(byte(group + 1))
			} else if sub == eip8297.DelegationLeafKey {
				delegation := append([]byte{}, eip8297.DelegationMarker[:]...)
				delegation = append(delegation, make([]byte, eip8297.DelegationCodeLength-len(delegation))...)
				encoded := eip8297.EncodeDelegation(delegation)
				value = encoded[:]
			}
			entries = append(entries, eip8297.Entry{Key: eip8297.TreeKeyAccount(address, byte(sub)), Value: value})
		}
	}
	address := []byte{0x40}
	for _, slot := range []uint64{64, 65, 256, 257, 1000} {
		entries = append(entries, eip8297.Entry{Key: eip8297.TreeKeyStorage(address, pbinResolverSlot(slot)), Value: pbinResolverValue(0x40, byte(slot))})
	}
	codeHash := common.Hash{0x55}
	for chunk := range 3 {
		entries = append(entries, eip8297.Entry{Key: eip8297.TreeKeyCodeChunk(codeHash, chunk), Value: pbinResolverValue(0x60, byte(chunk))})
	}
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
	return entries
}

func entriesToOps(entries []eip8297.Entry) []Op {
	ops := make([]Op, len(entries))
	for i, entry := range entries {
		var value [eip8297.ValueLength]byte
		copy(value[:], entry.Value)
		ops[i] = Op{Key: entry.Key, Value: value}
	}
	return ops
}

type pbinResolverHistoryContext struct {
	latest *trieTestContext
	parent map[string][]byte
}

func (c *pbinResolverHistoryContext) Branch(key []byte) ([]byte, kv.Step, error) {
	return c.latest.Branch(key)
}

func (c *pbinResolverHistoryContext) ParentBranch(key []byte) ([]byte, kv.Step, error) {
	return append([]byte(nil), c.parent[string(key)]...), 0, nil
}

func (c *pbinResolverHistoryContext) PutBranch(key, data, prev []byte) error {
	return c.latest.PutBranch(key, data, prev)
}

func (*pbinResolverHistoryContext) Account([]byte) (*commitment.Update, error) {
	return nil, fmt.Errorf("unexpected account read")
}

func (*pbinResolverHistoryContext) Storage([]byte) (*commitment.Update, error) {
	return nil, fmt.Errorf("unexpected storage read")
}

func pbinResolverValue(prefix, suffix byte) []byte {
	value := make([]byte, eip8297.ValueLength)
	value[0] = prefix
	value[len(value)-1] = suffix
	return value
}

func pbinResolverSlot(value uint64) []byte {
	slot := make([]byte, 32)
	for index := range 8 {
		slot[len(slot)-1-index] = byte(value >> (8 * index))
	}
	return slot
}

func pbinResolverAbsentPaths() [][]byte {
	bits := bytes.Repeat([]byte{0xff}, 66)
	longPath := eip8297.PathFromBits(bits, 527)
	shortPath := eip8297.PathFromBits(bits, 526)
	return [][]byte{
		witness.PBinPath(&longPath),
		witness.PBinPath(&shortPath),
	}
}

func pbinOracleNodes(t *testing.T, entries []eip8297.Entry) ([]pbinOracleNode, common.Hash) {
	t.Helper()
	var walk eip8297.Bitpath
	byPath := make(map[string]pbinOracleNode)
	root := pbinOracleBuild(t, entries, walk, byPath)
	paths := make([]string, 0, len(byPath))
	for path := range byPath {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	result := make([]pbinOracleNode, 0, len(paths))
	for _, path := range paths {
		result = append(result, byPath[path])
	}
	return result, root.hash
}

func pbinOracleBuild(t *testing.T, entries []eip8297.Entry, walk eip8297.Bitpath, nodes map[string]pbinOracleNode) pbinOracleNode {
	t.Helper()
	if len(entries) == 0 {
		t.Fatal("empty oracle subtree")
	}
	if len(entries) == 1 {
		blob, err := witness.PBinEncodeLeaf(entries[0].Key, entries[0].Value)
		require.NoError(t, err)
		return pbinOracleStore(walk, blob, nodes)
	}
	stem := entries[0].Key[:len(entries[0].Key)-1]
	allStem := true
	for _, entry := range entries[1:] {
		if !bytes.Equal(stem, entry.Key[:len(entry.Key)-1]) {
			allStem = false
			break
		}
	}
	if allStem {
		group := witness.PBinGroup{Position: uint16(walk.BitLen), Stem: bytes.Clone(stem), Subs: make([]byte, len(entries)), Values: make([][]byte, len(entries))}
		for i, entry := range entries {
			group.Subs[i] = entry.Key[len(entry.Key)-1]
			group.Values[i] = bytes.Clone(entry.Value)
		}
		if len(entries) == 1 {
			blob, err := witness.PBinEncodeLeaf(entries[0].Key, entries[0].Value)
			require.NoError(t, err)
			return pbinOracleStore(walk, blob, nodes)
		}
		blob, err := witness.PBinEncodeGroup(group)
		require.NoError(t, err)
		return pbinOracleStore(walk, blob, nodes)
	}
	firstPath := eip8297.PathFromBytes(entries[0].Key)
	lastPath := eip8297.PathFromBytes(entries[len(entries)-1].Key)
	divergence := walk.BitLen
	for divergence < firstPath.BitLen && divergence < lastPath.BitLen && firstPath.Bit(divergence) == lastPath.Bit(divergence) {
		divergence++
	}
	prefix := firstPath.Slice(walk.BitLen, divergence)
	leftEntries := make([]eip8297.Entry, 0, len(entries))
	rightEntries := make([]eip8297.Entry, 0, len(entries))
	for _, entry := range entries {
		path := eip8297.PathFromBytes(entry.Key)
		if path.Bit(divergence) == 0 {
			leftEntries = append(leftEntries, entry)
		} else {
			rightEntries = append(rightEntries, entry)
		}
	}
	leftWalk := walk
	leftWalk.Append(&prefix)
	leftWalk.AppendBit(0)
	rightWalk := walk
	rightWalk.Append(&prefix)
	rightWalk.AppendBit(1)
	left := pbinOracleBuild(t, leftEntries, leftWalk, nodes)
	right := pbinOracleBuild(t, rightEntries, rightWalk, nodes)
	blob, err := witness.PBinEncodeBranch(&prefix, &left.hash, &right.hash)
	require.NoError(t, err)
	return pbinOracleStore(walk, blob, nodes)
}

func pbinOracleStore(walk eip8297.Bitpath, blob []byte, nodes map[string]pbinOracleNode) pbinOracleNode {
	hash, err := witness.PBinHashBlob(blob)
	if err != nil {
		panic(err)
	}
	node := pbinOracleNode{path: witness.PBinPath(&walk), blob: blob, hash: hash}
	nodes[string(node.path)] = node
	return node
}
