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
	"math/bits"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func TestCollectUpdateV3WritesOnlyChangedChildRecords(t *testing.T) {
	t.Parallel()

	path := []byte{1, 2, 3, 4}
	prefix := nibbles.HexToCompact(path)
	var cells [16]cellEncodeData
	cells[3] = recordTestData("account", []byte{5, 6})
	cells[7] = recordTestData("branch", []byte{7, 8})
	cells[7].branchMask = 0x0a0b

	ctx := &recordingCtx{}
	be := NewBranchEncoder(1024)
	be.setEdgeRecords(true)

	require.NoError(t, be.CollectUpdate(ctx, prefix, 1<<3, 1<<3|1<<7, 1<<3|1<<7, &cells, false))
	require.Zero(t, ctx.branchCalls, "v3 writes must not read the previous bundled row")
	require.Len(t, ctx.puts, 1, "one changed child must produce one record")

	wantKey := nibbles.ChildKeyV3(nibbles.EncodeKeyV3(path), 3)
	require.Equal(t, wantKey, ctx.puts[0].prefix)
	require.Equal(t, EncodeLeafChild(&cells[3]), ctx.puts[0].data)
	require.Nil(t, ctx.puts[0].prev)

	require.NotEqual(t, nibbles.EncodeKeyV3(path), ctx.puts[0].prefix, "the node key must not be written as a separate record")
}

func TestCollectUpdateV3StoresBranchMaskOnParentEdge(t *testing.T) {
	t.Parallel()

	path := []byte{0xa, 0xb, 0xc}
	prefix := nibbles.HexToCompact(path)
	var cells [16]cellEncodeData
	cells[5] = recordTestData("branch", []byte{1, 2, 3})
	cells[5].branchMask = 0x1234

	ctx := &recordingCtx{}
	be := NewBranchEncoder(1024)
	be.setEdgeRecords(true)
	require.NoError(t, be.CollectUpdate(ctx, prefix, 1<<5, 1<<5, 1<<5, &cells, false))

	require.Len(t, ctx.puts, 1)
	require.Equal(t, nibbles.ChildKeyV3(nibbles.EncodeKeyV3(path), 5), ctx.puts[0].prefix)
	var decoded cell
	mask, err := DecodeRecordInto(ctx.puts[0].data, &decoded)
	require.NoError(t, err)
	require.Equal(t, uint16(0x1234), mask)
	require.Equal(t, int16(3), decoded.extLen)
	require.Equal(t, []byte{1, 2, 3}, decoded.extension[:decoded.extLen])

	for _, put := range ctx.puts {
		require.False(t, bytes.Equal(nibbles.EncodeKeyV3(path), put.prefix), "a node record must not carry the branch mask")
	}
}

func TestCollectDeferredUpdateV3WritesRecordsWithoutRowMerge(t *testing.T) {
	t.Parallel()

	path := []byte{2, 4, 6}
	prefix := nibbles.HexToCompact(path)
	var cells [16]cellEncodeData
	cells[1] = recordTestData("account", nil)
	cells[9] = recordTestData("storage", nil)

	ctx := &recordingCtx{}
	be := NewBranchEncoder(1024)
	be.setDeferUpdates(true)
	be.setEdgeRecords(true)
	require.NoError(t, be.CollectDeferredUpdate(ctx, prefix, 1<<1|1<<9, 1<<1|1<<9, 1<<1|1<<9, &cells, false))
	require.Zero(t, ctx.branchCalls)
	require.Len(t, be.deferred, 2)
	require.NoError(t, be.ApplyDeferredUpdates(1, ctx.PutBranch))
	require.Len(t, ctx.puts, 2)
	require.Equal(t, nibbles.ChildKeyV3(nibbles.EncodeKeyV3(path), 1), ctx.puts[0].prefix)
	require.Equal(t, nibbles.ChildKeyV3(nibbles.EncodeKeyV3(path), 9), ctx.puts[1].prefix)
}

func TestV3RootMaskIsEncodedInStateBlob(t *testing.T) {
	t.Parallel()

	hph := newHexPatriciaHashed()
	hph.rootMask = 0x4321

	encoded, err := hph.EncodeCurrentState(nil)
	require.NoError(t, err)

	var decoded state
	require.NoError(t, decoded.Decode(encoded))
	require.Equal(t, uint16(0x4321), decoded.RootMask)
}

func TestHexPatriciaHashedV3WritesFoldedMasksToParentEdges(t *testing.T) {
	t.Parallel()

	cfg := DefaultTrieConfig()
	cfg.DeferBranchUpdates = false
	cfg.EdgeRecords = true
	ms := NewMockState(t)
	hph := NewHexPatriciaHashed(length.Addr, ms, cfg)

	plainKeys, updates := fixtureBaseAccounts().Build()
	require.NoError(t, ms.applyPlainUpdates(plainKeys, updates))
	upds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer upds.Close()
	require.NotEmpty(t, updates)

	_, err := hph.Process(context.Background(), upds, "", nil, WarmupConfig{})
	require.NoError(t, err)
	require.NotEmpty(t, ms.cm)

	var branchRecords, parentMaskRecords int
	for key, record := range ms.cm {
		v3Key := []byte(key)
		require.True(t, nibbles.IsChildKeyV3(v3Key), "commitment key %x must be a v3 child key", v3Key)
		if BranchData(record).IsTombstone() {
			continue
		}
		var decoded cell
		mask, err := DecodeRecordInto(record, &decoded)
		require.NoError(t, err, "record %x", v3Key)
		if decoded.accountAddrLen == 0 && decoded.storageAddrLen == 0 {
			branchRecords++
			if mask != 0 {
				parentMaskRecords++
			}
		}
	}
	require.Positive(t, branchRecords)
	require.Equal(t, branchRecords, parentMaskRecords, "every persisted branch child must carry its child mask")

	stateBlob, err := hph.EncodeCurrentState(nil)
	require.NoError(t, err)
	var stateValue state
	require.NoError(t, stateValue.Decode(stateBlob))
	require.Equal(t, hph.rootMask, stateValue.RootMask)
	require.NotZero(t, stateValue.RootMask)
	for key := range ms.cm {
		require.NotEqual(t, nibbles.EncodeKeyV3(nil), []byte(key), "the root node key must not be persisted")
	}
}

// A multi-slot account carries its storage branch mask in the fused record, and a single-slot
// account carries a zero mask. The two cases are pinned together because a zero mask is what
// marks a singleton slot, so a producer that never runs looks exactly like an account with one slot.
func TestHexPatriciaHashedV3WritesStorageMaskToAccountEdge(t *testing.T) {
	t.Parallel()

	const branched = "8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e"
	const singleton = "ba7a3b7b095d3370c022ca655c790f0c0ead66f5"

	cfg := DefaultTrieConfig()
	cfg.DeferBranchUpdates = false
	cfg.EdgeRecords = true
	ms := NewMockState(t)
	hph := NewHexPatriciaHashed(length.Addr, ms, cfg)

	plainKeys, updates := NewUpdateBuilder().
		Balance(branched, 1233).
		Storage(branched, "24f3a02dc65eda502dbf75919e795458413d3c45b38bb35b51235432707900ed", "0401").
		Storage(branched, "0fa41642c48ecf8f2059c275353ce4fee173b3a8ce5480f040c4d2901603d14e", "0402").
		Storage(branched, "de3fea338c95ca16954e80eb603cd81a261ed6e2b10a03d0c86cf953fe8769a4", "0403").
		Balance(singleton, 5*1e17).
		Storage(singleton, "9f49fdd48601f00df18ebc29b1264e27d09cf7cbd514fe8af173e534db038033", "0501").
		Build()
	require.NoError(t, ms.applyPlainUpdates(plainKeys, updates))
	upds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer upds.Close()

	_, err := hph.Process(context.Background(), upds, "", nil, WarmupConfig{})
	require.NoError(t, err)

	// storageMask has to equal the storage children actually persisted under the account.
	wantMask := func(addr []byte) uint16 {
		nodeKey := nibbles.EncodeKeyV3(KeyToHexNibbleHash(addr))
		var mask uint16
		for key := range ms.cm {
			if len(key) != len(nodeKey)+1 || key[:len(nodeKey)] != string(nodeKey) {
				continue
			}
			if last := key[len(nodeKey)]; last >= 0x80 && last <= 0x8f {
				mask |= 1 << (last & 0x0f)
			}
		}
		return mask
	}

	seen := make(map[string]uint16)
	for key, record := range ms.cm {
		if BranchData(record).IsTombstone() {
			continue
		}
		var decoded cell
		if _, err := DecodeRecordInto(record, &decoded); err != nil {
			t.Fatalf("record %x: %v", key, err)
		}
		if decoded.accountAddrLen != length.Addr {
			continue
		}
		addr := decoded.accountAddr[:decoded.accountAddrLen]
		seen[common.Bytes2Hex(addr)] = decoded.storageMask
		require.Equalf(t, wantMask(addr), decoded.storageMask,
			"account %x storage mask does not match its persisted storage children", addr)
	}

	require.Contains(t, seen, branched)
	require.Contains(t, seen, singleton)
	require.Equal(t, 3, bits.OnesCount16(seen[branched]), "the branched account must carry a three-child storage mask")
	require.Zero(t, seen[singleton], "a single-slot account must keep the zero mask that marks a singleton")
}

func storageMaskOfAccount(t *testing.T, ms *MockState, addrHex string) uint16 {
	t.Helper()

	addr := common.Hex2Bytes(addrHex)
	for key, record := range ms.cm {
		if BranchData(record).IsTombstone() {
			continue
		}
		var decoded cell
		if _, err := DecodeRecordInto(record, &decoded); err != nil {
			t.Fatalf("record %x: %v", key, err)
		}
		if decoded.accountAddrLen == length.Addr && bytes.Equal(decoded.accountAddr[:length.Addr], addr) {
			return decoded.storageMask
		}
	}
	t.Fatalf("no account record for %s", addrHex)
	return 0
}

// A later batch that touches only the account never unfolds its storage subtree, so the fused
// record is rewritten from a grid cell the unfold populated. A mask that is not restored there
// comes back as zero, which is the value reserved for a singleton slot.
func TestHexPatriciaHashedV3KeepsStorageMaskWhenOnlyTheAccountChanges(t *testing.T) {
	t.Parallel()

	const branched = "8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e"
	const other = "ba7a3b7b095d3370c022ca655c790f0c0ead66f5"

	cfg := DefaultTrieConfig()
	cfg.DeferBranchUpdates = false
	cfg.EdgeRecords = true
	ms := NewMockState(t)
	ctx := &edgeRecordContext{MockState: ms}

	keys1, updates1 := NewUpdateBuilder().
		Balance(branched, 1233).
		Storage(branched, "24f3a02dc65eda502dbf75919e795458413d3c45b38bb35b51235432707900ed", "0401").
		Storage(branched, "0fa41642c48ecf8f2059c275353ce4fee173b3a8ce5480f040c4d2901603d14e", "0402").
		Storage(branched, "de3fea338c95ca16954e80eb603cd81a261ed6e2b10a03d0c86cf953fe8769a4", "0403").
		Balance(other, 5*1e17).
		Build()

	hph := NewHexPatriciaHashed(length.Addr, ctx, cfg)
	defer hph.Release()
	require.NoError(t, ms.applyPlainUpdates(keys1, updates1))
	upds1 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, keys1, updates1)
	_, err := hph.Process(context.Background(), upds1, "", nil, WarmupConfig{})
	upds1.Close()
	require.NoError(t, err)

	before := storageMaskOfAccount(t, ms, branched)
	require.Equal(t, 3, bits.OnesCount16(before), "the account starts out with a three-slot storage mask")

	blob, err := hph.EncodeCurrentState(nil)
	require.NoError(t, err)

	keys2, updates2 := NewUpdateBuilder().Balance(branched, 4321).Build()
	restored := NewHexPatriciaHashed(length.Addr, ctx, cfg)
	defer restored.Release()
	require.NoError(t, restored.SetState(blob))
	require.NoError(t, ms.applyPlainUpdates(keys2, updates2))
	upds2 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, keys2, updates2)
	_, err = restored.Process(context.Background(), upds2, "", nil, WarmupConfig{})
	upds2.Close()
	require.NoError(t, err)

	require.Equal(t, before, storageMaskOfAccount(t, ms, branched),
		"an account-only update must not replace the storage mask with the singleton marker")
}

// A lone slot is hoisted into the account cell and has no storage root of its own, so the record
// carries the slot key. Losing it strands the storage subtree: nothing else can address it.
func TestHexPatriciaHashedV3RecordsHoistedSlotOnAccountEdge(t *testing.T) {
	t.Parallel()

	const solo = "ba7a3b7b095d3370c022ca655c790f0c0ead66f5"
	const other = "8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e"
	const slot = "9f49fdd48601f00df18ebc29b1264e27d09cf7cbd514fe8af173e534db038033"

	cfg := DefaultTrieConfig()
	cfg.DeferBranchUpdates = false
	cfg.EdgeRecords = true
	ms := NewMockState(t)
	ctx := &edgeRecordContext{MockState: ms}

	plainKeys, updates := NewUpdateBuilder().
		Balance(other, 1233).
		Balance(solo, 5*1e17).
		Storage(solo, slot, "0501").
		Build()
	require.NoError(t, ms.applyPlainUpdates(plainKeys, updates))
	upds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer upds.Close()

	hph := NewHexPatriciaHashed(length.Addr, ctx, cfg)
	defer hph.Release()
	_, err := hph.Process(context.Background(), upds, "", nil, WarmupConfig{})
	require.NoError(t, err)

	addr := common.Hex2Bytes(solo)
	want := append(common.Hex2Bytes(solo), common.Hex2Bytes(slot)...)
	var found bool
	for key, record := range ms.cm {
		if BranchData(record).IsTombstone() {
			continue
		}
		var decoded cell
		_, err := DecodeRecordInto(record, &decoded)
		require.NoErrorf(t, err, "record %x", key)
		if decoded.accountAddrLen != length.Addr || !bytes.Equal(decoded.accountAddr[:length.Addr], addr) {
			continue
		}
		found = true
		require.Equal(t, int16(length.Addr+length.Hash), decoded.storageAddrLen,
			"the record must carry the hoisted slot key")
		require.Equal(t, want, decoded.storageAddr[:decoded.storageAddrLen])
	}
	require.True(t, found, "no record for the single-slot account")
}

// The two fused shapes have to survive a round trip: a real storage branch keeps its root and mask,
// a hoisted slot keeps its key. Neither may be decoded as the other.
func TestEncodeLeafChildFusedShapesRoundTrip(t *testing.T) {
	t.Parallel()

	t.Run("hoisted slot", func(t *testing.T) {
		var c cellEncodeData
		c.accountAddrLen = length.Addr
		copy(c.accountAddr[:], bytes.Repeat([]byte{0x31}, length.Addr))
		c.storageAddrLen = length.Addr + length.Hash
		copy(c.storageAddr[:], append(bytes.Repeat([]byte{0x31}, length.Addr), bytes.Repeat([]byte{0x72}, length.Hash)...))
		c.stateHashLen = length.Hash
		copy(c.stateHash[:], bytes.Repeat([]byte{0xab}, length.Hash))

		var decoded cell
		mask, err := DecodeRecordInto(EncodeLeafChild(&c), &decoded)
		require.NoError(t, err)
		require.Zero(t, mask)
		require.Equal(t, int16(length.Addr+length.Hash), decoded.storageAddrLen)
		require.Equal(t, c.storageAddr[:c.storageAddrLen], decoded.storageAddr[:decoded.storageAddrLen])
		require.Equal(t, c.accountAddr[:length.Addr], decoded.accountAddr[:decoded.accountAddrLen])
		require.Zero(t, decoded.hashLen, "a hoisted slot has no storage root to record")
	})

	t.Run("storage branch", func(t *testing.T) {
		var c cellEncodeData
		c.accountAddrLen = length.Addr
		copy(c.accountAddr[:], bytes.Repeat([]byte{0x44}, length.Addr))
		c.hashLen = length.Hash
		copy(c.hash[:], bytes.Repeat([]byte{0xcd}, length.Hash))
		c.storageMask = 0x4208
		c.stateHashLen = length.Hash
		copy(c.stateHash[:], bytes.Repeat([]byte{0xef}, length.Hash))

		var decoded cell
		mask, err := DecodeRecordInto(EncodeLeafChild(&c), &decoded)
		require.NoError(t, err)
		require.Equal(t, uint16(0x4208), mask)
		require.Equal(t, int16(length.Hash), decoded.hashLen)
		require.Equal(t, c.hash[:length.Hash], decoded.hash[:decoded.hashLen])
		require.Zero(t, decoded.storageAddrLen, "a storage branch has no slot key to record")
	})
}
