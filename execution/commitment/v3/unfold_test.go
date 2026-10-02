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

package v3

import (
	"bytes"
	"testing"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/stretchr/testify/require"
)

func TestUnfold(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"E31-E39/UnfoldReadsOneExactRecordWithoutStateReads", func(t *testing.T) {
			ctx := newMockContext()
			path := []byte{1, 2, 3}
			hash := bytes.Repeat([]byte{0xab}, 32)
			ctx.branches[string(AccountNodeKey(path, nil))] = commitmenttest.Records([]commitmenttest.RecordSpec{{
				Flags: hdrHasChildExt, ChildMask: 1<<2 | 1<<7, LeafMask: 1 << 7, ExtensionMask: 1 << 2,
				Hashes: [16][]byte{2: hash}, Extensions: [16][]byte{2: {3, 0x45, 0x60}},
				Leaves: [16]commitmenttest.Leaf{7: {Suffix: bytes.Repeat([]byte{0x99}, 30), Value: []byte{0x42}}},
			}})[0].Data

			got, err := unfold(ctx, path, planeAccount, nil)
			require.NoError(t, err)
			require.Equal(t, [][]byte{AccountNodeKey(path, nil)}, ctx.branchCalls)
			require.Equal(t, 0, ctx.accountCalls)
			require.Equal(t, 0, ctx.storageCalls)
			require.Equal(t, path, got.path)
			require.Equal(t, hash, got.childHashAt(2))
			require.Equal(t, []byte{4, 5, 6}, got.childExtAt(2))
			_, value := got.leafAt(7)
			require.Equal(t, []byte{0x42}, value)
		}},
		{"E31/UnfoldStorageUsesAddressQualifiedKey", func(t *testing.T) {
			ctx := newMockContext()
			path := []byte{4, 5}
			var addrHash [32]byte
			addrHash[0] = 0x9a
			hash := bytes.Repeat([]byte{0xcd}, 32)
			ctx.branches[string(StorageNodeKey(addrHash, path, nil))] = commitmenttest.Records([]commitmenttest.RecordSpec{{ChildMask: 1 << 6, Hashes: [16][]byte{6: hash}}})[0].Data

			got, err := unfold(ctx, path, planeStorage, addrHash[:])
			require.NoError(t, err)
			require.Equal(t, [][]byte{StorageNodeKey(addrHash, path, nil)}, ctx.branchCalls)
			require.Equal(t, path, got.path)
			require.Equal(t, hash, got.childHashAt(6))
		}},
		{"E31/UnfoldRootForms", func(t *testing.T) {
			t.Run("leaf root", func(t *testing.T) {
				ctx := newMockContext()
				fullPath := bytes.Repeat([]byte{3}, 64)
				ctx.branches[string(AccountNodeKey(nil, nil))] = commitmenttest.Records([]commitmenttest.RecordSpec{{Flags: hdrIsLeafRoot, Root: append(bytes.Repeat([]byte{0x33}, 32), 2, 1, 2)}})[0].Data

				got, err := unfold(ctx, nil, planeAccount, nil)
				require.NoError(t, err)
				suffix, value := got.leafAt(3)
				require.Equal(t, []byte{0x01, 0x02}, value)
				require.Equal(t, packPath(fullPath[1:], nil), suffix)
			})

			t.Run("extension root", func(t *testing.T) {
				ctx := newMockContext()
				ext := []byte{1, 2, 3}
				hash := bytes.Repeat([]byte{0x55}, 32)
				ctx.branches[string(AccountNodeKey(nil, nil))] = commitmenttest.Records([]commitmenttest.RecordSpec{{Flags: hdrHasSelfExt, SelfExtension: []byte{3, 0x12, 0x30}, ChildMask: 1 << 4, Hashes: [16][]byte{4: hash}}})[0].Data

				got, err := unfold(ctx, nil, planeAccount, nil)
				require.NoError(t, err)
				require.Equal(t, ext, got.path)
				require.Equal(t, hash, got.childHashAt(4))
			})
		}},
		{"E32/UnfoldMissingAndTombstone", func(t *testing.T) {
			ctx := newMockContext()
			got, err := unfold(ctx, nil, planeAccount, nil)
			require.NoError(t, err)
			require.Nil(t, got)

			ctx.branches[string(AccountNodeKey(nil, nil))] = []byte{}
			got, err = unfold(ctx, nil, planeAccount, nil)
			require.NoError(t, err)
			require.NotNil(t, got)
			require.Empty(t, got.childMask)
		}},
		{"E33/UnfoldKeepsOwnedBranchBytes", func(t *testing.T) {
			m := newMockContext()
			address := [32]byte{7}
			_, err := runStorageTask(m, storageTask{addrHash: address, entries: []storageEntry{
				entryOf(append([]byte{1}, bytes.Repeat([]byte{2}, 63)...), phaseAStorageUpdate([]byte{1})),
				entryOf(append([]byte{3}, bytes.Repeat([]byte{4}, 63)...), phaseAStorageUpdate([]byte{2})),
			}})
			require.NoError(t, err)
			stored := m.branches[string(StorageNodeKey(address, nil, nil))]
			require.NotEmpty(t, stored)

			for _, ctx := range []commitment.PatriciaContext{ownedBranchContext{m}, &meteredContext{ownedBranchContext{m}, new(meterCounts)}} {
				n, err := unfold(ctx, nil, planeStorage, address[:])
				require.NoError(t, err)
				require.Same(t, &stored[0], &n.raw[0])
			}

			n, err := unfold(m, nil, planeStorage, address[:])
			require.NoError(t, err)
			require.NotSame(t, &m.branchBuf[0], &n.raw[0])
		}},
	} {
		t.Run(tc.name, tc.run)
	}
}

type ownedBranchContext struct{ *mockContext }

func (c ownedBranchContext) BranchOwned(key []byte) ([]byte, kv.Step, error) {
	return c.branches[string(key)], 0, nil
}
