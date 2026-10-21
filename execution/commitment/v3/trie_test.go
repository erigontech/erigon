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
	"context"
	"fmt"
	"math/bits"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/erigontech/erigon/internal/commitmenttest/runner"
)

func TestNodeMutations(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"E9/NodeMutationsKeepMasksConsistent", func(t *testing.T) {
			n := fork([]byte{1, 2})
			require.Equal(t, []byte{1, 2}, n.path)
			require.Zero(t, n.childMask)
			require.Zero(t, n.leafMask)

			n.setLeaf(3, []byte{4, 5}, []byte{6})
			assertNodeSlot(t, n, true, true)
			suffix, value := n.leafAt(3)
			require.Equal(t, []byte{4, 5}, suffix)
			require.Equal(t, []byte{6}, value)

			child := fork([]byte{1, 2, 3})
			n.setChild(3, child)
			assertNodeSlot(t, n, true, false)
			require.Same(t, child, n.child(3))
			require.Nil(t, n.childHashAt(3))
			require.Nil(t, n.childExtAt(3))
			suffix, value = n.leafAt(3)
			require.Nil(t, suffix)
			require.Nil(t, value)

			hash := bytes.Repeat([]byte{0xab}, 32)
			ext := []byte{7, 8}
			n.setStoredChild(3, hash, ext)
			assertNodeSlot(t, n, true, false)
			require.Nil(t, n.child(3))
			require.Equal(t, hash, n.childHashAt(3))
			require.Equal(t, ext, n.childExtAt(3))

			n.setLeaf(3, []byte{9}, []byte{10, 11})
			assertNodeSlot(t, n, true, true)
			require.Nil(t, n.child(3))
			require.Nil(t, n.childHashAt(3))
			require.Nil(t, n.childExtAt(3))
			suffix, value = n.leafAt(3)
			require.Equal(t, []byte{9}, suffix)
			require.Equal(t, []byte{10, 11}, value)

			n.clear(3)
			assertNodeSlot(t, n, false, false)
			require.Nil(t, n.child(3))
			require.Nil(t, n.childHashAt(3))
			require.Nil(t, n.childExtAt(3))
			suffix, value = n.leafAt(3)
			require.Nil(t, suffix)
			require.Nil(t, value)
		}},
		{"E9/NodeMutationsCoverEveryNibble", func(t *testing.T) {
			n := fork(nil)
			for nib := range 16 {
				n.setLeaf(nib, []byte{byte(nib)}, []byte{byte(nib + 1)})
			}
			require.Equal(t, uint16(0xffff), n.childMask)
			require.Equal(t, uint16(0xffff), n.leafMask)

			for nib := range 16 {
				n.setChild(nib, fork([]byte{byte(nib)}))
			}
			require.Equal(t, uint16(0xffff), n.childMask)
			require.Zero(t, n.leafMask)

			for nib := range 16 {
				n.clear(nib)
			}
			require.Zero(t, n.childMask)
			require.Zero(t, n.leafMask)
		}},
		{"E10/NodeStoredChildCopiesReferences", func(t *testing.T) {
			n := fork(nil)
			hash := bytes.Repeat([]byte{1}, 32)
			ext := []byte{2, 3, 4}
			n.setStoredChild(5, hash, ext)
			hash[0] = 9
			ext[0] = 8
			require.Equal(t, byte(1), n.childHashAt(5)[0])
			require.Equal(t, byte(2), n.childExtAt(5)[0])
		}},
		{"E10/ForkReturnsSharedNodePointer", func(t *testing.T) {
			parent := fork([]byte{1})
			forked := parent
			forked.setLeaf(2, []byte{3}, []byte{4})

			require.Same(t, parent, forked)
			require.Equal(t, uint16(1<<2), parent.childMask)
			require.Equal(t, uint16(1<<2), parent.leafMask)
		}},
		{"E11/NodeRejectsInvalidReferences", func(t *testing.T) {
			n := fork(nil)
			require.Panics(t, func() { n.setStoredChild(0, nil, nil) })
			require.Panics(t, func() { n.setStoredChild(0, make([]byte, 31), nil) })
		}},
	} {
		t.Run(tc.name, tc.run)
	}
}

func assertNodeSlot(t *testing.T, n *node, child, leaf bool) {
	t.Helper()
	bit := uint16(1) << 3
	if child {
		require.NotZero(t, n.childMask&bit)
	} else {
		require.Zero(t, n.childMask&bit)
	}
	if leaf {
		require.NotZero(t, n.leafMask&bit)
	} else {
		require.Zero(t, n.leafMask&bit)
	}
	require.Zero(t, n.leafMask&^n.childMask)
}

func TestTrieAPI(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"E45/InitializeTrieAndUpdatesV3", func(t *testing.T) {
			cfg := commitment.DefaultTrieConfig()
			cfg.Variant = commitment.VariantCommitmentV3
			trie, updates := commitment.InitializeTrieAndUpdates(commitment.ModeCollect, t.TempDir(), cfg)
			t.Cleanup(trie.Release)
			t.Cleanup(updates.Close)

			require.IsType(t, &Trie{}, trie)
			require.Equal(t, commitment.ModeCollect, updates.Mode())
			require.Equal(t, commitment.VariantCommitmentV3, trie.Variant())
		}},
		{"E39/TrieProcessDoesNotReadState", func(t *testing.T) {
			trie, updates := NewTrie(t.TempDir(), commitment.TrieConfig{})
			t.Cleanup(trie.Release)
			t.Cleanup(updates.Close)
			ctx := newMockContext()
			trie.ResetContext(ctx)

			address := bytes.Repeat([]byte{0x11}, 20)
			slot := bytes.Repeat([]byte{0x22}, 32)
			account := fullAccountUpdate(3, 5, common.HexToHash("0x1234"))
			updates.TouchPlainKeyDirect(string(address), &account)
			storage := phaseAStorageUpdate([]byte{0xaa})
			updates.TouchPlainKeyDirect(string(append(append([]byte(nil), address...), slot...)), storage)

			_, err := trie.Process(context.Background(), updates, "", nil, commitment.WarmupConfig{})
			require.NoError(t, err)
			require.Zero(t, ctx.accountCalls)
			require.Zero(t, ctx.storageCalls)
			root, err := trie.RootHash()
			require.NoError(t, err)
			require.NotEqual(t, common.Hash{}, common.BytesToHash(root))
		}},
		{"E46/TrieDeferredUpdatesWaitForApply", func(t *testing.T) {
			trie := &Trie{}
			t.Cleanup(trie.Release)
			ctx := newMockContext()
			trie.ResetContext(ctx)
			trie.SetDeferCommitmentUpdates(true)

			updates := commitment.NewUpdates(commitment.ModeCollect, t.TempDir(), commitment.KeyToHexNibbleHash)
			t.Cleanup(updates.Close)
			address := bytes.Repeat([]byte{0x11}, 20)
			account := fullAccountUpdate(3, 5, common.HexToHash("0x1234"))
			updates.TouchPlainKeyDirect(string(address), &account)

			_, err := trie.Process(context.Background(), updates, "", nil, commitment.WarmupConfig{})
			require.NoError(t, err)
			require.Zero(t, ctx.putCalls)

			deltas := trie.TakeDeferredDeltas()
			require.NotNil(t, deltas)
			require.NoError(t, applyDeltas(deltas, ctx.PutBranch))
			require.NotZero(t, ctx.putCalls)
		}},
		{"E45/TrieProcessRejectsWrongUpdateMode", func(t *testing.T) {
			trie, unused := NewTrie(t.TempDir(), commitment.TrieConfig{})
			t.Cleanup(trie.Release)
			t.Cleanup(unused.Close)
			trie.ResetContext(newMockContext())
			updates := commitment.NewUpdates(commitment.ModeDirect, t.TempDir(), commitment.KeyToHexNibbleHash)
			t.Cleanup(updates.Close)

			_, err := trie.Process(context.Background(), updates, "", nil, commitment.WarmupConfig{})
			require.EqualError(t, err, "commitment v3: Process requires ModeCollect updates")
		}},
	} {
		t.Run(tc.name, tc.run)
	}
}

func TestTrieMutations(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"E12/InsertLeafAtEveryDepthInBothPlanes", func(t *testing.T) {
			eachPlane(t, func(t *testing.T, plane byte) {
				for depth := 0; depth <= 62; depth++ {
					prefix := repeatedPath(depth, 3)
					incoming := appendPath(prefix, 4, bytes.Repeat([]byte{6}, 63-depth))
					n := newNode(plane, prefix...)
					n.addLeaf(4, 5, 0x11)

					err := insert(n, incoming, []byte{0x22})
					require.NoError(t, err, "depth %d", depth)
					bit := uint16(1) << 4
					require.NotZero(t, n.childMask&bit, "depth %d", depth)
					require.Zero(t, n.leafMask&bit, "depth %d", depth)
					branch := n.child(4)
					require.NotNil(t, branch, "depth %d", depth)
					require.Equal(t, prefix, branch.path[:depth])
					require.Equal(t, byte(4), branch.path[depth])
					require.Equal(t, uint16(1<<5|1<<6), branch.childMask, "depth %d", depth)
					require.Equal(t, uint16(1<<5|1<<6), branch.leafMask, "depth %d", depth)
				}
			})
		}},
		{"E12/InsertCreatesExpectedExtensionLengths", func(t *testing.T) {
			for _, extensionLen := range []int{0, 1, 4} {
				t.Run(fmt.Sprintf("extension-%d", extensionLen), func(t *testing.T) {
					prefix := repeatedPath(7, 2)
					shared := repeatedPath(extensionLen, 8)
					oldTail := append(append([]byte(nil), shared...), bytes.Repeat([]byte{1}, 63-7-extensionLen)...)
					newTail := append(append([]byte(nil), shared...), byte(9))
					newTail = append(newTail, bytes.Repeat([]byte{3}, 63-7-extensionLen-1)...)
					oldPath := appendPath(prefix, 6, oldTail)
					newPath := appendPath(prefix, 6, newTail)
					n := fork(prefix)
					n.setLeaf(6, packPath(oldPath[8:], nil), []byte{1})

					require.NoError(t, insert(n, newPath, []byte{2}))
					branch := n.child(6)
					require.NotNil(t, branch)
					require.Equal(t, append(append([]byte(nil), prefix...), byte(6)), branch.path[:8])
					require.Len(t, branch.path, 8+extensionLen)
					expectedPath := append(append(append([]byte(nil), prefix...), byte(6)), shared...)
					require.Equal(t, expectedPath, branch.path)
				})
			}
		}},
		{"E12/InsertRecomputesPushedLeafSuffixFromFullPath", func(t *testing.T) {
			prefix := repeatedPath(5, 1)
			oldPath := appendPath(prefix, 2, bytes.Repeat([]byte{3}, 58))
			newPath := appendPath(prefix, 2, append([]byte{3, 4}, bytes.Repeat([]byte{5}, 56)...))
			n := fork(prefix)
			n.setLeaf(2, packPath(oldPath[6:], nil), []byte{0xaa})

			require.NoError(t, insert(n, newPath, []byte{0xbb}))
			branch := n.child(2)
			require.NotNil(t, branch)
			oldSuffix, oldValue := branch.leafAt(int(oldPath[len(branch.path)]))
			newSuffix, newValue := branch.leafAt(int(newPath[len(branch.path)]))
			require.Equal(t, packPath(oldPath[len(branch.path)+1:], nil), oldSuffix)
			require.Equal(t, packPath(newPath[len(branch.path)+1:], nil), newSuffix)
			require.Equal(t, []byte{0xaa}, oldValue)
			require.Equal(t, []byte{0xbb}, newValue)
		}},
		{"E13/InsertRejectsStoredDescendant", func(t *testing.T) {
			n := fork(nil)
			n.setStoredChild(1, bytes.Repeat([]byte{0xab}, 32), []byte{2, 3})
			storedPath := append([]byte{1, 2, 3}, bytes.Repeat([]byte{4}, 61)...)
			require.ErrorIs(t, insert(n, storedPath, []byte{2}), ErrInsertStoredChild)
		}},
		{"E9-E14-E16/RemoveClearsLeafAndMasks", func(t *testing.T) {
			n := fork([]byte{2})
			path := n.addLeaf(3, 4, 1)
			other := n.addLeaf(5, 6, 2)

			require.NoError(t, removeErr(n, path))
			require.Zero(t, n.childMask&(1<<3))
			require.Zero(t, n.leafMask&(1<<3))
			require.NotZero(t, n.childMask&(1<<5))
			require.NotZero(t, n.leafMask&(1<<5))

			require.ErrorIs(t, removeErr(n, path), ErrRemoveNotFound)
			require.NoError(t, removeErr(n, other))
			require.Zero(t, n.childMask)
			require.Zero(t, n.leafMask)
		}},
		{"E14/RemovePromotesSoleLeafSurvivor", func(t *testing.T) {
			parent := fork([]byte{4})
			child := fork([]byte{4, 0, 1, 2})
			removed := child.addLeaf(3, 5, 1)
			survivor := child.addLeaf(6, 7, 2)
			parent.setChild(0, child)

			require.NoError(t, removeErr(parent, removed))
			require.NotZero(t, parent.leafMask&(1<<0))
			require.Nil(t, parent.child(0))
			suffix, value := parent.leafAt(0)
			require.Equal(t, packPath(survivor[len(parent.path)+1:], nil), suffix)
			require.Equal(t, []byte{2}, value)
			full := append(append([]byte(nil), parent.path...), byte(0))
			full = append(full, unpackPath(suffix, 62, nil)...)
			require.Equal(t, survivor, full)
		}},
		{"E15/RemoveCollapseKeepsStoredHash", func(t *testing.T) {
			for _, tt := range []struct {
				name string
				fill byte
				ext  []byte
			}{
				{"D6 pre-extension", 0xa5, []byte{1, 2}},
				{"sole branch without extension", 0x31, nil},
			} {
				t.Run(tt.name, func(t *testing.T) {
					parent := fork([]byte{7})
					hash := bytes.Repeat([]byte{tt.fill}, 32)
					parent.setStoredChild(0, hash, tt.ext)
					removed := parent.addLeaf(1, 3, 9)

					require.NoError(t, removeErr(parent, removed))
					require.Zero(t, parent.leafMask)
					require.Equal(t, hash, parent.childHashAt(0))
					require.Equal(t, tt.ext, parent.childExtAt(0))
					if tt.ext != nil {
						require.Equal(t, uint16(1), parent.childMask)
						path := append(append([]byte(nil), parent.path...), byte(0))
						path = append(path, parent.childExtAt(0)...)
						require.Equal(t, []byte{7, 0, 1, 2}, path)
					}
				})
			}
		}},
		{"E15/RemoveConcatenatesNestedExtensions", func(t *testing.T) {
			parent := fork([]byte{7})
			child := fork([]byte{7, 0, 1, 2})
			hash := bytes.Repeat([]byte{0x4c}, 32)
			child.setStoredChild(3, hash, []byte{4, 5})
			removed := child.addLeaf(6, 8, 9)
			parent.setChild(0, child)

			require.NoError(t, removeErr(parent, removed))
			require.Nil(t, parent.child(0))
			require.Equal(t, hash, parent.childHashAt(0))
			require.Equal(t, []byte{1, 2, 3, 4, 5}, parent.childExtAt(0))
			path := append(append([]byte(nil), parent.path...), byte(0))
			path = append(path, parent.childExtAt(0)...)
			require.Equal(t, []byte{7, 0, 1, 2, 3, 4, 5}, path)
		}},
		{"E16/RemoveTreatsDivergedPathsAsNotFound", func(t *testing.T) {
			ext := fork([]byte{1, 2, 3})
			ext.addLeaf(4, 5, 1)
			diverged := appendPath([]byte{1, 2, 9}, 4, bytes.Repeat([]byte{5}, 60))
			require.ErrorIs(t, removeErr(ext, diverged), ErrRemoveNotFound)

			root := fork(nil)
			branch := fork([]byte{7, 8, 8})
			branch.addLeaf(1, 2, 3)
			root.setChild(7, branch)
			pastBranch := appendPath([]byte{7, 8, 9}, 1, bytes.Repeat([]byte{2}, 60))
			require.ErrorIs(t, removeErr(root, pastBranch), ErrRemoveNotFound)
		}},
		{"E13-E16/RemoveBelowDivergedStoredChildIsNotFound", func(t *testing.T) {
			n := fork(nil)
			n.setStoredChild(1, bytes.Repeat([]byte{3}, 32), []byte{4, 5})

			diverged := appendPath(nil, 1, append([]byte{9, 9}, bytes.Repeat([]byte{2}, 61)...))
			require.ErrorIs(t, removeErr(n, diverged), ErrRemoveNotFound)

			under := appendPath(nil, 1, append([]byte{4, 5}, bytes.Repeat([]byte{2}, 61)...))
			require.ErrorIs(t, removeErr(n, under), ErrRemoveStoredChild)
		}},
		{"E13/RemoveRejectsInvalidAndStoredPaths", func(t *testing.T) {
			n := fork(nil)
			path := appendPath(nil, 1, bytes.Repeat([]byte{2}, 63))
			n.setStoredChild(1, bytes.Repeat([]byte{3}, 32), []byte{2})

			require.ErrorIs(t, removeErr(n, nil), ErrRemovePath)
			require.ErrorIs(t, removeErr(n, path), ErrRemoveStoredChild)
		}},
		{"E17/RootRecordFormsAcrossPlanes", func(t *testing.T) {
			eachPlane(t, func(t *testing.T, plane byte) {
				n := newNode(plane)
				n.addLeaf(3, 4, 1)
				require.Equal(t, byte(hdrIsLeafRoot), encodeRecord(n, 0, nil)[0])

				second := appendPath(nil, 9, bytes.Repeat([]byte{5}, 63))
				require.NoError(t, insertRoot(n, second, []byte{2}))
				require.Zero(t, n.path)
				require.Equal(t, uint16(1<<3|1<<9), n.childMask)
				require.Equal(t, byte(recordFormat), encodeRecord(n, 0, nil)[0])

				require.NoError(t, removeRoot(n, second))
				require.Zero(t, n.path)
				require.Equal(t, byte(hdrIsLeafRoot), encodeRecord(n, 0, nil)[0])
			})
		}},
		{"E18/RootExtensionInsertDivergenceKeepsChildBody", func(t *testing.T) {
			eachPlane(t, func(t *testing.T, plane byte) {
				hash := bytes.Repeat([]byte{0xa5}, 32)
				childRecordPath := []byte{1, 2, 3}
				n := newNode(plane, childRecordPath...)
				n.setStoredChild(int(childRecordPath[0]), hash, nil)
				require.NoError(t, Validate(encodeRecord(n, 0, nil), 0))
				incoming := append([]byte{1, 9}, bytes.Repeat([]byte{7}, 62)...)

				require.NoError(t, insertRoot(n, incoming, []byte{0x42}))
				require.Equal(t, []byte{1}, n.path)
				require.Equal(t, 1, bits.OnesCount16(n.childMask))
				branch := n.child(bits.TrailingZeros16(n.childMask))
				require.NotNil(t, branch)
				require.Equal(t, []byte{1}, branch.path)
				require.Equal(t, hash, branch.childHashAt(2))
				require.Equal(t, []byte{3}, branch.childExtAt(2))
				_, value := branch.leafAt(9)
				require.Equal(t, []byte{0x42}, value)
				rebuilt := append(append([]byte(nil), branch.path...), 2)
				require.Equal(t, childRecordPath, append(rebuilt, branch.childExtAt(2)...))
			})
		}},
		{"E15-E17/RootCollapseRewritesOnlyTheRoot", func(t *testing.T) {
			eachPlane(t, func(t *testing.T, plane byte) {
				hash := bytes.Repeat([]byte{0x31}, 32)
				n := newNode(plane)
				n.setStoredChild(3, hash, []byte{4, 5})
				removed := n.addLeaf(9, 6, 0x44)

				require.NoError(t, removeRoot(n, removed))
				require.Equal(t, []byte{3, 4, 5}, n.path)
				require.Equal(t, hash, n.childHashAt(3))
				require.Empty(t, n.childExtAt(3))
				data := encodeRecord(n, 0, nil)
				require.NoError(t, Validate(data, 0))
				require.Equal(t, []byte{3, 4, 5}, unpackPath(Record{data: data, depth: 0}.SelfExt()[1:], 3, nil))
			})
		}},
		{"E17/RootTransitionsRejectMalformedShape", func(t *testing.T) {
			n := fork([]byte{1})
			n.setChild(2, fork([]byte{1, 2}))
			require.ErrorIs(t, insertRoot(n, bytes.Repeat([]byte{2}, 64), []byte{1}), ErrRootShape)

			n = fork(nil)
			n.setChild(1, fork([]byte{2}))
			require.ErrorIs(t, promoteRootExtension(n), ErrRootShape)
		}},
		{"E19/RootCollapseAdoptsInMemoryChild", func(t *testing.T) {
			eachPlane(t, func(t *testing.T, plane byte) {
				n := newNode(plane)
				child := fork([]byte{3})
				child.addLeaf(1, 7, 0xa1)
				child.addLeaf(2, 8, 0xa2)
				n.setChild(3, child)
				removed := n.addLeaf(9, 6, 0x44)

				require.NoError(t, removeRoot(n, removed))
				require.Equal(t, []byte{3}, n.path)
				require.Equal(t, 1, bits.OnesCount16(n.childMask))
				require.Same(t, child, n.child(3))
			})
		}},
		{"E18/RootExtensionKeepsItsChildUnderTheFirstNibble", func(t *testing.T) {
			eachPlane(t, func(t *testing.T, plane byte) {
				a := appendPath(nil, 5, append([]byte{5, 1}, bytes.Repeat([]byte{4}, 61)...))
				b := appendPath(nil, 5, append([]byte{5, 2}, bytes.Repeat([]byte{4}, 61)...))
				c := appendPath(nil, 6, bytes.Repeat([]byte{4}, 63))

				viaLeafRoot := newNode(plane)
				require.NoError(t, insertRoot(viaLeafRoot, a, []byte{1}))
				require.NoError(t, insertRoot(viaLeafRoot, b, []byte{2}))

				viaCollapse := newNode(plane)
				require.NoError(t, insertRoot(viaCollapse, a, []byte{1}))
				require.NoError(t, insertRoot(viaCollapse, c, []byte{3}))
				require.NoError(t, insertRoot(viaCollapse, b, []byte{2}))
				require.NoError(t, removeRoot(viaCollapse, c))

				require.Equal(t, []byte{5, 5}, viaCollapse.path)
				require.Equal(t, viaCollapse.path, viaLeafRoot.path)
				require.Equal(t, viaCollapse.childMask, viaLeafRoot.childMask)
			})
		}},
	} {
		t.Run(tc.name, tc.run)
	}
}

func repeatedPath(length int, nib byte) []byte {
	return bytes.Repeat([]byte{nib}, length)
}

func appendPath(prefix []byte, nib byte, suffix []byte) []byte {
	path := make([]byte, 0, len(prefix)+1+len(suffix))
	path = append(path, prefix...)
	path = append(path, nib)
	return append(path, suffix...)
}

func eachPlane(t *testing.T, run func(*testing.T, byte)) {
	t.Helper()
	for _, plane := range []byte{planeAccount, planeStorage} {
		t.Run(fmtPlane(plane), func(t *testing.T) { run(t, plane) })
	}
}

func fmtPlane(plane byte) string {
	if plane == planeAccount {
		return "account"
	}
	return "storage"
}

func removeErr(n *node, path []byte) error {
	_, err := remove(n, path)
	return err
}

func TestUnfold(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"E31-E39/UnfoldReadsOneExactRecordWithoutStateReads", func(t *testing.T) {
			ctx := newMockContext()
			path := []byte{1, 2, 3}
			hash := bytes.Repeat([]byte{0xab}, 32)
			ctx.branches[string(AccountNodeKey(path, nil))] = commitmenttest.Record(&commitmenttest.RecordSpec{
				Flags: hdrHasChildExt, ChildMask: 1<<2 | 1<<7, LeafMask: 1 << 7, ExtensionMask: 1 << 2,
				Hashes: [16][]byte{2: hash}, Extensions: [16][]byte{2: {3, 0x45, 0x60}},
				Leaves: [16]commitmenttest.Leaf{7: {Suffix: bytes.Repeat([]byte{0x99}, 30), Value: []byte{0x42}}},
			})

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
			ctx.branches[string(StorageNodeKey(addrHash, path, nil))] = commitmenttest.Record(&commitmenttest.RecordSpec{ChildMask: 1 << 6, Hashes: [16][]byte{6: hash}})

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
				ctx.branches[string(AccountNodeKey(nil, nil))] = commitmenttest.Record(&commitmenttest.RecordSpec{Flags: hdrIsLeafRoot, Root: append(bytes.Repeat([]byte{0x33}, 32), 2, 1, 2)})

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
				ctx.branches[string(AccountNodeKey(nil, nil))] = commitmenttest.Record(&commitmenttest.RecordSpec{Flags: hdrHasSelfExt, SelfExtension: []byte{3, 0x12, 0x30}, ChildMask: 1 << 4, Hashes: [16][]byte{4: hash}})

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

func TestFoldParity(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"E29/FoldEmptyTrie", func(t *testing.T) {
			for _, plane := range []byte{planeAccount, planeStorage} {
				n := newNode(plane)
				got, err := fold(n, 0)
				require.NoError(t, err)
				require.Equal(t, empty.RootHash[:], got[:])
			}
		}},
		{"E29/FoldSingleLeafMatchesHexPatriciaHashed", func(t *testing.T) {
			for _, plane := range []byte{planeAccount, planeStorage} {
				key := foldKey(1)
				path := commitment.KeyToHexNibbleHash(key)
				value := foldValue(plane, 1)
				n := newNode(plane)
				require.NoError(t, insert(n, path, value))

				want := hexPatriciaRoot(t, plane, [][]byte{key}, [][]byte{path}, []commitment.Update{foldUpdate(plane, 1)})
				require.Equal(t, want, foldPlaneRoot(t, n))
			}
		}},
		{"E29/seeded", func(t *testing.T) {
			for _, plane := range []byte{planeAccount, planeStorage} {
				for _, tc := range []struct {
					name        string
					count       int
					seed        int64
					checkBranch bool
				}{
					{"each-plane/1", 1, 1 + int64(plane), false},
					{"each-plane/2", 2, 2 + int64(plane), plane == planeStorage},
					{"each-plane/16", 16, 16 + int64(plane), false},
					{"each-plane/1000", 1000, 1000 + int64(plane), false},
					{"mixed-planes/16", 16, 91 + int64(plane), false},
				} {
					t.Run(fmtPlane(plane)+"/"+tc.name, func(t *testing.T) {
						keys, paths := distinctKeysAndPaths(tc.count, tc.seed)
						updates := make([]commitment.Update, tc.count)
						n := newNode(plane)
						for i, path := range paths {
							updates[i] = foldUpdate(plane, i+1)
							require.NoError(t, insert(n, path, foldValue(plane, i+1)))
						}
						want := hexPatriciaRoot(t, plane, keys, paths, updates)
						if tc.checkBranch {
							var refs [16][]byte
							for i, path := range paths {
								key := append(append([]byte(nil), path[1:]...), nibbles.Terminator)
								refs[path[0]] = storageLeafRef(nibbles.HexToCompact(key), foldValue(plane, i+1), nil)
							}
							got, err := fold(n, 0)
							require.NoError(t, err)
							require.Equal(t, branchRef(&refs), got)
						}
						require.Equal(t, want, foldPlaneRoot(t, n))
					})
				}
			}
		}},
		{"E30-E39/FoldUsesStoredChildHashWithoutStateReads", func(t *testing.T) {
			n := newNode(planeStorage)
			stored := bytes.Repeat([]byte{0x31}, 32)
			n.setStoredChild(2, stored, []byte{3, 4})
			path := n.addLeaf(5, 6, 7)

			got, err := fold(n, 0)
			require.NoError(t, err)
			var refs [16][]byte
			wrapped := extensionRef([]byte{3, 4}, stored)
			refs[2] = wrapped[:]
			refs[5] = storageLeafRef(nibbles.HexToCompact(append(append([]byte(nil), path[1:]...), nibbles.Terminator)), []byte{7}, nil)
			want := branchRef(&refs)
			require.Equal(t, want, got)
		}},
		{"E30/FoldRejectsUnknownPlane", func(t *testing.T) {
			n := newNode(0xff)
			n.addLeaf(0, 1, 1)
			_, err := fold(n, 0)
			require.ErrorIs(t, err, errFoldPlane)
		}},
	} {
		t.Run(tc.name, tc.run)
	}
}

var foldStorageAddr = bytes.Repeat([]byte{0x5a}, length.Addr)

func foldPlaneRoot(t *testing.T, n *node) []byte {
	t.Helper()
	got, err := fold(n, 0)
	require.NoError(t, err)
	if n.plane != planeStorage {
		return got[:]
	}
	owner := foldUpdate(planeAccount, 1)
	account := newNode(planeAccount)
	require.NoError(t, insert(account, commitment.KeyToHexNibbleHash(foldStorageAddr), encodeAccountLeaf(&owner, got[:], nil)))
	root, err := fold(account, 0)
	require.NoError(t, err)
	return root[:]
}

func hexPatriciaRoot(t *testing.T, plane byte, keys, paths [][]byte, updates []commitment.Update) []byte {
	t.Helper()
	ctx := runner.NewMemory(runner.ContextSpec{})
	trie := commitment.NewHexPatriciaHashed(length.Addr, ctx, commitment.TrieConfig{DeferBranchUpdates: false})
	defer trie.Release()
	batch := commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), commitment.KeyToHexNibbleHash)
	for i := range paths {
		key := keys[i]
		if plane == planeStorage {
			key = append(bytes.Clone(foldStorageAddr), key...)
		}
		batch.TouchPlainKeyDirect(string(key), &updates[i])
	}
	if plane == planeStorage {
		owner := foldUpdate(planeAccount, 1)
		batch.TouchPlainKeyDirect(string(foldStorageAddr), &owner)
	}
	got, err := trie.Process(context.Background(), batch, "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	return got
}

func foldValue(plane byte, number int) []byte {
	if plane == planeStorage {
		return []byte{byte(number), byte(number >> 8)}
	}
	update := foldUpdate(plane, number)
	return encodeAccountLeaf(&update, empty.RootHash[:], nil)
}

func foldUpdate(plane byte, number int) commitment.Update {
	if plane == planeStorage {
		return *storageUpdate(commitmenttest.Storage(commitmenttest.StorageSpec{Number: number}))
	}
	return *testAccountUpdate(commitmenttest.Account(commitmenttest.AccountSpec{Kind: "fold", Number: number}))
}

func distinctKeysAndPaths(count int, seed int64) ([][]byte, [][]byte) {
	keys := commitmenttest.Keys(commitmenttest.MathRand(seed), commitmenttest.KeySpec{Kind: "random-distinct", Size: 20, Count: count})
	paths := make([][]byte, 0, len(keys))
	for _, key := range keys {
		paths = append(paths, commitment.KeyToHexNibbleHash(key))
	}
	return keys, paths
}

func foldKey(number int) []byte {
	return commitmenttest.Key(commitmenttest.KeySpec{Kind: "fold", Size: 20}, number)
}

func itoa(value int) string {
	return strconv.Itoa(value)
}
