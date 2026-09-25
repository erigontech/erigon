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
	"fmt"
	"math/bits"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTrieMutations(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"E12/InsertLeafAtEveryDepthInBothPlanes", func(t *testing.T) {
			for _, plane := range []byte{planeAccount, planeStorage} {
				t.Run(fmtPlane(plane), func(t *testing.T) {
					for depth := 0; depth <= 62; depth++ {
						prefix := repeatedPath(depth, 3)
						existing := appendPath(prefix, 4, bytes.Repeat([]byte{5}, 63-depth))
						incoming := appendPath(prefix, 4, bytes.Repeat([]byte{6}, 63-depth))
						n := fork(prefix)
						n.plane = plane
						n.setLeaf(4, packPath(existing[depth+1:], nil), []byte{0x11})

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
			}
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
		{"E39/InsertUsesNoStateReads", func(t *testing.T) {
			n := fork(nil)
			oldPath := repeatedPath(64, 1)
			newPath := append(append([]byte(nil), oldPath[:2]...), bytes.Repeat([]byte{2}, 62)...)
			n.setLeaf(1, packPath(oldPath[1:], nil), []byte{1})
			require.NoError(t, insert(n, newPath, []byte{2}))
		}},
		{"E13/InsertRejectsStoredDescendant", func(t *testing.T) {
			n := fork(nil)
			n.setStoredChild(1, bytes.Repeat([]byte{0xab}, 32), []byte{2, 3})
			storedPath := append([]byte{1, 2, 3}, bytes.Repeat([]byte{4}, 61)...)
			require.ErrorIs(t, insert(n, storedPath, []byte{2}), ErrInsertStoredChild)
		}},
		{"E9-E14-E16/RemoveClearsLeafAndMasks", func(t *testing.T) {
			n := fork([]byte{2})
			path := appendPath(n.path, 3, bytes.Repeat([]byte{4}, 62))
			n.setLeaf(3, packPath(path[2:], nil), []byte{1})
			other := appendPath(n.path, 5, bytes.Repeat([]byte{6}, 62))
			n.setLeaf(5, packPath(other[2:], nil), []byte{2})

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
			removed := appendPath(child.path, 3, bytes.Repeat([]byte{5}, 59))
			survivor := appendPath(child.path, 6, bytes.Repeat([]byte{7}, 59))
			child.setLeaf(3, packPath(removed[len(child.path)+1:], nil), []byte{1})
			child.setLeaf(6, packPath(survivor[len(child.path)+1:], nil), []byte{2})
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
		{"E15/RemoveD6CollapseKeepsPreExtensionHash", func(t *testing.T) {
			parent := fork([]byte{7})
			hash := bytes.Repeat([]byte{0xa5}, 32)
			parent.setStoredChild(0, hash, []byte{1, 2})
			removed := appendPath(parent.path, 1, bytes.Repeat([]byte{3}, 62))
			parent.setLeaf(1, packPath(removed[len(parent.path)+1:], nil), []byte{9})

			require.NoError(t, removeErr(parent, removed))
			require.Equal(t, uint16(1), parent.childMask)
			require.Zero(t, parent.leafMask)
			require.Equal(t, hash, parent.childHashAt(0))
			require.Equal(t, []byte{1, 2}, parent.childExtAt(0))
			path := append(append([]byte(nil), parent.path...), byte(0))
			path = append(path, parent.childExtAt(0)...)
			require.Equal(t, []byte{7, 0, 1, 2}, path)
		}},
		{"E15/RemoveCollapseSoleBranchWithoutExtension", func(t *testing.T) {
			parent := fork([]byte{7})
			hash := bytes.Repeat([]byte{0x31}, 32)
			parent.setStoredChild(0, hash, nil)
			removed := appendPath(parent.path, 1, bytes.Repeat([]byte{3}, 62))
			parent.setLeaf(1, packPath(removed[len(parent.path)+1:], nil), []byte{9})

			require.NoError(t, removeErr(parent, removed))
			require.Equal(t, hash, parent.childHashAt(0))
			require.Empty(t, parent.childExtAt(0))
			require.Zero(t, parent.leafMask)
		}},
		{"E15/RemoveConcatenatesNestedExtensions", func(t *testing.T) {
			parent := fork([]byte{7})
			child := fork([]byte{7, 0, 1, 2})
			hash := bytes.Repeat([]byte{0x4c}, 32)
			child.setStoredChild(3, hash, []byte{4, 5})
			removed := appendPath(child.path, 6, bytes.Repeat([]byte{8}, 59))
			child.setLeaf(6, packPath(removed[len(child.path)+1:], nil), []byte{9})
			parent.setChild(0, child)

			require.NoError(t, removeErr(parent, removed))
			require.Nil(t, parent.child(0))
			require.Equal(t, hash, parent.childHashAt(0))
			require.Equal(t, []byte{1, 2, 3, 4, 5}, parent.childExtAt(0))
			path := append(append([]byte(nil), parent.path...), byte(0))
			path = append(path, parent.childExtAt(0)...)
			require.Equal(t, []byte{7, 0, 1, 2, 3, 4, 5}, path)
		}},
		{"E39/RemoveDoesNotReadBranchRecords", func(t *testing.T) {
			n := fork(nil)
			removed := appendPath(nil, 1, bytes.Repeat([]byte{2}, 63))
			survivor := appendPath(nil, 3, bytes.Repeat([]byte{4}, 63))
			n.setLeaf(1, packPath(removed[1:], nil), []byte{1})
			n.setLeaf(3, packPath(survivor[1:], nil), []byte{2})

			require.NoError(t, removeErr(n, removed))
		}},
		{"E16/RemoveTreatsDivergedPathsAsNotFound", func(t *testing.T) {
			ext := fork([]byte{1, 2, 3})
			kept := appendPath(ext.path, 4, bytes.Repeat([]byte{5}, 60))
			ext.setLeaf(4, packPath(kept[4:], nil), []byte{1})
			diverged := appendPath([]byte{1, 2, 9}, 4, bytes.Repeat([]byte{5}, 60))
			require.ErrorIs(t, removeErr(ext, diverged), ErrRemoveNotFound)

			root := fork(nil)
			branch := fork([]byte{7, 8, 8})
			branchLeaf := appendPath(branch.path, 1, bytes.Repeat([]byte{2}, 60))
			branch.setLeaf(1, packPath(branchLeaf[4:], nil), []byte{3})
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
			for _, plane := range []byte{planeAccount, planeStorage} {
				t.Run(fmtPlane(plane), func(t *testing.T) {
					n := fork(nil)
					n.plane = plane
					path := appendPath(nil, 3, bytes.Repeat([]byte{4}, 63))
					n.setLeaf(3, packPath(path[1:], nil), []byte{1})
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
			}
		}},
		{"E18/RootExtensionInsertDivergenceKeepsChildBody", func(t *testing.T) {
			for _, plane := range []byte{planeAccount, planeStorage} {
				t.Run(fmtPlane(plane), func(t *testing.T) {
					hash := bytes.Repeat([]byte{0xa5}, 32)
					childRecordPath := []byte{1, 2, 3}
					n := fork(childRecordPath)
					n.plane = plane
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
			}
		}},
		{"E15-E17/RootCollapseRewritesOnlyTheRoot", func(t *testing.T) {
			for _, plane := range []byte{planeAccount, planeStorage} {
				t.Run(fmtPlane(plane), func(t *testing.T) {
					hash := bytes.Repeat([]byte{0x31}, 32)
					n := fork(nil)
					n.plane = plane
					n.setStoredChild(3, hash, []byte{4, 5})
					removed := appendPath(nil, 9, bytes.Repeat([]byte{6}, 63))
					n.setLeaf(9, packPath(removed[1:], nil), []byte{0x44})

					require.NoError(t, removeRoot(n, removed))
					require.Equal(t, []byte{3, 4, 5}, n.path)
					require.Equal(t, hash, n.childHashAt(3))
					require.Empty(t, n.childExtAt(3))
					data := encodeRecord(n, 0, nil)
					require.NoError(t, Validate(data, 0))
					require.Equal(t, []byte{3, 4, 5}, unpackPath(Record{data: data, depth: 0}.SelfExt()[1:], 3, nil))
				})
			}
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
			for _, plane := range []byte{planeAccount, planeStorage} {
				t.Run(fmtPlane(plane), func(t *testing.T) {
					n := fork(nil)
					n.plane = plane
					child := fork([]byte{3})
					leafA := appendPath([]byte{3}, 1, bytes.Repeat([]byte{7}, 62))
					leafB := appendPath([]byte{3}, 2, bytes.Repeat([]byte{8}, 62))
					child.setLeaf(1, packPath(leafA[2:], nil), []byte{0xa1})
					child.setLeaf(2, packPath(leafB[2:], nil), []byte{0xa2})
					n.setChild(3, child)
					removed := appendPath(nil, 9, bytes.Repeat([]byte{6}, 63))
					n.setLeaf(9, packPath(removed[1:], nil), []byte{0x44})

					require.NoError(t, removeRoot(n, removed))
					require.Equal(t, []byte{3}, n.path)
					require.Equal(t, 1, bits.OnesCount16(n.childMask))
					require.Same(t, child, n.child(3))
				})
			}
		}},
		{"E18/RootExtensionKeepsItsChildUnderTheFirstNibble", func(t *testing.T) {
			for _, plane := range []byte{planeAccount, planeStorage} {
				t.Run(fmtPlane(plane), func(t *testing.T) {
					a := appendPath(nil, 5, append([]byte{5, 1}, bytes.Repeat([]byte{4}, 61)...))
					b := appendPath(nil, 5, append([]byte{5, 2}, bytes.Repeat([]byte{4}, 61)...))
					c := appendPath(nil, 6, bytes.Repeat([]byte{4}, 63))

					viaLeafRoot := fork(nil)
					viaLeafRoot.plane = plane
					require.NoError(t, insertRoot(viaLeafRoot, a, []byte{1}))
					require.NoError(t, insertRoot(viaLeafRoot, b, []byte{2}))

					viaCollapse := fork(nil)
					viaCollapse.plane = plane
					require.NoError(t, insertRoot(viaCollapse, a, []byte{1}))
					require.NoError(t, insertRoot(viaCollapse, c, []byte{3}))
					require.NoError(t, insertRoot(viaCollapse, b, []byte{2}))
					require.NoError(t, removeRoot(viaCollapse, c))

					require.Equal(t, []byte{5, 5}, viaCollapse.path)
					require.Equal(t, viaCollapse.path, viaLeafRoot.path)
					require.Equal(t, viaCollapse.childMask, viaLeafRoot.childMask)
				})
			}
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
