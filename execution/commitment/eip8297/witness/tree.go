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

package witness

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type PBinResolveFunc func(path []byte) ([]byte, error)

type PBinResolvedNode struct {
	Path []byte
	Blob []byte
}

type pbinChild struct {
	hash common.Hash
	node *pbinNode
}

type pbinBranch struct {
	prefix      eip8297.Bitpath
	left, right *pbinChild
}

type pbinNode struct {
	walk    eip8297.Bitpath
	created bool
	branch  *pbinBranch
	group   *PBinGroup
}

type PBinTree struct {
	root     *pbinChild
	resolve  PBinResolveFunc
	resolved map[string]PBinResolvedNode
}

func NewPBinTree(root common.Hash, resolve PBinResolveFunc) (*PBinTree, error) {
	tree := &PBinTree{resolve: resolve, resolved: make(map[string]PBinResolvedNode)}
	if root == (common.Hash{}) {
		return tree, nil
	}
	if resolve == nil {
		return nil, errors.New("pbin witness: non-empty tree needs a resolver")
	}
	tree.root = &pbinChild{hash: root}
	var walk eip8297.Bitpath
	if _, err := tree.resolveChild(tree.root, walk); err != nil {
		return nil, err
	}
	return tree, nil
}

func (t *PBinTree) RootHash() common.Hash {
	if t == nil || t.root == nil {
		return common.Hash{}
	}
	return pbinRefHash(t.root)
}

func (t *PBinTree) Read(key []byte) ([]byte, bool, error) {
	if err := pbinValidateKey(key); err != nil {
		return nil, false, err
	}
	if t.root == nil {
		return nil, false, nil
	}
	return t.readChild(t.root, eip8297.Bitpath{}, eip8297.PathFromBytes(key))
}

func (t *PBinTree) PBinHasPrefix(prefix []byte) (bool, error) {
	if len(prefix) == 0 {
		return t.root != nil, nil
	}
	target := eip8297.PathFromBytes(prefix)
	return t.pbinHasPrefix(t.root, eip8297.Bitpath{}, target)
}

func (t *PBinTree) pbinHasPrefix(ref *pbinChild, walk, target eip8297.Bitpath) (bool, error) {
	if ref == nil || (ref.node == nil && ref.hash == (common.Hash{})) {
		return false, nil
	}
	if walk.BitLen >= target.BitLen {
		return walk.HasPrefix(&target), nil
	}
	if !target.HasPrefix(&walk) {
		return false, nil
	}
	node, err := t.resolveChild(ref, walk)
	if err != nil {
		return false, err
	}
	if node.group != nil {
		for _, sub := range node.group.Subs {
			key := append(slices.Clone(node.group.Stem), sub)
			path := eip8297.PathFromBytes(key)
			if path.HasPrefix(&target) {
				return true, nil
			}
		}
		return false, nil
	}
	branchPath := pbinAppend(walk, &node.branch.prefix)
	if target.BitLen <= branchPath.BitLen {
		return branchPath.HasPrefix(&target), nil
	}
	if !target.HasPrefix(&branchPath) {
		return false, nil
	}
	edge := target.Bit(branchPath.BitLen)
	childWalk := pbinChildWalk(walk, &node.branch.prefix, edge)
	if edge == 0 {
		return t.pbinHasPrefix(node.branch.left, childWalk, target)
	}
	return t.pbinHasPrefix(node.branch.right, childWalk, target)
}

func (t *PBinTree) Put(key, value []byte) error {
	if err := pbinValidateKey(key); err != nil {
		return err
	}
	if len(value) != eip8297.ValueLength {
		return fmt.Errorf("pbin witness: value has length %d, want %d", len(value), eip8297.ValueLength)
	}
	keyPath := eip8297.PathFromBytes(key)
	if t.root == nil {
		t.root = pbinLeafChild(eip8297.Bitpath{}, key, value)
		return nil
	}
	root, err := t.insertRef(t.root, eip8297.Bitpath{}, keyPath, key, value)
	if err != nil {
		return err
	}
	t.root = root
	return nil
}

func (t *PBinTree) Delete(key []byte) error {
	if err := pbinValidateKey(key); err != nil {
		return err
	}
	if t.root == nil {
		return nil
	}
	root, _, err := t.deleteChild(t.root, eip8297.Bitpath{}, eip8297.PathFromBytes(key))
	if err != nil {
		return err
	}
	t.root = root
	return nil
}

func (t *PBinTree) DeleteAccount(address []byte) error {
	if len(address) == 0 {
		return errors.New("pbin witness: empty account address")
	}
	var cache eip8297.DigestCache
	root, err := t.deletePrefix(t.root, eip8297.Bitpath{}, eip8297.PathFromBytes(cache.AccountHeaderStem(address)))
	if err != nil {
		return err
	}
	root, err = t.deletePrefix(root, eip8297.Bitpath{}, eip8297.PathFromBytes(cache.AccountStoragePrefix(address)))
	if err != nil {
		return err
	}
	t.root = root
	return nil
}

func (t *PBinTree) Resolved() []PBinResolvedNode {
	if t == nil || len(t.resolved) == 0 {
		return nil
	}
	result := make([]PBinResolvedNode, 0, len(t.resolved))
	for _, path := range slices.Sorted(maps.Keys(t.resolved)) {
		node := t.resolved[path]
		result = append(result, PBinResolvedNode{Path: slices.Clone(node.Path), Blob: slices.Clone(node.Blob)})
	}
	return result
}

func (t *PBinTree) resolveChild(child *pbinChild, walk eip8297.Bitpath) (*pbinNode, error) {
	if child == nil {
		return nil, nil
	}
	if child.node != nil {
		return child.node, nil
	}
	if t.resolve == nil {
		return nil, errors.New("pbin witness: missing resolver")
	}
	path := PBinPath(&walk)
	blob, err := t.resolve(slices.Clone(path))
	if err != nil {
		return nil, err
	}
	hash, err := PBinHashBlob(blob)
	if err != nil {
		return nil, fmt.Errorf("pbin witness: decode node at path %x: %w", path, err)
	}
	if hash != child.hash {
		return nil, fmt.Errorf("pbin witness: node at path %x hashes to %x, want %x", path, hash, child.hash)
	}
	decoded, err := PBinDecodeBlob(blob)
	if err != nil {
		return nil, err
	}
	node, err := pbinNodeFromDecoded(decoded, walk)
	if err != nil {
		return nil, err
	}
	child.node = node
	pathKey := string(path)
	if _, ok := t.resolved[pathKey]; !ok {
		t.resolved[pathKey] = PBinResolvedNode{Path: slices.Clone(path), Blob: slices.Clone(blob)}
	}
	return node, nil
}

func (t *PBinTree) readChild(child *pbinChild, walk, keyPath eip8297.Bitpath) ([]byte, bool, error) {
	node, err := t.resolveChild(child, walk)
	if err != nil {
		return nil, false, err
	}
	if node.group != nil {
		key := keyPath.AppendPackedBits(nil)
		if len(key) < len(node.group.Stem) || !bytes.Equal(node.group.Stem, key[:len(node.group.Stem)]) {
			return nil, false, nil
		}
		value, present := pbinGroupRead(node.group, key[len(key)-1])
		return value, present, nil
	}
	branch := node.branch
	matched := eip8297.CommonPrefixBitsAt(&keyPath, walk.BitLen, &branch.prefix)
	if matched < branch.prefix.BitLen {
		return nil, false, nil
	}
	edge := keyPath.Bit(walk.BitLen + branch.prefix.BitLen)
	childWalk := pbinChildWalk(walk, &branch.prefix, edge)
	if edge == 0 {
		return t.readChild(branch.left, childWalk, keyPath)
	}
	return t.readChild(branch.right, childWalk, keyPath)
}

func (t *PBinTree) insertRef(ref *pbinChild, walk, keyPath eip8297.Bitpath, key, value []byte) (*pbinChild, error) {
	if ref == nil {
		return pbinLeafChild(walk, key, value), nil
	}
	node, err := t.resolveChild(ref, walk)
	if err != nil {
		return nil, err
	}
	if node.group != nil {
		if bytes.Equal(node.group.Stem, key[:len(key)-1]) {
			pbinGroupPut(node.group, key[len(key)-1], value)
			return ref, nil
		}
		return pbinSplitGroup(node, keyPath, key, value), nil
	}
	branch := node.branch
	matched := eip8297.CommonPrefixBitsAt(&keyPath, walk.BitLen, &branch.prefix)
	if matched < branch.prefix.BitLen {
		return pbinSplitBranch(node, keyPath, matched, walk.BitLen, key, value), nil
	}
	edge := keyPath.Bit(walk.BitLen + branch.prefix.BitLen)
	childWalk := pbinChildWalk(walk, &branch.prefix, edge)
	if edge == 0 {
		branch.left, err = t.insertRef(branch.left, childWalk, keyPath, key, value)
	} else {
		branch.right, err = t.insertRef(branch.right, childWalk, keyPath, key, value)
	}
	if err != nil {
		return nil, err
	}
	return ref, nil
}

func (t *PBinTree) deleteChild(ref *pbinChild, walk, keyPath eip8297.Bitpath) (*pbinChild, bool, error) {
	if ref == nil {
		return nil, false, nil
	}
	node, err := t.resolveChild(ref, walk)
	if err != nil {
		return nil, false, err
	}
	if node.group != nil {
		key := keyPath.AppendPackedBits(nil)
		if len(key) < len(node.group.Stem) || !bytes.Equal(node.group.Stem, key[:len(node.group.Stem)]) {
			return ref, false, nil
		}
		if !pbinGroupDelete(node.group, key[len(key)-1]) {
			return ref, false, nil
		}
		if len(node.group.Subs) == 0 {
			return nil, true, nil
		}
		return ref, true, nil
	}
	branch := node.branch
	matched := eip8297.CommonPrefixBitsAt(&keyPath, walk.BitLen, &branch.prefix)
	if matched < branch.prefix.BitLen {
		return ref, false, nil
	}
	edge := keyPath.Bit(walk.BitLen + branch.prefix.BitLen)
	childWalk := pbinChildWalk(walk, &branch.prefix, edge)
	var next *pbinChild
	var changed bool
	if edge == 0 {
		next, changed, err = t.deleteChild(branch.left, childWalk, keyPath)
		branch.left = next
	} else {
		next, changed, err = t.deleteChild(branch.right, childWalk, keyPath)
		branch.right = next
	}
	if err != nil || !changed {
		return ref, changed, err
	}
	if branch.left == nil || branch.right == nil {
		return t.collapseMissingChild(branch.left, branch.right, walk, &branch.prefix)
	}
	return ref, true, nil
}

func (t *PBinTree) collapse(survivor *pbinChild, parentWalk eip8297.Bitpath, parentPrefix *eip8297.Bitpath, edge uint64) (*pbinChild, error) {
	if survivor == nil {
		return nil, nil
	}
	survivorWalk := pbinChildWalk(parentWalk, parentPrefix, edge)
	node, err := t.resolveChild(survivor, survivorWalk)
	if err != nil {
		return nil, err
	}
	extra := *parentPrefix
	extra.AppendBit(edge)
	return &pbinChild{node: pbinRebaseNode(node, parentWalk, &extra)}, nil
}

func (t *PBinTree) deletePrefix(ref *pbinChild, walk, target eip8297.Bitpath) (*pbinChild, error) {
	if ref == nil {
		return nil, nil
	}
	if walk.BitLen >= target.BitLen {
		if walk.HasPrefix(&target) {
			return nil, nil
		}
		return ref, nil
	}
	if !target.HasPrefix(&walk) {
		return ref, nil
	}
	node, err := t.resolveChild(ref, walk)
	if err != nil {
		return nil, err
	}
	if node.group != nil {
		firstKey := append(slices.Clone(node.group.Stem), node.group.Subs[0])
		firstPath := eip8297.PathFromBytes(firstKey)
		if target.BitLen <= int16(len(node.group.Stem)*8) && firstPath.HasPrefix(&target) {
			return nil, nil
		}
		for i := range slices.Backward(node.group.Subs) {
			key := append(slices.Clone(node.group.Stem), node.group.Subs[i])
			path := eip8297.PathFromBytes(key)
			if path.HasPrefix(&target) {
				node.group.Subs = slices.Delete(node.group.Subs, i, i+1)
				node.group.Values = slices.Delete(node.group.Values, i, i+1)
			}
		}
		if len(node.group.Subs) == 0 {
			return nil, nil
		}
		return ref, nil
	}
	branch := node.branch
	branchPath := pbinAppend(walk, &branch.prefix)
	if target.BitLen <= branchPath.BitLen && branchPath.HasPrefix(&target) {
		return nil, nil
	}
	if !target.HasPrefix(&branchPath) {
		return ref, nil
	}
	edge := target.Bit(branchPath.BitLen)
	childWalk := pbinChildWalk(walk, &branch.prefix, edge)
	if edge == 0 {
		branch.left, err = t.deletePrefix(branch.left, childWalk, target)
	} else {
		branch.right, err = t.deletePrefix(branch.right, childWalk, target)
	}
	if err != nil {
		return nil, err
	}
	if branch.left == nil || branch.right == nil {
		collapsed, _, err := t.collapseMissingChild(branch.left, branch.right, walk, &branch.prefix)
		return collapsed, err
	}
	return ref, nil
}

func (t *PBinTree) collapseMissingChild(left, right *pbinChild, walk eip8297.Bitpath, prefix *eip8297.Bitpath) (*pbinChild, bool, error) {
	if left != nil && right != nil {
		return nil, false, nil
	}
	survivor, edge := left, uint64(0)
	if left == nil {
		survivor, edge = right, 1
	}
	collapsed, err := t.collapse(survivor, walk, prefix, edge)
	return collapsed, true, err
}

func pbinNodeFromDecoded(decoded PBinDecodedBlob, walk eip8297.Bitpath) (*pbinNode, error) {
	switch {
	case decoded.Leaf != nil:
		key := decoded.Leaf.Key
		return &pbinNode{walk: walk, group: &PBinGroup{Position: uint16(walk.BitLen), Stem: slices.Clone(key[:len(key)-1]), Subs: []byte{key[len(key)-1]}, Values: [][]byte{slices.Clone(decoded.Leaf.Value)}}}, nil
	case decoded.Group != nil:
		if decoded.Group.Position != uint16(walk.BitLen) {
			return nil, fmt.Errorf("pbin witness: group position %d does not match path length %d", decoded.Group.Position, walk.BitLen)
		}
		return &pbinNode{walk: walk, group: decoded.Group}, nil
	case decoded.Branch != nil:
		return &pbinNode{walk: walk, branch: &pbinBranch{prefix: decoded.Branch.Prefix, left: &pbinChild{hash: decoded.Branch.Left}, right: &pbinChild{hash: decoded.Branch.Right}}}, nil
	default:
		return nil, errors.New("pbin witness: empty node")
	}
}

func pbinLeafChild(walk eip8297.Bitpath, key, value []byte) *pbinChild {
	return &pbinChild{node: &pbinNode{walk: walk, created: true, group: &PBinGroup{Position: uint16(walk.BitLen), Stem: slices.Clone(key[:len(key)-1]), Subs: []byte{key[len(key)-1]}, Values: [][]byte{slices.Clone(value)}}}}
}

func pbinRefHash(ref *pbinChild) common.Hash {
	if ref == nil {
		return common.Hash{}
	}
	if ref.node == nil {
		return ref.hash
	}
	return pbinNodeHash(ref.node)
}

func pbinNodeHash(node *pbinNode) common.Hash {
	if node.group != nil {
		return pbinFoldGroup(node.group)
	}
	left := pbinRefHash(node.branch.left)
	right := pbinRefHash(node.branch.right)
	return eip8297.HashBytes(eip8297.BranchPreimage(nil, &node.branch.prefix, &left, &right))
}

func pbinGroupPut(group *PBinGroup, sub byte, value []byte) {
	index, found := slices.BinarySearch(group.Subs, sub)
	if found {
		group.Values[index] = slices.Clone(value)
		return
	}
	group.Subs = slices.Insert(group.Subs, index, sub)
	group.Values = slices.Insert(group.Values, index, slices.Clone(value))
}

func pbinGroupRead(group *PBinGroup, sub byte) ([]byte, bool) {
	index := slices.Index(group.Subs, sub)
	if index >= 0 {
		return slices.Clone(group.Values[index]), true
	}
	return nil, false
}

func pbinGroupDelete(group *PBinGroup, sub byte) bool {
	index := slices.Index(group.Subs, sub)
	if index < 0 {
		return false
	}
	group.Subs = slices.Delete(group.Subs, index, index+1)
	group.Values = slices.Delete(group.Values, index, index+1)
	return true
}

func pbinBranchChild(walk, prefix eip8297.Bitpath, left, right *pbinChild) *pbinChild {
	return &pbinChild{node: &pbinNode{walk: walk, created: true, branch: &pbinBranch{prefix: prefix, left: left, right: right}}}
}

func pbinSplitGroup(node *pbinNode, keyPath eip8297.Bitpath, key, value []byte) *pbinChild {
	existingKey := append(slices.Clone(node.group.Stem), node.group.Subs[0])
	existingPath := eip8297.PathFromBytes(existingKey)
	existingSuffix := existingPath.Slice(node.walk.BitLen, existingPath.BitLen)
	divergence := node.walk.BitLen + eip8297.CommonPrefixBitsAt(&keyPath, node.walk.BitLen, &existingSuffix)
	prefix := keyPath.Slice(node.walk.BitLen, divergence)
	existingEdge := existingPath.Bit(divergence)
	newEdge := keyPath.Bit(divergence)
	existingWalk := pbinChildWalk(node.walk, &prefix, existingEdge)
	newWalk := pbinChildWalk(node.walk, &prefix, newEdge)
	newChild := pbinLeafChild(newWalk, key, value)
	var left, right *pbinChild
	if existingEdge == 0 {
		left = &pbinChild{node: pbinRebaseNode(node, existingWalk, nil)}
		right = newChild
	} else {
		left = newChild
		right = &pbinChild{node: pbinRebaseNode(node, existingWalk, nil)}
	}
	return pbinBranchChild(node.walk, prefix, left, right)
}

func pbinSplitBranch(node *pbinNode, keyPath eip8297.Bitpath, matched, start int16, key, value []byte) *pbinChild {
	oldPrefix := node.branch.prefix
	commonPrefix := oldPrefix.Slice(0, matched)
	existingEdge := oldPrefix.Bit(matched)
	newEdge := keyPath.Bit(start + matched)
	existingWalk := pbinChildWalk(node.walk, &commonPrefix, existingEdge)
	suffix := oldPrefix.Slice(matched+1, oldPrefix.BitLen)
	existingNode := &pbinNode{walk: existingWalk, created: true, branch: &pbinBranch{prefix: suffix, left: node.branch.left, right: node.branch.right}}
	newChild := pbinLeafChild(pbinChildWalk(node.walk, &commonPrefix, newEdge), key, value)
	var left, right *pbinChild
	if existingEdge == 0 {
		left = &pbinChild{node: existingNode}
		right = newChild
	} else {
		left = newChild
		right = &pbinChild{node: existingNode}
	}
	return pbinBranchChild(node.walk, commonPrefix, left, right)
}

func pbinRebaseNode(node *pbinNode, walk eip8297.Bitpath, extra *eip8297.Bitpath) *pbinNode {
	if node.group != nil {
		group := *node.group
		group.Position = uint16(walk.BitLen)
		return &pbinNode{walk: walk, created: true, group: &group}
	}
	prefix := node.branch.prefix
	if extra != nil {
		prefix = pbinAppend(*extra, &prefix)
	}
	return &pbinNode{walk: walk, created: true, branch: &pbinBranch{prefix: prefix, left: node.branch.left, right: node.branch.right}}
}

func pbinChildWalk(walk eip8297.Bitpath, prefix *eip8297.Bitpath, edge uint64) eip8297.Bitpath {
	result := walk
	result.Append(prefix)
	result.AppendBit(edge)
	return result
}

func pbinAppend(walk eip8297.Bitpath, suffix *eip8297.Bitpath) eip8297.Bitpath {
	walk.Append(suffix)
	return walk
}
