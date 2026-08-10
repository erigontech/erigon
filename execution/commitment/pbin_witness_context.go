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
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
)

// A decoded witness served as a PatriciaContext, so PBinPatriciaHashed is itself
// the mutable trie a post-state root comes out of — leaf splitting, branch
// creation, BASIC_DATA packing and code chunking included — instead of a second
// binary trie written beside it.

var (
	errPBinWitnessBlinded = errors.New("pbin: witness node is blinded")
	errPBinWitnessNoState = errors.New("pbin: witness holds no state")
)

// pbinWitnessContext turns node preimages into the branch records the engine
// unfolds. A record is derived on first read and cached; PutBranch replaces it,
// so a fold reads back what it wrote.
type pbinWitnessContext struct {
	tree    *pbinWitnessTree
	records map[string][]byte
	leaves  map[string]Update
	codes   map[string][]byte
	keys    pbinDigestCache
}

var (
	_ PatriciaContext    = (*pbinWitnessContext)(nil)
	_ pbinCodeContext    = (*pbinWitnessContext)(nil)
	_ pbinDerivedContext = (*pbinWitnessContext)(nil)
)

func (c *pbinWitnessContext) pbinRecordsAreDerived() {}

func pbinNewWitnessContext(tree *pbinWitnessTree) *pbinWitnessContext {
	return &pbinWitnessContext{
		tree:    tree,
		records: make(map[string][]byte),
		leaves:  make(map[string]Update),
		codes:   make(map[string][]byte),
		keys:    pbinDigestCache{sum: pbinSelectedSum},
	}
}

// setCode supplies bytecode the node set cannot hold: code a block deploys has
// no pre-state chunk leaves to reassemble.
func (c *pbinWitnessContext) setCode(plainKey, code []byte) {
	c.codes[string(plainKey)] = bytes.Clone(code)
}

func (c *pbinWitnessContext) Branch(prefix []byte) ([]byte, kv.Step, error) {
	if record, ok := c.records[string(prefix)]; ok {
		return record, 0, nil
	}
	record, err := c.deriveRecord(prefix)
	if err != nil {
		return nil, 0, err
	}
	c.records[string(prefix)] = record
	return record, 0, nil
}

func (c *pbinWitnessContext) PutBranch(prefix, data, prevData []byte) error {
	c.records[string(prefix)] = bytes.Clone(data)
	return nil
}

func (c *pbinWitnessContext) Account(plainKey []byte) (*Update, error) { return c.leafState(plainKey) }

func (c *pbinWitnessContext) Storage(plainKey []byte) (*Update, error) { return c.leafState(plainKey) }

func (c *pbinWitnessContext) Code(plainKey []byte) ([]byte, error) {
	if code, ok := c.codes[string(plainKey)]; ok {
		return code, nil
	}
	code, err := c.codeFromLeaves(plainKey)
	if err != nil {
		return nil, err
	}
	if code == nil {
		return nil, fmt.Errorf("%w: no code for %x", errPBinWitnessNoState, plainKey)
	}
	return code, nil
}

// leafState resolves the handle a witness leaf cell carries in place of a plain
// key: the witness holds a leaf's value, never the address it was derived from.
// Anything else is refused — an empty read would hash a zeroed leaf into the
// root instead of failing.
func (c *pbinWitnessContext) leafState(plainKey []byte) (*Update, error) {
	state, ok := c.leaves[string(plainKey)]
	if !ok {
		return nil, fmt.Errorf("%w for plain key %x", errPBinWitnessNoState, plainKey)
	}
	return &state, nil
}

func (c *pbinWitnessContext) deriveRecord(prefix []byte) ([]byte, error) {
	if bytes.Equal(prefix, pbinRootKey) {
		return c.rootRecord()
	}
	path, err := pbinDecodeBitPath(prefix)
	if err != nil {
		return nil, err
	}
	node, err := c.nodeAt(&path)
	if err != nil {
		return nil, err
	}
	return c.branchRecord(&node, &path)
}

// rootRecord holds the one cell no descent can name. An empty tree has no
// record at all, which is the only shape a caller may read as absent.
func (c *pbinWitnessContext) rootRecord() ([]byte, error) {
	if c.tree.root == pbinEmptyTreeHash {
		return []byte{}, nil
	}
	if _, ok := c.tree.nodes[c.tree.root]; !ok {
		return nil, fmt.Errorf("%w: no preimage for root %x", errPBinWitnessBlinded, c.tree.root)
	}
	var cell pbinCell
	cell.reset()
	var path pbinBitpath
	if err := c.fillCell(&cell, c.tree.root, &path); err != nil {
		return nil, err
	}
	return pbinAppendCell(nil, &cell)
}

func (c *pbinWitnessContext) branchRecord(node *pbinWitnessNode, path *pbinBitpath) ([]byte, error) {
	if path.bitLen >= pbinMaxPathBits {
		return nil, fmt.Errorf("%w: a branch at %d bits leaves no room for a child",
			errPBinWitnessNode, path.bitLen)
	}
	var cells [2]pbinCell
	for bit := range cells {
		childPath := *path
		childPath.appendBit(uint64(bit))
		cells[bit].reset()
		if err := c.fillCell(&cells[bit], node.children[bit], &childPath); err != nil {
			return nil, err
		}
	}
	var encoder pbinBranchEncoder
	// The touch map is write-time bookkeeping a read discards, so it says the same
	// as the after map.
	record, err := encoder.encode(pbinCellBits, pbinCellBits, &cells)
	if err != nil {
		return nil, err
	}
	return bytes.Clone(record), nil
}

// fillCell describes one child of a branch. A child with no preimage is opaque:
// it hashes to what its parent commits to, and a descent into it fails in
// nodeAt, where the path is known.
func (c *pbinWitnessContext) fillCell(cell *pbinCell, hash common.Hash, path *pbinBitpath) error {
	if hash == pbinEmptyTreeHash {
		// A binary node with one child is a node the fold would collapse, quietly
		// moving the root.
		return fmt.Errorf("%w: branch child at bit %d is the empty tree", errPBinWitnessNode, path.bitLen)
	}
	node, ok := c.tree.nodes[hash]
	if !ok || !node.isLeaf() {
		cell.kind = pbinNodeBranch
		if ok {
			cell.prefix = node.prefix
		}
		cell.hash, cell.hashLen = hash, length.Hash
		return nil
	}
	return c.fillLeafCell(cell, &node, hash, path)
}

func (c *pbinWitnessContext) fillLeafCell(cell *pbinCell, node *pbinWitnessNode, hash common.Hash, path *pbinBitpath) error {
	key := pbinPathFromBytes(node.key)
	if !key.hasPrefix(path) {
		return fmt.Errorf("%w: leaf %x does not sit under the %d-bit path it was reached by",
			errPBinWitnessNode, node.key, path.bitLen)
	}
	cell.kind = pbinNodeLeaf
	cell.prefix = key.slice(path.bitLen, key.bitLen)

	// A record holds a leaf value either verbatim or as the account fields it is
	// packed from. Which one applies is decided by re-encoding, not by zone, so
	// this cannot drift from pbinLeafValue.
	verbatim := cell.Update
	verbatim.Flags, verbatim.StorageLen = StorageUpdate, pbinValueLength
	copy(verbatim.Storage[:], node.value)
	if value, err := pbinLeafValue(node.key, &verbatim); err == nil && bytes.Equal(value[:], node.value) {
		cell.Update = verbatim
		return nil
	}

	state, err := pbinWitnessLeafState(node.key, node.value)
	if err != nil {
		return err
	}
	handle := hash[:length.Addr]
	if prev, seen := c.leaves[string(handle)]; seen && prev != state {
		return fmt.Errorf("%w: two leaves share the handle %x", errPBinWitnessNode, handle)
	}
	c.leaves[string(handle)] = state
	cell.accountAddrLen = length.Addr
	copy(cell.accountAddr[:], handle)
	return nil
}

// pbinWitnessLeafState inverts the packing pbinLeafValue applies, for the leaves
// a record cannot carry verbatim: BASIC_DATA and CODE_HASH are built from
// account fields, so the cell has to hold those fields instead. The result is
// re-encoded before it is returned, which rejects any value the tree could not
// have produced.
func pbinWitnessLeafState(key, value []byte) (Update, error) {
	var u Update
	u.Reset()
	if key[0] == pbinAccountZone {
		switch key[len(key)-1] {
		case pbinBasicDataLeafKey:
			u.Flags = NonceUpdate | BalanceUpdate | CodeUpdate
			u.CodeSize = uint64(binary.BigEndian.Uint32(value[pbinBasicDataCodeSizeOffset:]))
			u.Nonce = binary.BigEndian.Uint64(value[pbinBasicDataNonceOffset:])
			u.Balance.SetBytes(value[pbinBasicDataBalanceOffset:])
		case pbinCodeHashLeafKey:
			u.Flags = CodeUpdate
			u.CodeHash = common.BytesToHash(value)
		}
	}
	if u.Flags == 0 {
		return u, fmt.Errorf("%w: leaf %x carries a value no record can hold", errPBinWitnessNode, key)
	}
	got, err := pbinLeafValue(key, &u)
	if err != nil {
		return u, err
	}
	if !bytes.Equal(got[:], value) {
		return u, fmt.Errorf("%w: leaf %x holds %x, which no state packs to", errPBinWitnessNode, key, value)
	}
	return u, nil
}

// nodeAt finds the node whose absolute path is p: the root node's path is its
// own prefix, and a child's is its parent's path, the bit it hangs off, and its
// own prefix.
func (c *pbinWitnessContext) nodeAt(p *pbinBitpath) (pbinWitnessNode, error) {
	hash, pos := c.tree.root, int16(0)
	for {
		node, ok := c.tree.nodes[hash]
		if !ok {
			return node, fmt.Errorf("%w: no preimage for %x, reached at bit %d of the %d-bit path %x",
				errPBinWitnessBlinded, hash, pos, p.bitLen, p.appendPackedBits(nil))
		}
		if node.isLeaf() {
			return node, fmt.Errorf("%w: a leaf covers bit %d of the %d-bit path %x",
				errPBinWitnessNode, pos, p.bitLen, p.appendPackedBits(nil))
		}
		end := pos + node.prefix.bitLen
		if end > p.bitLen || pbinCommonPrefixBitsAt(p, pos, &node.prefix) != node.prefix.bitLen {
			return node, fmt.Errorf("%w: no node at the %d-bit path %x",
				errPBinWitnessNode, p.bitLen, p.appendPackedBits(nil))
		}
		if end == p.bitLen {
			return node, nil
		}
		hash, pos = node.children[p.bit(end)], end+1
	}
}
