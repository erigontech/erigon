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
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

// Reading back the preimages pbinHasher emits. A witness arrives from a peer, so
// every field the writer guarantees is checked here rather than assumed.

var errPBinWitnessNode = errors.New("pbin: malformed witness node")

// pbinWitnessNode is one decoded preimage. key and value alias the preimage they
// came from.
type pbinWitnessNode struct {
	tag      byte
	key      []byte // leaf: the whole tree key
	value    []byte // leaf: pbinValueLength bytes
	prefix   pbinBitpath
	children [2]common.Hash // branch: an absent child is pbinEmptyTreeHash
}

func (n *pbinWitnessNode) isLeaf() bool { return n.tag == pbinLeafTag }

func pbinDecodeWitnessNode(preimage []byte) (pbinWitnessNode, error) {
	if len(preimage) == 0 {
		return pbinWitnessNode{}, fmt.Errorf("%w: empty preimage", errPBinWitnessNode)
	}
	switch preimage[0] {
	case pbinLeafTag:
		return pbinDecodeWitnessLeaf(preimage)
	case pbinBranchTag:
		return pbinDecodeWitnessBranch(preimage)
	default:
		return pbinWitnessNode{}, fmt.Errorf("%w: unknown node tag %#x", errPBinWitnessNode, preimage[0])
	}
}

func pbinDecodeWitnessLeaf(preimage []byte) (pbinWitnessNode, error) {
	body := preimage[1:]
	if len(body) <= pbinValueLength {
		return pbinWitnessNode{}, fmt.Errorf("%w: leaf of %d bytes carries no key", errPBinWitnessNode, len(preimage))
	}
	key := body[:len(body)-pbinValueLength]
	// Key length is fixed per zone, which is what keeps the key space prefix-free
	// (eip:284-288).
	if want, known := pbinZoneKeyLength(key[0]); !known || len(key) != want {
		return pbinWitnessNode{}, fmt.Errorf("%w: leaf key %x is no key of zone %#x", errPBinWitnessNode, key, key[0])
	}
	return pbinWitnessNode{tag: pbinLeafTag, key: key, value: body[len(body)-pbinValueLength:]}, nil
}

func pbinDecodeWitnessBranch(preimage []byte) (pbinWitnessNode, error) {
	const head = 1 + 2 // tag, then the bit count encode_bit_prefix leads with
	if len(preimage) < head {
		return pbinWitnessNode{}, fmt.Errorf("%w: branch of %d bytes carries no bit count", errPBinWitnessNode, len(preimage))
	}
	bitLen := int(binary.BigEndian.Uint16(preimage[1:head]))
	if bitLen > pbinMaxPathBits {
		return pbinWitnessNode{}, fmt.Errorf("%w: branch prefix of %d bits exceeds the %d-bit path", errPBinWitnessNode, bitLen, pbinMaxPathBits)
	}
	packed := (bitLen + 7) / 8
	if want := head + packed + 2*length.Hash; len(preimage) != want {
		return pbinWitnessNode{}, fmt.Errorf("%w: branch of %d bytes, want %d for a %d-bit prefix", errPBinWitnessNode, len(preimage), want, bitLen)
	}
	if used := bitLen % 8; used != 0 && preimage[head+packed-1]&(byte(0xFF)>>uint(used)) != 0 {
		return pbinWitnessNode{}, fmt.Errorf("%w: %w in a %d-bit branch prefix", errPBinWitnessNode, errPBinNonCanonicalPad, bitLen)
	}
	n := pbinWitnessNode{
		tag:    pbinBranchTag,
		prefix: pbinPathFromBits(preimage[head:head+packed], int16(bitLen)),
	}
	children := preimage[head+packed:]
	n.children[0] = common.BytesToHash(children[:length.Hash])
	n.children[1] = common.BytesToHash(children[length.Hash:])
	return n, nil
}

// pbinWitnessTree is a decoded node set indexed by H(preimage), rooted at the
// hash the capture reported.
type pbinWitnessTree struct {
	nodes  map[common.Hash]pbinWitnessNode
	root   common.Hash
	hasher pbinHasher
}

// pbinDecodeWitness decodes a captured node set. preimages come root first per
// the witnessNodeSet.nodes contract, and root is checked against the first one:
// taking whatever leads the slice as the root would silently re-root the tree if
// that entry went missing.
func pbinDecodeWitness(preimages [][]byte, root []byte) (*pbinWitnessTree, error) {
	if len(root) != length.Hash {
		return nil, fmt.Errorf("%w: witness root of %d bytes", errPBinWitnessNode, len(root))
	}
	w := &pbinWitnessTree{
		nodes:  make(map[common.Hash]pbinWitnessNode, len(preimages)),
		root:   common.BytesToHash(root),
		hasher: pbinHasher{sum: pbinSelectedSum},
	}
	if len(preimages) == 0 {
		if w.root != pbinEmptyTreeHash {
			return nil, fmt.Errorf("%w: no nodes for root %x", errPBinWitnessNode, root)
		}
		return w, nil
	}
	for i, preimage := range preimages {
		node, err := pbinDecodeWitnessNode(preimage)
		if err != nil {
			return nil, fmt.Errorf("witness node %d: %w", i, err)
		}
		w.nodes[w.hasher.hash(preimage)] = node
	}
	if w.hasher.hash(preimages[0]) != w.root {
		return nil, fmt.Errorf("%w: first node is not root %x", errPBinWitnessNode, root)
	}
	return w, nil
}

// merkelize rehashes the tree from its root, so a decode that lost anything
// fails here instead of downstream. A child hash the set has no preimage for is
// blinded: opaque, and carried up as it stands.
func (w *pbinWitnessTree) merkelize() (common.Hash, error) {
	if len(w.nodes) == 0 {
		return pbinEmptyTreeHash, nil
	}
	got, err := w.merkelizeFrom(w.root, 0)
	if err != nil {
		return common.Hash{}, err
	}
	if got != w.root {
		return common.Hash{}, fmt.Errorf("%w: root %x re-merkelizes to %x", errPBinWitnessNode, w.root, got)
	}
	return got, nil
}

// merkelizeFrom rehashes the subtree at hash, sitting at bit position depth. The
// depth bounds the recursion: a branch consumes its prefix plus the bit it
// splits on, so nodes referencing each other in a cycle run out of path rather
// than running forever.
func (w *pbinWitnessTree) merkelizeFrom(hash common.Hash, depth int16) (common.Hash, error) {
	node, ok := w.nodes[hash]
	if !ok {
		return hash, nil
	}
	if node.isLeaf() {
		return w.hasher.leafNodeHash(node.key, node.value), nil
	}
	next := depth + node.prefix.bitLen + 1
	if int(next) > pbinMaxPathBits {
		return common.Hash{}, fmt.Errorf("%w: branch at bit %d with a %d-bit prefix overflows the %d-bit path",
			errPBinWitnessNode, depth, node.prefix.bitLen, pbinMaxPathBits)
	}
	left, err := w.merkelizeFrom(node.children[0], next)
	if err != nil {
		return common.Hash{}, err
	}
	right, err := w.merkelizeFrom(node.children[1], next)
	if err != nil {
		return common.Hash{}, err
	}
	return w.hasher.branchHash(&node.prefix, &left, &right), nil
}

// leafNodeHash is H over a leaf preimage built from a decoded key, where
// leafCellHash packs the key from a path and a cell.
func (h *pbinHasher) leafNodeHash(key, value []byte) common.Hash {
	buf := append(h.buf[:0], pbinLeafTag)
	buf = append(buf, key...)
	buf = append(buf, value...)
	return h.hash(buf)
}
