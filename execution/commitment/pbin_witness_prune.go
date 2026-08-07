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
	"fmt"

	"github.com/erigontech/erigon/common"
)

// Pruning the captured superset down to the proof paths of the keys the fold
// walked — the binary analogue of trie.WitnessNodesForKeysFromNodes. A binary
// branch commits to both of its children, so a path carries its own siblings and
// there is nothing to materialize alongside it.

// PBinWitnessNodesForKeys keeps the nodes on the proof path of every proved key
// and drops the rest, returning them in walk order so the root leads. A
// path that runs into a leaf, a diverging branch prefix or a blinded child stops
// there — what it walked through is the proof that the key is absent.
func PBinWitnessNodesForKeys(nodes [][]byte, root []byte, provedKeys [][]byte) ([][]byte, error) {
	if len(nodes) == 0 {
		return nil, nil
	}
	tree, err := pbinDecodeWitness(nodes, root)
	if err != nil {
		return nil, err
	}
	p := pbinWitnessPruner{tree: tree, kept: make(map[common.Hash]struct{}, len(tree.nodes))}
	// The root node leads the output even when no key descends past it.
	p.keep(tree.root)
	for _, key := range provedKeys {
		if err := p.walk(key); err != nil {
			return nil, err
		}
	}
	out := make([][]byte, 0, len(p.order))
	for _, hash := range p.order {
		out = append(out, tree.nodes[hash].preimage)
	}
	return out, nil
}

type pbinWitnessPruner struct {
	tree  *pbinWitnessTree
	kept  map[common.Hash]struct{}
	order []common.Hash
}

func (p *pbinWitnessPruner) keep(hash common.Hash) {
	if _, seen := p.kept[hash]; seen {
		return
	}
	p.kept[hash] = struct{}{}
	p.order = append(p.order, hash)
}

func (p *pbinWitnessPruner) walk(key []byte) error {
	path, err := pbinWitnessProvedPath(key)
	if err != nil {
		return err
	}
	hash, pos := p.tree.root, int16(0)
	for {
		node, ok := p.tree.nodes[hash]
		if !ok {
			return nil
		}
		p.keep(hash)
		if node.isLeaf() {
			return nil
		}
		end := pos + node.prefix.bitLen
		if end >= path.bitLen || pbinCommonPrefixBitsAt(&path, pos, &node.prefix) != node.prefix.bitLen {
			return nil
		}
		hash, pos = node.children[path.bit(end)], end+1
	}
}

// pbinWitnessProvedPath rejects a key no zone admits rather than letting
// pbinPathFromBytes panic on it. A key shorter than its zone's length is a
// subtree prefix, which an account removal proves in place of the leaves it
// drops, so the walk stops where that subtree begins.
func pbinWitnessProvedPath(key []byte) (pbinBitpath, error) {
	if len(key) == 0 {
		return pbinBitpath{}, fmt.Errorf("%w: empty proved key", errPBinWitnessNode)
	}
	if want, known := pbinZoneKeyLength(key[0]); !known || len(key) > want {
		return pbinBitpath{}, fmt.Errorf("%w: proved key %x is no key of zone %#x", errPBinWitnessNode, key, key[0])
	}
	return pbinPathFromBytes(key), nil
}
