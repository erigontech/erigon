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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/execution/rlp"
)

// ProofFromNodes returns the proof for key (keybytes) and its leaf value, which is nil when key is absent.
func ProofFromNodes(byHash map[string][]byte, root, key []byte) (proof [][]byte, value []byte, err error) {
	enc, ok := byHash[string(root)]
	if !ok {
		if common.BytesToHash(root) == empty.RootHash { // an empty trie proves absence with no nodes at all
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("proof node %x absent", root)
	}
	path := nibbles.KeybytesToHex(key)[:2*len(key)] // without the terminator nibble
	for enc != nil {
		// An inline child is carried inside its parent, so emitting it again would leave the
		// verifier an element it never asks for.
		if len(proof) == 0 || len(enc) >= length.Hash {
			proof = append(proof, enc)
		}
		elems, _, err := rlp.SplitList(enc)
		if err != nil {
			return nil, nil, err
		}
		n, err := rlp.CountValues(elems)
		if err != nil {
			return nil, nil, err
		}
		switch n {
		case 2:
			compact, rest, err := rlp.SplitString(elems)
			if err != nil {
				return nil, nil, err
			}
			nodeKey := nibbles.CompactToHex(compact)
			leaf := nibbles.HasTerm(nodeKey)
			if leaf {
				nodeKey = nodeKey[:len(nodeKey)-1]
			}
			if !bytes.HasPrefix(path, nodeKey) {
				return proof, nil, nil
			}
			path = path[len(nodeKey):]
			if leaf {
				if len(path) != 0 {
					return proof, nil, nil
				}
				value, _, err = rlp.SplitString(rest)
				return proof, value, err
			}
			if enc, err = childNode(byHash, rest); err != nil {
				return nil, nil, err
			}
		case 17:
			rest := elems
			if len(path) == 0 {
				return nil, nil, errors.New("key ends at a branch node")
			}
			for range path[0] {
				if _, _, rest, err = rlp.Split(rest); err != nil {
					return nil, nil, err
				}
			}
			path = path[1:]
			if enc, err = childNode(byHash, rest); err != nil {
				return nil, nil, err
			}
		default:
			return nil, nil, fmt.Errorf("invalid number of list elements: %d", n)
		}
	}
	return proof, nil, nil
}

func childNode(byHash map[string][]byte, buf []byte) ([]byte, error) {
	kind, val, rest, err := rlp.Split(buf)
	switch {
	case err != nil:
		return nil, err
	case kind == rlp.List:
		return buf[:len(buf)-len(rest)], nil
	case len(val) == 0:
		return nil, nil
	case len(val) == 32:
		enc, ok := byHash[string(val)]
		if !ok {
			return nil, fmt.Errorf("proof node %x absent", val)
		}
		return enc, nil
	default:
		return nil, fmt.Errorf("invalid child reference of %d bytes", len(val))
	}
}
