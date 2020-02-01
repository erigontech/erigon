// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package trie

import (
	"bytes"
	"errors"
	"fmt"
	"hash"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/pool"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/trie/rlphacks"
	"golang.org/x/crypto/sha3"
)

type hasher struct {
	sha                  keccakState
	valueNodesRlpEncoded bool
	buffers              [1024 * 1024]byte
	prefixBuf            [8]byte
	bw                   *ByteArrayWriter
}

const rlpPrefixLength = 4

// keccakState wraps sha3.state. In addition to the usual hash methods, it also supports
// Read to get a variable amount of data from the hash state. Read is faster than Sum
// because it doesn't copy the internal state, but also modifies the internal state.
type keccakState interface {
	hash.Hash
	Read([]byte) (int, error)
}

// hashers live in a global db.
var hasherPool = make(chan *hasher, 128)

func newHasher(valueNodesRlpEncoded bool) *hasher {
	var h *hasher
	select {
	case h = <-hasherPool:
	default:
		h = &hasher{
			sha: sha3.NewLegacyKeccak256().(keccakState),
			bw:  &ByteArrayWriter{},
		}
	}
	h.valueNodesRlpEncoded = valueNodesRlpEncoded
	return h
}

func returnHasherToPool(h *hasher) {
	select {
	case hasherPool <- h:
	default:
		fmt.Printf("Allowing hasher to be garbage collected, pool is full\n")
	}
}

// hash collapses a node down into a hash node, also returning a copy of the
// original node initialized with the computed hash to replace the original one.
func (h *hasher) hash(n node, force bool, storeTo []byte) (int, error) {
	//n.makedirty()
	return h.hashInternal(n, force, storeTo, 0)
}

// hash collapses a node down into a hash node, also returning a copy of the
// original node initialized with the computed hash to replace the original one.
func (h *hasher) hashInternal(n node, force bool, storeTo []byte, bufOffset int) (int, error) {
	if hn, ok := n.(hashNode); ok {
		copy(storeTo, hn)
		return common.HashLength, nil
	}
	if !n.dirty() {
		copy(storeTo, n.hash())
		return common.HashLength, nil
	}
	// Trie not processed yet or needs storage, walk the children
	children, err := h.hashChildren(n, bufOffset)
	if err != nil {
		return 0, err
	}

	hashLen := h.store(children, force, storeTo)

	if hashLen == common.HashLength {
		switch n := n.(type) {
		case *accountNode:
			n.hashCorrect = true
		case *duoNode:
			copy(n.flags.hash[:], storeTo)
			n.flags.dirty = false
		case *fullNode:
			copy(n.flags.hash[:], storeTo)
			n.flags.dirty = false
		}
	}
	return hashLen, nil
}

func writeRlpPrefix(buffer []byte, pos int) []byte {
	serLength := pos - rlpPrefixLength
	if serLength < 56 {
		buffer[3] = byte(192 + serLength)
		return buffer[3:pos]
	} else if serLength < 256 {
		// serLength can be encoded as 1 byte
		buffer[3] = byte(serLength)
		buffer[2] = byte(247 + 1)
		return buffer[2:pos]
	} else if serLength < 65536 {
		buffer[3] = byte(serLength & 255)
		buffer[2] = byte(serLength >> 8)
		buffer[1] = byte(247 + 2)
		return buffer[1:pos]
	} else {
		buffer[3] = byte(serLength & 255)
		buffer[2] = byte((serLength >> 8) & 255)
		buffer[1] = byte(serLength >> 16)
		buffer[0] = byte(247 + 3)
		return buffer[0:pos]
	}
}

// hashChildren replaces the children of a node with their hashes if the encoded
// size of the child is larger than a hash, returning the collapsed node as well
// as a replacement for the original node with the child hashes cached in.
// DESCRIBED: docs/programmers_guide/guide.md#hexary-radix-patricia-tree
func (h *hasher) hashChildren(original node, bufOffset int) ([]byte, error) {
	buffer := h.buffers[bufOffset:]
	pos := rlpPrefixLength

	switch n := original.(type) {
	case *shortNode:
		// Starting at position 3, to leave space for len prefix
		// Encode key
		compactKey := hexToCompact(n.Key)
		h.bw.Setup(buffer, pos)
		written, err := rlphacks.EncodeByteArrayAsRlp(compactKey, h.bw, h.prefixBuf[:])
		if err != nil {
			return nil, err
		}
		pos += written

		// Encode value
		if vn, ok := n.Val.(valueNode); ok {
			written, err := h.valueNodeToBuffer(vn, buffer, pos)
			if err != nil {
				return nil, err
			}
			pos += written

		} else if ac, ok := n.Val.(*accountNode); ok {
			// Hashing the storage trie if necessary
			if ac.storage == nil {
				ac.Root = EmptyRoot
			} else {
				_, err := h.hashInternal(ac.storage, true, ac.Root[:], bufOffset+pos)
				if err != nil {
					return nil, err
				}
			}

			written, err := h.accountNodeToBuffer(ac, buffer, pos)
			if err != nil {
				return nil, err
			}
			pos += written
		} else {
			written, err := h.hashChild(n.Val, buffer, pos, bufOffset)
			if err != nil {
				return nil, err
			}
			pos += written
		}
		return writeRlpPrefix(buffer, pos), nil

	case *duoNode:
		i1, i2 := n.childrenIdx()
		for i := 0; i < 17; i++ {
			var child node

			if i == int(i1) {
				child = n.child1
			} else if i == int(i2) {
				child = n.child2
			}

			if child != nil {
				written, err := h.hashChild(child, buffer, pos, bufOffset)
				if err != nil {
					return nil, err
				}
				pos += written
			} else {
				pos += writeEmptyByteArray(buffer, pos)
			}
		}
		return writeRlpPrefix(buffer, pos), nil

	case *fullNode:
		// Hash the full node's children, caching the newly hashed subtrees
		for _, child := range n.Children[:16] {
			written, err := h.hashChild(child, buffer, pos, bufOffset)
			if err != nil {
				return nil, err
			}
			pos += written
		}
		switch n := n.Children[16].(type) {
		case *accountNode:
			written, err := h.accountNodeToBuffer(n, buffer, pos)
			if err != nil {
				return nil, err
			}
			pos += written

		case valueNode:
			written, err := h.valueNodeToBuffer(n, buffer, pos)
			if err != nil {
				return nil, err
			}
			pos += written

		case nil:
			pos += writeEmptyByteArray(buffer, pos)

		default:
			pos += writeEmptyByteArray(buffer, pos)
		}

		return writeRlpPrefix(buffer, pos), nil

	case valueNode:
		written, err := h.valueNodeToBuffer(n, buffer, pos)
		if err != nil {
			return nil, err
		}
		pos += written

		return buffer[rlpPrefixLength:pos], nil

	case *accountNode:
		// we don't do double RLP here, so `accountNodeToBuffer` is not applicable
		encodedAccount := pool.GetBuffer(n.EncodingLengthForHashing())

		n.EncodeForHashing(encodedAccount.B)
		pos += copy(buffer[pos:], encodedAccount.Bytes())

		pool.PutBuffer(encodedAccount)

		return buffer[rlpPrefixLength:pos], nil

	case hashNode:
		return nil, errors.New("hasher#hashChildren: met unexpected hash node")
	}

	return nil, nil
}

func (h *hasher) valueNodeToBuffer(vn valueNode, buffer []byte, pos int) (int, error) {
	h.bw.Setup(buffer, pos)

	var val rlphacks.RlpSerializable

	if h.valueNodesRlpEncoded {
		val = rlphacks.RlpEncodedBytes(vn)
	} else {
		val = rlphacks.RlpSerializableBytes(vn)
	}

	if err := val.ToDoubleRLP(h.bw, h.prefixBuf[:]); err != nil {
		return 0, err
	}
	return val.DoubleRLPLen(), nil
}

func (h *hasher) accountNodeToBuffer(ac *accountNode, buffer []byte, pos int) (int, error) {
	encodedAccount := pool.GetBuffer(ac.EncodingLengthForHashing())
	defer pool.PutBuffer(encodedAccount)

	ac.EncodeForHashing(encodedAccount.B)
	enc := rlphacks.RlpEncodedBytes(encodedAccount.Bytes())
	h.bw.Setup(buffer, pos)

	if err := enc.ToDoubleRLP(h.bw, h.prefixBuf[:]); err != nil {
		return 0, err
	}

	return enc.DoubleRLPLen(), nil
}

func EncodeAsValue(data []byte) ([]byte, error) {
	tmp := new(bytes.Buffer)
	err := rlp.Encode(tmp, valueNode(data))
	if err != nil {
		return nil, err
	}
	return tmp.Bytes(), nil
}

// store hashes the node n and if we have a storage layer specified, it writes
// the key/value pair to it and tracks any node->child references as well as any
// node->external trie references.
func (h *hasher) store(children []byte, force bool, storeTo []byte) int {
	if children == nil {
		copy(storeTo, emptyHash[:])
		return 32
	}
	if len(children) < 32 && !force {
		copy(storeTo, children)
		return len(children)
	}
	h.sha.Reset()
	h.sha.Write(children)
	h.sha.Read(storeTo[:32]) // Only squize first 32 bytes
	return 32
}

func (h *hasher) makeHashNode(data []byte) hashNode {
	n := make(hashNode, h.sha.Size())
	h.sha.Reset()
	h.sha.Write(data)
	h.sha.Read(n)
	return n
}

func (h *hasher) hashChild(child node, buffer []byte, pos int, bufOffset int) (int, error) {
	if child == nil {
		return writeEmptyByteArray(buffer, pos), nil
	}

	// Reserve one byte for length
	hashLen, err := h.hashInternal(child, false, buffer[pos+1:], bufOffset+pos+1)
	if err != nil {
		return 0, err
	}

	if hashLen == common.HashLength {
		buffer[pos] = byte(0x80 + common.HashLength)
		return common.HashLength + 1, nil
	}

	// Shift one byte backwards, because it is not treated as a byte array but embedded RLP
	copy(buffer[pos:pos+hashLen], buffer[pos+1:])
	return hashLen, nil
}

func writeEmptyByteArray(buffer []byte, pos int) int {
	buffer[pos] = rlp.EmptyStringCode
	return 1
}
