// Copyright 2019 The go-ethereum Authors
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
	"errors"
	"fmt"
	"hash"

	"golang.org/x/crypto/sha3"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/pool"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/trie/rlphacks"
)

type hasher struct {
	sha                  keccakState
	valueNodesRlpEncoded bool
	buffers              [1024 * 1024]byte
	prefixBuf            [8]byte
	bw                   *ByteArrayWriter
	callback             func(common.Hash, node)
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
	h.callback = nil
	select {
	case hasherPool <- h:
	default:
		fmt.Printf("Allowing hasher to be garbage collected, pool is full\n")
	}
}

// hash calculates node's RLP for hashing
// and stores the RLP if len(RLP) < 32 and not force,
// otherwise it stores hash(RLP).
// It also updates node's ref with that value.
func (h *hasher) hash(n node, force bool, storeTo []byte) (int, error) {
	return h.hashInternal(n, force, storeTo, 0)
}

// hashInternal calculates node's RLP for hashing
// and stores the RLP if len(RLP) < 32 and not force,
// otherwise it stores hash(RLP).
// It also updates node's ref with that value.
func (h *hasher) hashInternal(n node, force bool, storeTo []byte, bufOffset int) (int, error) {
	if hn, ok := n.(hashNode); ok {
		copy(storeTo, hn)
		return common.HashLength, nil
	}
	if len(n.reference()) > 0 {
		copy(storeTo, n.reference())
		return len(n.reference()), nil
	}
	// Trie not processed yet or needs storage, walk the children
	nodeRlp, err := h.hashChildren(n, bufOffset)
	if err != nil {
		return 0, err
	}

	refLen, err := h.nodeRef(nodeRlp, force, storeTo)
	if err != nil {
		return 0, err
	}

	switch n := n.(type) {
	case *shortNode:
		copy(n.ref.data[:], storeTo)
		n.ref.len = byte(refLen)
	case *accountNode:
		n.rootCorrect = true
	case *duoNode:
		copy(n.ref.data[:], storeTo)
		n.ref.len = byte(refLen)
	case *fullNode:
		copy(n.ref.data[:], storeTo)
		n.ref.len = byte(refLen)
	}

	if h.callback != nil && len(n.reference()) == common.HashLength {
		var hash common.Hash
		copy(hash[:], storeTo)
		h.callback(hash, n)
	}

	return refLen, nil
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

// hashChildren replaces the children of a node with their hashes
// if the RLP-encoded size of the child is >= 32,
// returning node's RLP with the child hashes cached in.
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
	acRlp := encodedAccount.Bytes()

	enc := rlphacks.RlpEncodedBytes(acRlp)
	h.bw.Setup(buffer, pos)

	if err := enc.ToDoubleRLP(h.bw, h.prefixBuf[:]); err != nil {
		return 0, err
	}

	return enc.DoubleRLPLen(), nil
}

// nodeRef writes either node's RLP (if less than 32 bytes) or its hash
// to storeTo and returns the size of the reference written.
// force enforces the hashing even for short RLPs.
func (h *hasher) nodeRef(nodeRlp []byte, force bool, storeTo []byte) (int, error) {
	if nodeRlp == nil {
		copy(storeTo, emptyHash[:])
		return 32, nil
	}
	if len(nodeRlp) < 32 && !force {
		copy(storeTo, nodeRlp)
		return len(nodeRlp), nil
	}
	h.sha.Reset()
	if _, err := h.sha.Write(nodeRlp); err != nil {
		return 0, err
	}

	// Only squeeze first 32 bytes
	if _, err := h.sha.Read(storeTo[:32]); err != nil {
		return 0, err
	}

	return 32, nil
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
