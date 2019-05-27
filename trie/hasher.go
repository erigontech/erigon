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
	"fmt"
	"hash"

	"github.com/ledgerwatch/turbo-geth/common/pool"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"golang.org/x/crypto/sha3"
)

type hasher struct {
	sha           keccakState
	encodeToBytes bool
	buffers       [1024 * 1024]byte
}

// keccakState wraps sha3.state. In addition to the usual hash methods, it also supports
// Read to get a variable amount of data from the hash state. Read is faster than Sum
// because it doesn't copy the internal state, but also modifies the internal state.
type keccakState interface {
	hash.Hash
	Read([]byte) (int, error)
}

// hashers live in a global db.
var hasherPool = make(chan *hasher, 128)

func newHasher(encodeToBytes bool) *hasher {
	var h *hasher
	select {
	case h = <-hasherPool:
	default:
		h = &hasher{sha: sha3.NewLegacyKeccak256().(keccakState)}
	}
	h.encodeToBytes = encodeToBytes
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
func (h *hasher) hash(n node, force bool, storeTo []byte) int {
	//n.makedirty()
	hh := h.hashInternal(n, force, storeTo, 0)
	return hh
}

// hash collapses a node down into a hash node, also returning a copy of the
// original node initialized with the computed hash to replace the original one.
func (h *hasher) hashInternal(n node, force bool, storeTo []byte, bufOffset int) int {
	if hn, ok := n.(hashNode); ok {
		copy(storeTo, hn)
		return 32
	}
	if !n.dirty() {
		copy(storeTo, n.hash())
		return 32
	}
	// Trie not processed yet or needs storage, walk the children
	children := h.hashChildren(n, bufOffset)
	hashLen := h.store(children, force, storeTo)
	if hashLen == 32 {
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
	return hashLen
}

func finishRLP(buffer []byte, pos int) []byte {
	serLength := pos - 4
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

func generateByteArrayLen(buffer []byte, pos int, l int) int {
	if l < 56 {
		buffer[pos] = byte(128 + l)
		pos++
	} else if l < 256 {
		// len(vn) can be encoded as 1 byte
		buffer[pos] = byte(183 + 1)
		pos++
		buffer[pos] = byte(l)
		pos++
	} else if l < 65536 {
		// len(vn) is encoded as two bytes
		buffer[pos] = byte(183 + 2)
		pos++
		buffer[pos] = byte(l >> 8)
		pos++
		buffer[pos] = byte(l & 255)
		pos++
	} else {
		// len(vn) is encoded as three bytes
		buffer[pos] = byte(183 + 3)
		pos++
		buffer[pos] = byte(l >> 16)
		pos++
		buffer[pos] = byte((l >> 8) & 255)
		pos++
		buffer[pos] = byte(l & 255)
		pos++
	}
	return pos
}

func generateByteArrayLenDouble(buffer []byte, pos int, l int) int {
	if l < 55 {
		// After first wrapping, the length will be l + 1 < 56
		buffer[pos] = byte(128 + l + 1)
		pos++
		buffer[pos] = byte(128 + l)
		pos++
	} else if l < 254 {
		// After first wrapping, the length will be l + 2 < 256
		buffer[pos] = byte(183 + 1)
		pos++
		buffer[pos] = byte(l + 2)
		pos++
		buffer[pos] = byte(183 + 1)
		pos++
		buffer[pos] = byte(l)
		pos++
	} else if l < 256 {
		// First wrapping is 2 bytes, second wrapping 3 bytes
		buffer[pos] = byte(183 + 2)
		pos++
		buffer[pos] = byte((l + 2) >> 8)
		pos++
		buffer[pos] = byte((l + 2) & 255)
		pos++
		buffer[pos] = byte(183 + 1)
		pos++
		buffer[pos] = byte(l)
		pos++
	} else if l < 65534 {
		// Both wrappings are 3 bytes
		buffer[pos] = byte(183 + 2)
		pos++
		buffer[pos] = byte((l + 3) >> 8)
		pos++
		buffer[pos] = byte((l + 3) & 255)
		pos++
		buffer[pos] = byte(183 + 2)
		pos++
		buffer[pos] = byte(l >> 8)
		pos++
		buffer[pos] = byte(l & 255)
		pos++
	} else if l < 65536 {
		// First wrapping is 3 bytes, second wrapping is 4 bytes
		buffer[pos] = byte(183 + 3)
		pos++
		buffer[pos] = byte((l + 3) >> 16)
		pos++
		buffer[pos] = byte(((l + 3) >> 8) & 255)
		pos++
		buffer[pos] = byte((l + 3) & 255)
		pos++
		buffer[pos] = byte(183 + 2)
		pos++
		buffer[pos] = byte((l >> 8) & 255)
		pos++
		buffer[pos] = byte(l & 255)
		pos++
	} else {
		// Both wrappings are 4 bytes
		buffer[pos] = byte(183 + 3)
		pos++
		buffer[pos] = byte((l + 4) >> 16)
		pos++
		buffer[pos] = byte(((l + 4) >> 8) & 255)
		pos++
		buffer[pos] = byte((l + 4) & 255)
		pos++
		buffer[pos] = byte(183 + 3)
		pos++
		buffer[pos] = byte(l >> 16)
		pos++
		buffer[pos] = byte((l >> 8) & 255)
		pos++
		buffer[pos] = byte(l & 255)
		pos++
	}
	return pos
}

// hashChildren replaces the children of a node with their hashes if the encoded
// size of the child is larger than a hash, returning the collapsed node as well
// as a replacement for the original node with the child hashes cached in.
// DESCRIBED: docs/programmers_guide/guide.md#hexary-radix-patricia-tree
func (h *hasher) hashChildren(original node, bufOffset int) []byte {
	buffer := h.buffers[bufOffset:]
	pos := 4
	switch n := original.(type) {
	case *shortNode:
		// Starting at position 3, to leave space for len prefix
		// Encode key
		compactKey := hexToCompact(n.Key)
		if len(compactKey) == 1 && compactKey[0] < 128 {
			buffer[pos] = compactKey[0]
			pos++
		} else {
			buffer[pos] = byte(128 + len(compactKey))
			pos++
			copy(buffer[pos:], compactKey)
			pos += len(compactKey)
		}
		// Encode value
		if vn, ok := n.Val.(valueNode); ok {
			if len(vn) == 1 && vn[0] < 128 {
				buffer[pos] = vn[0]
				pos++
			} else {
				if h.encodeToBytes {
					// Wrapping into another byte array
					pos = generateByteArrayLenDouble(buffer, pos, len(vn))
				} else {
					pos = generateByteArrayLen(buffer, pos, len(vn))
				}
				copy(buffer[pos:], vn)
				pos += len(vn)
			}
		} else if ac, ok := n.Val.(*accountNode); ok {
			// Hashing the storage trie if necessary
			if ac.storage == nil {
				ac.Root = EmptyRoot
			} else {
				h.hashInternal(ac.storage, true, ac.Root[:], bufOffset+pos)
			}

			encodingLen := ac.EncodingLengthForHashing()
			pos = generateByteArrayLen(buffer, pos, int(encodingLen))
			ac.EncodeForHashing(buffer[pos:])
			pos += int(encodingLen)
		} else {
			if n.Val == nil {
				// empty byte array
				buffer[pos] = byte(128)
				pos++
			} else {
				// Reserve one byte for length
				hashLen := h.hashInternal(n.Val, false, buffer[pos+1:], bufOffset+pos+1)
				if hashLen == 32 {
					buffer[pos] = byte(128 + 32)
					pos += 33
				} else {
					// Shift one byte backwards, because it is not treated as a byte array but embedded RLP
					copy(buffer[pos:pos+hashLen], buffer[pos+1:])
					pos += hashLen
				}
			}
		}
		return finishRLP(buffer, pos)

	case *duoNode:
		i1, i2 := n.childrenIdx()
		for i := 0; i < 17; i++ {
			if i == int(i1) {
				if n.child1 == nil {
					// empty byte array
					buffer[pos] = byte(128)
					pos++
				} else {
					// Reserve one byte for length
					hashLen := h.hashInternal(n.child1, false, buffer[pos+1:], bufOffset+pos+1)
					if hashLen == 32 {
						buffer[pos] = byte(128 + 32)
						pos += 33
					} else {
						// Shift one byte backwards, because it is not treated as a byte array but embedded RLP
						copy(buffer[pos:pos+hashLen], buffer[pos+1:])
						pos += hashLen
					}
				}
			} else if i == int(i2) {
				if n.child2 == nil {
					// empty byte array
					buffer[pos] = byte(128)
					pos++
				} else {
					// Reserve one byte for length
					hashLen := h.hashInternal(n.child2, false, buffer[pos+1:], bufOffset+pos+1)
					if hashLen == 32 {
						buffer[pos] = byte(128 + 32)
						pos += 33
					} else {
						// Shift one byte backwards, because it is not treated as a byte array but embedded RLP
						copy(buffer[pos:pos+hashLen], buffer[pos+1:])
						pos += hashLen
					}
				}
			} else {
				// empty byte array
				buffer[pos] = byte(128)
				pos++
			}
		}
		return finishRLP(buffer, pos)

	case *fullNode:
		// Hash the full node's children, caching the newly hashed subtrees
		for _, child := range n.Children[:16] {
			if child == nil {
				// empty byte array
				buffer[pos] = byte(128)
				pos++
			} else {
				// Reserve one byte for length
				hashLen := h.hashInternal(child, false, buffer[pos+1:], bufOffset+pos+1)
				if hashLen == 32 {
					buffer[pos] = byte(128 + 32)
					pos += 33
				} else {
					// Shift one byte backwards, because it is not treated as a byte array but embedded RLP
					copy(buffer[pos:pos+hashLen], buffer[pos+1:])
					pos += hashLen
				}
			}
		}
		var enc []byte
		switch n := n.Children[16].(type) {
		case *accountNode:
			encodedAccount := pool.GetBuffer(n.EncodingLengthForHashing())
			n.EncodeForHashing(encodedAccount.B)
			enc = encodedAccount.Bytes()
			pool.PutBuffer(encodedAccount)
		case valueNode:
			enc = n
		case nil:
		//	skip
		default:
			//	skip
		}

		if enc == nil {
			buffer[pos] = byte(128)
			pos++
		} else if len(enc) == 1 && enc[0] < 128 {
			buffer[pos] = enc[0]
			pos++
		} else {
			pos = generateByteArrayLen(buffer, pos, len(enc))
			copy(buffer[pos:], enc)
			pos += len(enc)
		}
		return finishRLP(buffer, pos)

	case valueNode:
		if len(n) == 1 && n[0] < 128 {
			buffer[pos] = n[0]
			pos++
		} else {
			if h.encodeToBytes {
				// Wrapping into another byte array
				pos = generateByteArrayLen(buffer, pos, len(n))
			}
			copy(buffer[pos:], n)
			pos += len(n)
		}
		return buffer[4:pos]

	case *accountNode:
		encodedAccount := pool.GetBuffer(n.EncodingLengthForHashing())
		n.EncodeForHashing(encodedAccount.B)
		enc := encodedAccount.Bytes()
		pool.PutBuffer(encodedAccount)
		if len(enc) == 1 && enc[0] < 128 {
			buffer[pos] = enc[0]
			pos++
		} else {
			if h.encodeToBytes {
				// Wrapping into another byte array
				pos = generateByteArrayLen(buffer, pos, len(enc))
			}
			copy(buffer[pos:], enc)
			pos += len(enc)
		}
		return buffer[4:pos]

	case hashNode:
		panic("hashNode")
	}
	return nil
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
