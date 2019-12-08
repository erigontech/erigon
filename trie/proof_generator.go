// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty off
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Generation of block proofs for stateless clients

package trie

import (
	"bytes"
	"fmt"
	"io"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ugorji/go/codec"
)

type WitnessTapeStats map[string]int

// TapeBuilder stores the sequence of values that is getting serialised using CBOR into a byte buffer
type TapeBuilder struct {
	buffer  bytes.Buffer     // Byte buffer where the CBOR-encoded values end up being written
	handle  codec.CborHandle // Object used to control the behavior of CBOR encoding
	encoder *codec.Encoder   // Values are supplied to this object (via its Encode function)
}

// init allocates a new encoder, binding it to the buffer and the handle
func (t *TapeBuilder) init() {
	t.encoder = codec.NewEncoder(&t.buffer, &t.handle)
}

const (
	KeyTape       = "keys"
	ValueTape     = "values"
	NonceTape     = "nonces"
	BalanceTape   = "balances"
	HashesTape    = "hashes"
	CodesTape     = "codes"
	StructureTape = "structure"
)

// BlockWitnessBuilder accumulates data that can later be turned into a serialised
// version of the block witness
// All buffers are streams of CBOR-encoded items (not a CBOR array, but individual items back-to-back)
// `Keys` are binary strings
// `Values` are binary strings
// `Nonces` are uint64 integers
// `Balances` instances of big.Int
// `Hashes` are binary strings, all of size 32
// `Codes` are binary strings
// `Structure` are integers (for opcodes themselves), potentially followed by binary strings (key for EXTENSION) or
// integers (bitmaps for BRANCH or length of LEAF or number of hashes for HASH)
type BlockWitnessBuilder struct {
	Keys      TapeBuilder // Sequence of keys that are consumed by LEAF, LEAFHASH, ACCOUNTLEAF, and ACCOUNTLEAFHASH opcodes
	Values    TapeBuilder // Sequence of values that are consumed by LEAF, and LEAFHASH opcodes
	Nonces    TapeBuilder // Sequence of nonces that are consumed by ACCOUNTLEAF or ACCOUNTLEAFHASH opcodes
	Balances  TapeBuilder // Sequence of balances that are consumed by ACCOUNTLEAF or ACCOUNTLEAFHASH opcodes
	Hashes    TapeBuilder // Sequence of hashes that are consumed by the HASH opcode
	Codes     TapeBuilder // Sequence of contract codes that are consumed by the CODE opcode
	Structure TapeBuilder // Sequence of opcodes and operands that define the structure of the witness
	trace     bool
}

// Instruction is "enum" type for defining the opcodes of the stack machine that reconstructs the structure of tries from Structure tape
type Instruction uint8

const (
	// OpLeaf consumes key from key tape, value from value tape, creates leaf node and pushes it onto the node stack, its hash onto the hash stack
	OpLeaf Instruction = iota
	// OpLeafHash consumes key from key tape, value from value tape, computes hash of would-be leaf node and pushes it onto the hash stack
	OpLeafHash
	// OpExtension pops a node from the node stack, constructs extension node from it and its operand's key, and pushes this extension node onto
	// the node stack, its hash onto the hash stack
	OpExtension
	// OpExtensionHash pops a hash from the hash stack, computes the hash of would-be extension node from it and its operand's key,
	// and pushes this hash onto the hash stack
	OpExtensionHash
	// OpBranch has operand, which is a bitset representing digits in the branch node. Pops the children nodes from the node stack (the number of
	// children is equal to the number of bits in the bitset), constructs branch node and pushes it onto the node stack, its hash onto the hash stack
	OpBranch
	// OpBranchHash has operand, which is a bitset representing digits in the branch node. Pops the children hashes from the hash stack (the number of
	// children hashes is equal to the number of bits in the bitset), computes the hash of would-be branch node and pushes that hash onto the hash stack
	OpBranchHash
	// OpHash consumes given (in the operant) number of hash from the hash tape, and pushes them onto the stack. The first item consumed ends up
	// the deepest on the stack, the last item consumed ends up on the top of the stack.
	OpHash
	// OpCode consumes bytecode item from the code tape, construct code node and pushes it onto the node stack, its hash onto the hash stack.
	OpCode
	// OpAccountLeaf consumes key from the key tape, and two values from the value tape, one for nonce, another for balance. It constructs
	// an account node (without any storage and code) and pushes it onto the node stack, its hash onto the hash stack.
	OpAccountLeaf
	// OpAccountLeafHash consumes key from the key tape, and two values from the value tape, one for nonce, another for balance.
	// It computes the hash of would-be account node (without any storage and code) and pushes it onto the hash stack.
	OpAccountLeafHash
	// OpEmptyRoot places nil onto the node stack, and empty root hash onto the hash stack.
	OpEmptyRoot
)

// NewBlockWitnessBuilder creates an initialised block witness builder ready for use
func NewBlockWitnessBuilder(trace bool) *BlockWitnessBuilder {
	var bwb BlockWitnessBuilder
	bwb.trace = trace
	bwb.Keys.init()
	bwb.Values.init()
	bwb.Nonces.init()
	bwb.Balances.init()
	bwb.Hashes.init()
	bwb.Codes.init()
	bwb.Structure.init()
	return &bwb
}

// keyValue supplies the next key for the key tape
func (bwb *BlockWitnessBuilder) supplyKey(key []byte) error {
	if err := bwb.Keys.encoder.Encode(key); err != nil {
		return err
	}
	return nil
}

func (bwb *BlockWitnessBuilder) supplyValue(value []byte) error {
	if err := bwb.Values.encoder.Encode(value); err != nil {
		return err
	}
	return nil
}

func (bwb *BlockWitnessBuilder) supplyNonce(nonce uint64) error {
	if err := bwb.Nonces.encoder.Encode(&nonce); err != nil {
		return err
	}
	return nil
}

// TODO [Alexey] utilise CBOR tag to make this value as bit integer rather than just a string of bytes
func (bwb *BlockWitnessBuilder) supplyBalance(balance *big.Int) error {
	var v = balance.Bytes()
	if err := bwb.Balances.encoder.Encode(v); err != nil {
		return err
	}
	return nil
}

func (bwb *BlockWitnessBuilder) supplyCode(code []byte) error {
	if err := bwb.Codes.encoder.Encode(code); err != nil {
		return err
	}
	return nil
}

func (bwb *BlockWitnessBuilder) supplyHash(hash common.Hash) error {
	if err := bwb.Hashes.encoder.Encode(hash[:]); err != nil {
		return err
	}
	return nil
}

func (bwb *BlockWitnessBuilder) leaf(length int) error {
	o := OpLeaf
	if err := bwb.Structure.encoder.Encode(&o); err != nil {
		return err
	}
	if err := bwb.Structure.encoder.Encode(&length); err != nil {
		return err
	}
	return nil
}

func (bwb *BlockWitnessBuilder) leafHash(length int) error {
	o := OpLeafHash
	if err := bwb.Structure.encoder.Encode(&o); err != nil {
		return err
	}
	if err := bwb.Structure.encoder.Encode(&length); err != nil {
		return err
	}
	return nil
}

func (bwb *BlockWitnessBuilder) extension(key []byte) error {
	o := OpExtension
	if err := bwb.Structure.encoder.Encode(&o); err != nil {
		return err
	}
	if err := bwb.Structure.encoder.Encode(key); err != nil {
		return err
	}
	return nil
}

func (bwb *BlockWitnessBuilder) extensionHash(key []byte) error {
	o := OpExtensionHash
	if err := bwb.Structure.encoder.Encode(&o); err != nil {
		return err
	}
	if err := bwb.Structure.encoder.Encode(key); err != nil {
		return err
	}
	return nil
}

func (bwb *BlockWitnessBuilder) branch(set uint32) error {
	o := OpBranch
	if err := bwb.Structure.encoder.Encode(&o); err != nil {
		return err
	}
	if err := bwb.Structure.encoder.Encode(&set); err != nil {
		return err
	}
	return nil
}

func (bwb *BlockWitnessBuilder) branchHash(set uint32) error {
	o := OpBranchHash
	if err := bwb.Structure.encoder.Encode(&o); err != nil {
		return err
	}
	if err := bwb.Structure.encoder.Encode(&set); err != nil {
		return err
	}
	return nil
}

func (bwb *BlockWitnessBuilder) hash(number int) error {
	o := OpHash
	if err := bwb.Structure.encoder.Encode(&o); err != nil {
		return err
	}
	if err := bwb.Structure.encoder.Encode(&number); err != nil {
		return err
	}
	return nil
}

func (bwb *BlockWitnessBuilder) code() error {
	o := OpCode
	if err := bwb.Structure.encoder.Encode(&o); err != nil {
		return err
	}
	return nil
}

func (bwb *BlockWitnessBuilder) accountLeaf(length int, fieldSet uint32) error {
	o := OpAccountLeaf
	if err := bwb.Structure.encoder.Encode(&o); err != nil {
		return err
	}
	if err := bwb.Structure.encoder.Encode(&length); err != nil {
		return err
	}
	if err := bwb.Structure.encoder.Encode(&fieldSet); err != nil {
		return err
	}
	return nil
}

func (bwb *BlockWitnessBuilder) accountLeafHash(length int, fieldSet uint32) error {
	o := OpAccountLeafHash
	if err := bwb.Structure.encoder.Encode(&o); err != nil {
		return err
	}
	if err := bwb.Structure.encoder.Encode(&length); err != nil {
		return err
	}
	if err := bwb.Structure.encoder.Encode(&fieldSet); err != nil {
		return err
	}
	return nil
}

func (bwb *BlockWitnessBuilder) emptyRoot() error {
	o := OpEmptyRoot
	if err := bwb.Structure.encoder.Encode(&o); err != nil {
		return err
	}
	return nil
}

// MakeBlockWitness constructs block witness from the given trie and the
// list of keys that need to be accessible in such witness
func (bwb *BlockWitnessBuilder) MakeBlockWitness(t *Trie, rs *ResolveSet, codeMap map[common.Hash][]byte) error {
	hr := newHasher(false)
	defer returnHasherToPool(hr)
	return bwb.makeBlockWitness(t.root, []byte{}, rs, hr, true, codeMap, expandKeyHex)
}

func expandKeyHex(hex []byte, nibble byte) []byte {
	result := make([]byte, len(hex)+1)
	copy(result, hex)
	result[len(hex)] = nibble
	return result
}

type expandKeyFunc func([]byte, byte) []byte

func (bwb *BlockWitnessBuilder) makeBlockWitness(
	nd node, hex []byte, rs *ResolveSet, hr *hasher, force bool,
	codeMap map[common.Hash][]byte,
	expandKeyFunc expandKeyFunc,
) error {

	switch n := nd.(type) {
	case nil:
		return nil
	case valueNode:
		return bwb.supplyValue(n)
	case *shortNode:
		h := n.Key
		// Remove terminator
		if h[len(h)-1] == 16 {
			h = h[:len(h)-1]
		}
		hexVal := concat(hex, h...)
		if err := bwb.makeBlockWitness(n.Val, hexVal, rs, hr, false, codeMap, expandKeyFunc); err != nil {
			return err
		}
		switch v := n.Val.(type) {
		case valueNode:
			// Recursive invocation would have supplied the value
			if err := bwb.supplyKey(n.Key); err != nil {
				return err
			}
			if err := bwb.leaf(len(n.Key)); err != nil {
				return err
			}
		case *accountNode:
			if err := bwb.supplyKey(n.Key); err != nil {
				return err
			}
			if v.IsEmptyRoot() && v.IsEmptyCodeHash() {
				if err := bwb.accountLeaf(len(n.Key), 3); err != nil {
					return err
				}
			} else {
				if err := bwb.accountLeaf(len(n.Key), 15); err != nil {
					return err
				}
			}
		default:
			if err := bwb.extension(n.Key); err != nil {
				return err
			}
		}
		return nil
	case *duoNode:
		hashOnly := rs.HashOnly(hex) // Save this because rs can move on to other keys during the recursive invocation
		if hashOnly {
			var hn common.Hash
			hr.hash(n, force, hn[:])
			if err := bwb.supplyHash(hn); err != nil {
				return err
			}
			return bwb.hash(1)
		}
		i1, i2 := n.childrenIdx()

		if err := bwb.makeBlockWitness(n.child1, expandKeyFunc(hex, i1), rs, hr, false, codeMap, expandKeyFunc); err != nil {
			return err
		}
		if err := bwb.makeBlockWitness(n.child2, expandKeyFunc(hex, i2), rs, hr, false, codeMap, expandKeyFunc); err != nil {
			return err
		}
		return bwb.branch(n.mask)
	case *fullNode:
		hashOnly := rs.HashOnly(hex) // Save this because rs can move on to other keys during the recursive invocation
		if hashOnly {
			var hn common.Hash
			hr.hash(n, len(hex) == 0, hn[:])
			if err := bwb.supplyHash(hn); err != nil {
				return err
			}
			return bwb.hash(1)
		}
		var set uint32
		for i, child := range n.Children {
			if child != nil {
				if err := bwb.makeBlockWitness(child, expandKeyFunc(hex, byte(i)), rs, hr, false, codeMap, expandKeyFunc); err != nil {
					return err
				}
				set |= (uint32(1) << uint(i))
			}
		}
		return bwb.branch(set)
	case *accountNode:
		if !n.IsEmptyRoot() || !n.IsEmptyCodeHash() {
			code, ok := codeMap[n.CodeHash]
			if ok {
				if err := bwb.supplyCode(code); err != nil {
					return err
				}
				if err := bwb.code(); err != nil {
					return err
				}
			} else {
				if err := bwb.supplyHash(n.CodeHash); err != nil {
					return err
				}
				if err := bwb.hash(1); err != nil {
					return err
				}
			}
			if n.storage == nil {
				if err := bwb.emptyRoot(); err != nil {
					return err
				}
			} else {
				// Here we substitute rs parameter for storageRs, because it needs to become the default
				if err := bwb.makeBlockWitness(n.storage, hex, rs, hr, true, codeMap, expandKeyFunc); err != nil {
					return err
				}
			}
		}
		if err := bwb.supplyNonce(n.Nonce); err != nil {
			return err
		}
		if err := bwb.supplyBalance(&n.Balance); err != nil {
			return err
		}
		return nil
	case hashNode:
		hashOnly := rs.HashOnly(hex)
		if !hashOnly {
			if c := rs.Current(); len(c) == len(hex)+1 && c[len(c)-1] == 16 {
				hashOnly = true
			}
		}
		if hashOnly {
			var hn common.Hash
			copy(hn[:], []byte(n))
			if err := bwb.supplyHash(hn); err != nil {
				return err
			}
			return bwb.hash(1)
		}
		return fmt.Errorf("unexpected hashNode: %s, at hex: %x (%d)", n, hex, len(hex))
	default:
		return fmt.Errorf("unexpected node: %T", nd)
	}
}

// WriteTo creates serialised representation of the block witness
// and writes it into the given writer
// returns stats (tape lengths) and stuff
func (bwb *BlockWitnessBuilder) WriteTo(w io.Writer) (WitnessTapeStats, error) {
	// Calculate the lengths of all the tapes and write them as an array
	var lens = map[string]int{
		KeyTape:       bwb.Keys.buffer.Len(),
		ValueTape:     bwb.Values.buffer.Len(),
		NonceTape:     bwb.Nonces.buffer.Len(),
		BalanceTape:   bwb.Balances.buffer.Len(),
		HashesTape:    bwb.Hashes.buffer.Len(),
		CodesTape:     bwb.Codes.buffer.Len(),
		StructureTape: bwb.Structure.buffer.Len(),
	}
	var handle codec.CborHandle
	handle.EncodeOptions.Canonical = true
	encoder := codec.NewEncoder(w, &handle)
	if err := encoder.Encode(&lens); err != nil {
		return nil, err
	}
	if _, err := bwb.Keys.buffer.WriteTo(w); err != nil {
		return nil, err
	}
	if _, err := bwb.Values.buffer.WriteTo(w); err != nil {
		return nil, err
	}
	if _, err := bwb.Nonces.buffer.WriteTo(w); err != nil {
		return nil, err
	}
	if _, err := bwb.Balances.buffer.WriteTo(w); err != nil {
		return nil, err
	}
	if _, err := bwb.Hashes.buffer.WriteTo(w); err != nil {
		return nil, err
	}
	if _, err := bwb.Codes.buffer.WriteTo(w); err != nil {
		return nil, err
	}
	if _, err := bwb.Structure.buffer.WriteTo(w); err != nil {
		return nil, err
	}

	return WitnessTapeStats(lens), nil
}

// CborBytesTape implements BytesTape and takes values from CBOR-encoded slice of bytes
type CborBytesTape struct {
	decoder *codec.Decoder // Values are decoded by this object
}

// NewCborBytesTape creates new tape
func NewCborBytesTape(in []byte) *CborBytesTape {
	var handle codec.CborHandle // Object used to control the behavior of CBOR decoding
	return &CborBytesTape{decoder: codec.NewDecoderBytes(in, &handle)}
}

// Next belongs to the BytesTape interface, and decodes the next byte slice
func (cbt *CborBytesTape) Next() ([]byte, error) {
	var b []byte
	if err := cbt.decoder.Decode(&b); err != nil {
		return nil, err
	}
	return b, nil
}

// CborUint64Tape implements Uint64Tape and takes values from CBOR-encoded integers
type CborUint64Tape struct {
	decoder *codec.Decoder // Values are decoded by this object
}

// NewCborUint64Tape creates new tape
func NewCborUint64Tape(in []byte) *CborUint64Tape {
	var handle codec.CborHandle // Object used to control the behavior of CBOR decoding
	return &CborUint64Tape{decoder: codec.NewDecoderBytes(in, &handle)}
}

// Next belongs to the Uint64Tape interface, and decodes the next integer
func (cut *CborUint64Tape) Next() (uint64, error) {
	var u uint64
	if err := cut.decoder.Decode(&u); err != nil {
		return 0, err
	}
	return u, nil
}

// CborBigIntTape implements BigIntTape and takes values from CBOR-encoded *big.Int
type CborBigIntTape struct {
	decoder *codec.Decoder // Values are decoded by this object
}

// NewCborBigIntTape creates new tape
func NewCborBigIntTape(in []byte) *CborBigIntTape {
	var handle codec.CborHandle // Object used to control the behavior of CBOR decoding
	return &CborBigIntTape{decoder: codec.NewDecoderBytes(in, &handle)}
}

// Next belongs to the Uint64Tape interface, and decodes the next big.Int
func (cut *CborBigIntTape) Next() (*big.Int, error) {
	var b []byte
	if err := cut.decoder.Decode(&b); err != nil {
		return nil, err
	}
	return big.NewInt(0).SetBytes(b), nil
}

// CborHashTape implements BytesTape and takes values from CBOR-encoded hashes common.Hash
type CborHashTape struct {
	decoder *codec.Decoder // Values are decoded by this object
}

// NewCborHashTape creates new tape
func NewCborHashTape(in []byte) *CborHashTape {
	var handle codec.CborHandle // Object used to control the behavior of CBOR decoding
	return &CborHashTape{decoder: codec.NewDecoderBytes(in, &handle)}
}

// Next belongs to the BytesTape interface, and decodes the next byte slice
func (cht *CborHashTape) Next() (common.Hash, error) {
	var hash common.Hash
	var b []byte
	if err := cht.decoder.Decode(&b); err != nil {
		return common.Hash{}, err
	}
	copy(hash[:], b)
	return hash, nil
}

// BlockWitnessToTrie creates trie and code map, given serialised representation of block witness
func BlockWitnessToTrie(bw []byte, trace bool) (*Trie, map[common.Hash][]byte, error) {
	return BlockWitnessToTrieBin(bw, trace, false)
}

// BlockWitnessToTrie creates trie and code map, given serialised representation of block witness
func BlockWitnessToTrieBin(bw []byte, trace bool, isBinary bool) (*Trie, map[common.Hash][]byte, error) {
	codeMap := make(map[common.Hash][]byte)
	var lens map[string]int
	var handle codec.CborHandle
	decoder := codec.NewDecoderBytes(bw, &handle)
	if err := decoder.Decode(&lens); err != nil {
		return nil, nil, err
	}
	hb := NewHashBuilder(false)
	// It is important to read the tapes in the same order as they were written
	startOffset := decoder.NumBytesRead()
	endOffset := startOffset + lens[KeyTape]

	keyTape := NewCborBytesTape(bw[startOffset:endOffset])

	startOffset = endOffset
	endOffset = startOffset + lens[ValueTape]
	hb.SetValueTape(
		NewRlpSerializableBytesTape(
			NewCborBytesTape(bw[startOffset:endOffset])))

	startOffset = endOffset
	endOffset = startOffset + lens[NonceTape]
	hb.SetNonceTape(NewCborUint64Tape(bw[startOffset:endOffset]))
	startOffset = endOffset
	endOffset = startOffset + lens[BalanceTape]
	balanceTape := NewCborBigIntTape(bw[startOffset:endOffset])
	startOffset = endOffset
	endOffset = startOffset + lens[HashesTape]

	hashTape := NewCborHashTape(bw[startOffset:endOffset])
	startOffset = endOffset
	endOffset = startOffset + lens[CodesTape]

	codeTape := NewCborBytesTape(bw[startOffset:endOffset])
	startOffset = endOffset
	endOffset = startOffset + lens[StructureTape]
	structureB := bw[startOffset:endOffset]
	decoder.ResetBytes(structureB)
	for decoder.NumBytesRead() < len(structureB) {
		var opcode Instruction
		if err := decoder.Decode(&opcode); err != nil {
			return nil, nil, err
		}
		switch opcode {
		case OpLeaf:
			if trace {
				fmt.Printf("LEAF ")
			}
			var length int
			if err := decoder.Decode(&length); err != nil {
				return nil, nil, err
			}
			keyHex, err := keyTape.Next()
			if err != nil {
				return nil, nil, err
			}
			if err := hb.leaf(length, keyHex); err != nil {
				return nil, nil, err
			}
		case OpLeafHash:
			if trace {
				fmt.Printf("LEAFHASH ")
			}
			var length int
			if err := decoder.Decode(&length); err != nil {
				return nil, nil, err
			}
			keyHex, err := keyTape.Next()
			if err != nil {
				return nil, nil, err
			}
			if err := hb.leafHash(length, keyHex); err != nil {
				return nil, nil, err
			}
		case OpExtension:
			if trace {
				fmt.Printf("EXTENSION ")
			}
			var key []byte
			if err := decoder.Decode(&key); err != nil {
				return nil, nil, err
			}
			if err := hb.extension(key); err != nil {
				return nil, nil, err
			}
		case OpExtensionHash:
			if trace {
				fmt.Printf("EXTENSIONHASH ")
			}
			var key []byte
			if err := decoder.Decode(&key); err != nil {
				return nil, nil, err
			}
			if err := hb.extensionHash(key); err != nil {
				return nil, nil, err
			}
		case OpBranch:
			if trace {
				fmt.Printf("BRANCH ")
			}
			var set uint16
			if err := decoder.Decode(&set); err != nil {
				return nil, nil, err
			}
			if err := hb.branch(set); err != nil {
				return nil, nil, err
			}
		case OpBranchHash:
			if trace {
				fmt.Printf("BRANCHHASH ")
			}
			var set uint16
			if err := decoder.Decode(&set); err != nil {
				return nil, nil, err
			}
			if err := hb.branchHash(set); err != nil {
				return nil, nil, err
			}
		case OpHash:
			if trace {
				fmt.Printf("HASH ")
			}
			var number int
			if err := decoder.Decode(&number); err != nil {
				return nil, nil, err
			}
			hashes := make([]common.Hash, number)
			for i := range hashes {
				hash, err := hashTape.Next()
				if err != nil {
					return nil, nil, err
				}
				hashes[i] = hash
			}
			if err := hb.hash(hashes...); err != nil {
				return nil, nil, err
			}
		case OpCode:
			if trace {
				fmt.Printf("CODE ")
			}
			code, err := codeTape.Next()
			if err != nil {
				return nil, nil, err
			}
			if code, codeHash, err := hb.code(code); err == nil {
				codeMap[codeHash] = code
			} else {
				return nil, nil, err
			}
		case OpAccountLeaf:
			var length int
			var fieldSet uint32
			if err := decoder.Decode(&length); err != nil {
				return nil, nil, err
			}
			if err := decoder.Decode(&fieldSet); err != nil {
				return nil, nil, err
			}
			if trace {
				fmt.Printf("ACCOUNTLEAF(%b) ", fieldSet)
			}
			keyHex, err := keyTape.Next()
			if err != nil {
				return nil, nil, err
			}
			balance := big.NewInt(0)
			if fieldSet&uint32(2) != 0 {
				balance, err = balanceTape.Next()
				if err != nil {
					return nil, nil, err
				}
			}
			if err := hb.accountLeaf(length, keyHex, 0, balance, fieldSet); err != nil {
				return nil, nil, err
			}
		case OpAccountLeafHash:
			if trace {
				fmt.Printf("ACCOUNTLEAFHASH ")
			}
			var length int
			var fieldSet uint32
			if err := decoder.Decode(&length); err != nil {
				return nil, nil, err
			}
			if err := decoder.Decode(&fieldSet); err != nil {
				return nil, nil, err
			}
			keyHex, err := keyTape.Next()
			if err != nil {
				return nil, nil, err
			}

			balance := big.NewInt(0)
			if fieldSet&uint32(2) != 0 {
				balance, err = balanceTape.Next()
				if err != nil {
					return nil, nil, err
				}
			}
			if err := hb.accountLeafHash(length, keyHex, 0, balance, fieldSet); err != nil {
				return nil, nil, err
			}
		case OpEmptyRoot:
			if trace {
				fmt.Printf("EMPTYROOT ")
			}
			hb.emptyRoot()
		default:
			return nil, nil, fmt.Errorf("unknown opcode: %d", opcode)
		}
	}
	if trace {
		fmt.Printf("\n")
	}
	r := hb.root()
	var tr *Trie
	if isBinary {
		tr = NewBinary(hb.rootHash())
	} else {
		tr = New(hb.rootHash())
	}
	tr.root = r
	return tr, codeMap, nil
}

// ProofGenerator is the structure that accumulates the set of keys that were read or changes (touched) during
// the execution of a block. It also tracks the contract codes that were created and used during the execution
// of a block
type ProofGenerator struct {
	touches        [][]byte               // Read/change set of account keys (account hashes)
	storageTouches [][]byte               // Read/change set of storage keys (account hashes concatenated with storage key hashes)
	proofCodes     map[common.Hash][]byte // Contract codes that have been accessed
	createdCodes   map[common.Hash][]byte // Contract codes that were created (deployed)
}

// NewProofGenerator creates new ProofGenerator and initialised its maps
func NewProofGenerator() *ProofGenerator {
	return &ProofGenerator{
		proofCodes:   make(map[common.Hash][]byte),
		createdCodes: make(map[common.Hash][]byte),
	}
}

// AddTouch adds a key (in KEY encoding) into the read/change set of account keys
func (pg *ProofGenerator) AddTouch(touch []byte) {
	pg.touches = append(pg.touches, common.CopyBytes(touch))
}

// AddStorageTouch adds a key (in KEY encoding) into the read/change set of storage keys
func (pg *ProofGenerator) AddStorageTouch(touch []byte) {
	pg.storageTouches = append(pg.storageTouches, common.CopyBytes(touch))
}

// ExtractTouches returns accumulated read/change sets and clears them for the next block's execution
func (pg *ProofGenerator) ExtractTouches() ([][]byte, [][]byte) {
	touches := pg.touches
	storageTouches := pg.storageTouches
	pg.touches = nil
	pg.storageTouches = nil
	return touches, storageTouches
}

// ExtractCodeMap returns the map of all contract codes that were required during the block's execution
// but were not created during that same block. It also clears the maps for the next block's execution
func (pg *ProofGenerator) ExtractCodeMap() map[common.Hash][]byte {
	proofCodes := pg.proofCodes
	pg.proofCodes = make(map[common.Hash][]byte)
	pg.createdCodes = make(map[common.Hash][]byte)
	return proofCodes
}

// ReadCode registers that given contract code has been accessed during current block's execution
func (pg *ProofGenerator) ReadCode(codeHash common.Hash, code []byte) {
	if _, ok := pg.createdCodes[codeHash]; !ok {
		pg.proofCodes[codeHash] = code
	}
}

// CreateCode registers that given contract code has been created (deployed) during current block's execution
func (pg *ProofGenerator) CreateCode(codeHash common.Hash, code []byte) {
	if _, ok := pg.proofCodes[codeHash]; !ok {
		pg.createdCodes[codeHash] = code
	}
}
