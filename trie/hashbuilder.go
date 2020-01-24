package trie

import (
	"fmt"
	"io"
	"math/big"
	"math/bits"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/pool"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/trie/rlphacks"
	"golang.org/x/crypto/sha3"
)

const hashStackStride = common.HashLength + 1 // + 1 byte for RLP encoding
var EmptyCodeHash = crypto.Keccak256Hash(nil)

// HashBuilder implements the interface `structInfoReceiver` and opcodes that the structural information of the trie
// is comprised of
// DESCRIBED: docs/programmers_guide/guide.md#separation-of-keys-and-the-structure
type HashBuilder struct {
	byteArrayWriter *ByteArrayWriter

	hashStack []byte           // Stack of sub-slices, each 33 bytes each, containing RLP encodings of node hashes (or of nodes themselves, if shorter than 32 bytes)
	nodeStack []node           // Stack of nodes
	acc       accounts.Account // Working account instance (to avoid extra allocations)
	sha       keccakState      // Keccak primitive that can absorb data (Write), and get squeezed to the hash out (Read)

	trace bool // Set to true when HashBuilder is required to print trace information for diagnostics
}

// NewHashBuilder creates a new HashBuilder
func NewHashBuilder(trace bool) *HashBuilder {
	return &HashBuilder{
		sha:             sha3.NewLegacyKeccak256().(keccakState),
		byteArrayWriter: &ByteArrayWriter{},
		trace:           trace,
	}
}

// Reset makes the HashBuilder suitable for reuse
func (hb *HashBuilder) Reset() {
	hb.hashStack = hb.hashStack[:0]
	hb.nodeStack = hb.nodeStack[:0]
}

func (hb *HashBuilder) leaf(length int, keyHex []byte, val rlphacks.RlpSerializable) error {
	if hb.trace {
		fmt.Printf("LEAF %d\n", length)
	}
	if length < 0 {
		return fmt.Errorf("length %d", length)
	}
	key := keyHex[len(keyHex)-length:]
	s := &shortNode{Key: common.CopyBytes(key), Val: valueNode(common.CopyBytes(val.RawBytes()))}
	hb.nodeStack = append(hb.nodeStack, s)
	if err := hb.leafHashWithKeyVal(key, val); err != nil {
		return err
	}
	if hb.trace {
		fmt.Printf("Stack depth: %d\n", len(hb.nodeStack))
	}
	return nil
}

// To be called internally
func (hb *HashBuilder) leafHashWithKeyVal(key []byte, val rlphacks.RlpSerializable) error {
	var hash [hashStackStride]byte // RLP representation of hash (or of un-hashed value if short)
	// Compute the total length of binary representation
	var keyPrefix [1]byte
	var lenPrefix [4]byte
	var kp, kl int
	// Write key
	var compactLen int
	var ni int
	var compact0 byte
	if hasTerm(key) {
		compactLen = (len(key)-1)/2 + 1
		if len(key)&1 == 0 {
			compact0 = 0x30 + key[0] // Odd: (3<<4) + first nibble
			ni = 1
		} else {
			compact0 = 0x20
		}
	} else {
		compactLen = len(key)/2 + 1
		if len(key)&1 == 1 {
			compact0 = 0x10 + key[0] // Odd: (1<<4) + first nibble
			ni = 1
		}
	}
	if compactLen > 1 {
		keyPrefix[0] = 0x80 + byte(compactLen)
		kp = 1
		kl = compactLen
	} else {
		kl = 1
	}

	err := hb.completeLeafHash(kp, kl, compactLen, key, keyPrefix, compact0, ni, lenPrefix, hash[:], val)
	if err != nil {
		return err
	}

	hb.hashStack = append(hb.hashStack, hash[:]...)
	if len(hb.hashStack) > hashStackStride*len(hb.nodeStack) {
		hb.nodeStack = append(hb.nodeStack, nil)
	}
	return nil
}

func (hb *HashBuilder) completeLeafHash(kp, kl, compactLen int, key []byte, keyPrefix [1]byte, compact0 byte, ni int, lenPrefix [4]byte, hash []byte, val rlphacks.RlpSerializable) error {
	totalLen := kp + kl + val.DoubleRLPLen()
	pt := rlphacks.GenerateStructLen(lenPrefix[:], totalLen)

	var writer io.Writer
	var reader io.Reader

	if totalLen+pt < common.HashLength {
		// Embedded node
		hb.byteArrayWriter.Setup(hash[:], 0)
		writer = hb.byteArrayWriter
	} else {
		hb.sha.Reset()
		writer = hb.sha
		reader = hb.sha
	}

	if _, err := writer.Write(lenPrefix[:pt]); err != nil {
		return err
	}
	if _, err := writer.Write(keyPrefix[:kp]); err != nil {
		return err
	}
	var b [1]byte
	b[0] = compact0
	if _, err := writer.Write(b[:]); err != nil {
		return err
	}
	for i := 1; i < compactLen; i++ {
		b[0] = key[ni]*16 + key[ni+1]
		if _, err := writer.Write(b[:]); err != nil {
			return err
		}
		ni += 2
	}

	if err := val.ToDoubleRLP(writer); err != nil {
		return err
	}

	if reader != nil {
		hash[0] = 0x80 + common.HashLength
		if _, err := reader.Read(hash[1:]); err != nil {
			return err
		}
	}

	return nil
}

func (hb *HashBuilder) leafHash(length int, keyHex []byte, val rlphacks.RlpSerializable) error {
	if hb.trace {
		fmt.Printf("LEAFHASH %d\n", length)
	}
	if length < 0 {
		return fmt.Errorf("length %d", length)
	}
	key := keyHex[len(keyHex)-length:]
	return hb.leafHashWithKeyVal(key, val)
}

func (hb *HashBuilder) accountLeaf(length int, keyHex []byte, storageSize uint64, balance *big.Int, nonce uint64, incarnation uint64, fieldSet uint32) (err error) {
	if hb.trace {
		fmt.Printf("ACCOUNTLEAF %d (%b)\n", length, fieldSet)
	}
	key := keyHex[len(keyHex)-length:]
	hb.acc.Root = EmptyRoot
	hb.acc.CodeHash = EmptyCodeHash
	hb.acc.Nonce = nonce
	hb.acc.Balance.Set(balance)
	hb.acc.Initialised = true
	hb.acc.StorageSize = storageSize
	hb.acc.HasStorageSize = hb.acc.StorageSize > 0
	hb.acc.Incarnation = incarnation

	popped := 0
	var root node
	if fieldSet&uint32(4) != 0 {
		copy(hb.acc.Root[:], hb.hashStack[len(hb.hashStack)-popped*hashStackStride-common.HashLength:len(hb.hashStack)-popped*hashStackStride])
		if hb.acc.Root != EmptyRoot {
			// Root is on top of the stack
			root = hb.nodeStack[len(hb.nodeStack)-popped-1]
			if root == nil {
				root = hashNode(common.CopyBytes(hb.acc.Root[:]))
			}
		}
		popped++
	}
	if fieldSet&uint32(8) != 0 {
		copy(hb.acc.CodeHash[:], hb.hashStack[len(hb.hashStack)-popped*hashStackStride-common.HashLength:len(hb.hashStack)-popped*hashStackStride])
		popped++
	}
	var accCopy accounts.Account
	accCopy.Copy(&hb.acc)

	s := &shortNode{Key: common.CopyBytes(key), Val: &accountNode{accCopy, root, true}}
	// this invocation will take care of the popping given number of items from both hash stack and node stack,
	// pushing resulting hash to the hash stack, and nil to the node stack
	if err = hb.accountLeafHashWithKey(key, popped); err != nil {
		return err
	}
	// Replace top of the stack
	hb.nodeStack[len(hb.nodeStack)-1] = s
	if hb.trace {
		fmt.Printf("Stack depth: %d\n", len(hb.nodeStack))
	}
	return nil
}

func (hb *HashBuilder) accountLeafHash(length int, keyHex []byte, storageSize uint64, balance *big.Int, nonce uint64, incarnation uint64, fieldSet uint32) (err error) {
	if hb.trace {
		fmt.Printf("ACCOUNTLEAFHASH %d (%b)\n", length, fieldSet)
	}
	key := keyHex[len(keyHex)-length:]
	hb.acc.Root = EmptyRoot
	hb.acc.CodeHash = EmptyCodeHash
	hb.acc.Nonce = nonce
	hb.acc.Balance.Set(balance)
	hb.acc.Initialised = true
	hb.acc.StorageSize = storageSize
	hb.acc.HasStorageSize = storageSize > 0
	hb.acc.Incarnation = incarnation

	popped := 0
	if fieldSet&uint32(4) != 0 {
		copy(hb.acc.Root[:], hb.hashStack[len(hb.hashStack)-popped*hashStackStride-common.HashLength:len(hb.hashStack)-popped*hashStackStride])
		popped++
	}
	if fieldSet&uint32(8) != 0 {
		copy(hb.acc.CodeHash[:], hb.hashStack[len(hb.hashStack)-popped*hashStackStride-common.HashLength:len(hb.hashStack)-popped*hashStackStride])
		popped++
	}
	return hb.accountLeafHashWithKey(key, popped)
}

// To be called internally
func (hb *HashBuilder) accountLeafHashWithKey(key []byte, popped int) error {
	var hash [hashStackStride]byte // RLP representation of hash (or un-hashes value)
	// Compute the total length of binary representation
	var keyPrefix [1]byte
	var lenPrefix [4]byte
	var kp, kl int
	// Write key
	var compactLen int
	var ni int
	var compact0 byte
	if hasTerm(key) {
		compactLen = (len(key)-1)/2 + 1
		if len(key)&1 == 0 {
			compact0 = 48 + key[0] // Odd (1<<4) + first nibble
			ni = 1
		} else {
			compact0 = 32
		}
	} else {
		compactLen = len(key)/2 + 1
		if len(key)&1 == 1 {
			compact0 = 16 + key[0] // Odd (1<<4) + first nibble
			ni = 1
		}
	}
	if compactLen > 1 {
		keyPrefix[0] = byte(128 + compactLen)
		kp = 1
		kl = compactLen
	} else {
		kl = 1
	}
	valLen := hb.acc.EncodingLengthForHashing()
	valBuf := pool.GetBuffer(valLen)
	defer pool.PutBuffer(valBuf)
	hb.acc.EncodeForHashing(valBuf.B)
	val := rlphacks.RlpEncodedBytes(valBuf.B)

	err := hb.completeLeafHash(kp, kl, compactLen, key, keyPrefix, compact0, ni, lenPrefix, hash[:], val)
	if err != nil {
		return err
	}

	if popped > 0 {
		hb.hashStack = hb.hashStack[:len(hb.hashStack)-popped*hashStackStride]
		hb.nodeStack = hb.nodeStack[:len(hb.nodeStack)-popped]
	}
	hb.hashStack = append(hb.hashStack, hash[:]...)
	hb.nodeStack = append(hb.nodeStack, nil)

	return nil
}

func (hb *HashBuilder) extension(key []byte) error {
	if hb.trace {
		fmt.Printf("EXTENSION %x\n", key)
	}
	nd := hb.nodeStack[len(hb.nodeStack)-1]
	switch n := nd.(type) {
	case nil:
		branchHash := common.CopyBytes(hb.hashStack[len(hb.hashStack)-common.HashLength:])
		hb.nodeStack[len(hb.nodeStack)-1] = &shortNode{Key: common.CopyBytes(key), Val: hashNode(branchHash)}
	case *fullNode:
		hb.nodeStack[len(hb.nodeStack)-1] = &shortNode{Key: common.CopyBytes(key), Val: n}
	default:
		return fmt.Errorf("wrong Val type for an extension: %T", nd)
	}
	if err := hb.extensionHash(key); err != nil {
		return err
	}
	if hb.trace {
		fmt.Printf("Stack depth: %d\n", len(hb.nodeStack))
	}
	return nil
}

func (hb *HashBuilder) extensionHash(key []byte) error {
	if hb.trace {
		fmt.Printf("EXTENSIONHASH %x\n", key)
	}
	branchHash := hb.hashStack[len(hb.hashStack)-hashStackStride:]
	// Compute the total length of binary representation
	var keyPrefix [1]byte
	var lenPrefix [4]byte
	var kp, kl int
	// Write key
	var compactLen int
	var ni int
	var compact0 byte
	// https://github.com/ethereum/wiki/wiki/Patricia-Tree#specification-compact-encoding-of-hex-sequence-with-optional-terminator
	if hasTerm(key) {
		compactLen = (len(key)-1)/2 + 1
		if len(key)&1 == 0 {
			compact0 = 0x30 + key[0] // Odd: (3<<4) + first nibble
			ni = 1
		} else {
			compact0 = 0x20
		}
	} else {
		compactLen = len(key)/2 + 1
		if len(key)&1 == 1 {
			compact0 = 0x10 + key[0] // Odd: (1<<4) + first nibble
			ni = 1
		}
	}
	if compactLen > 1 {
		keyPrefix[0] = 0x80 + byte(compactLen)
		kp = 1
		kl = compactLen
	} else {
		kl = 1
	}
	totalLen := kp + kl + 33
	pt := rlphacks.GenerateStructLen(lenPrefix[:], totalLen)
	hb.sha.Reset()
	if _, err := hb.sha.Write(lenPrefix[:pt]); err != nil {
		return err
	}
	if _, err := hb.sha.Write(keyPrefix[:kp]); err != nil {
		return err
	}
	var b [1]byte
	b[0] = compact0
	if _, err := hb.sha.Write(b[:]); err != nil {
		return err
	}
	for i := 1; i < compactLen; i++ {
		b[0] = key[ni]*16 + key[ni+1]
		if _, err := hb.sha.Write(b[:]); err != nil {
			return err
		}
		ni += 2
	}
	if _, err := hb.sha.Write(branchHash[:branchHash[0]-127]); err != nil {
		return err
	}
	// Replace previous hash with the new one
	if _, err := hb.sha.Read(hb.hashStack[len(hb.hashStack)-common.HashLength:]); err != nil {
		return err
	}
	if _, ok := hb.nodeStack[len(hb.nodeStack)-1].(*fullNode); ok {
		return fmt.Errorf("extensionHash cannot be emitted when a node is on top of the stack")
	}
	return nil
}

func (hb *HashBuilder) branch(set uint16) error {
	if hb.trace {
		fmt.Printf("BRANCH (%b)\n", set)
	}
	f := &fullNode{}
	digits := bits.OnesCount16(set)
	if len(hb.nodeStack) < digits {
		return fmt.Errorf("len(hb.nodeStask) %d < digits %d", len(hb.nodeStack), digits)
	}
	nodes := hb.nodeStack[len(hb.nodeStack)-digits:]
	hashes := hb.hashStack[len(hb.hashStack)-hashStackStride*digits:]
	var i int
	for digit := uint(0); digit < 16; digit++ {
		if ((uint16(1) << digit) & set) != 0 {
			if nodes[i] == nil {
				f.Children[digit] = hashNode(common.CopyBytes(hashes[hashStackStride*i+1 : hashStackStride*(i+1)]))
			} else {
				f.Children[digit] = nodes[i]
			}
			i++
		}
	}
	hb.nodeStack = hb.nodeStack[:len(hb.nodeStack)-digits+1]
	hb.nodeStack[len(hb.nodeStack)-1] = f
	if err := hb.branchHash(set); err != nil {
		return err
	}
	copy(f.flags.hash[:], hb.hashStack[len(hb.hashStack)-common.HashLength:])
	if hb.trace {
		fmt.Printf("Stack depth: %d\n", len(hb.nodeStack))
	}
	return nil
}

func (hb *HashBuilder) branchHash(set uint16) error {
	if hb.trace {
		fmt.Printf("BRANCHHASH (%b)\n", set)
	}
	digits := bits.OnesCount16(set)
	if len(hb.hashStack) < hashStackStride*digits {
		return fmt.Errorf("len(hb.hashStack) %d < hashStackStride*digits %d", len(hb.hashStack), hashStackStride*digits)
	}
	hashes := hb.hashStack[len(hb.hashStack)-hashStackStride*digits:]
	// Calculate the size of the resulting RLP
	totalSize := 17 // These are 17 length prefixes
	var i int
	for digit := uint(0); digit < 16; digit++ {
		if ((uint16(1) << digit) & set) != 0 {
			if hashes[hashStackStride*i] == 0x80+common.HashLength {
				totalSize += common.HashLength
			} else {
				// Embedded node
				totalSize += int(hashes[hashStackStride*i] - rlp.EmptyListCode)
			}
			i++
		}
	}
	hb.sha.Reset()
	var lenPrefix [4]byte
	pt := rlphacks.GenerateStructLen(lenPrefix[:], totalSize)
	if _, err := hb.sha.Write(lenPrefix[:pt]); err != nil {
		return err
	}
	// Output children hashes or embedded RLPs
	i = 0
	var b [1]byte
	b[0] = rlp.EmptyStringCode
	for digit := uint(0); digit < 17; digit++ {
		if ((uint16(1) << digit) & set) != 0 {
			if hashes[hashStackStride*i] == byte(0x80+common.HashLength) {
				if _, err := hb.sha.Write(hashes[hashStackStride*i : hashStackStride*i+hashStackStride]); err != nil {
					return err
				}
			} else {
				// Embedded node
				size := int(hashes[hashStackStride*i]) - rlp.EmptyListCode
				if _, err := hb.sha.Write(hashes[hashStackStride*i : hashStackStride*i+size+1]); err != nil {
					return err
				}
			}
			i++
		} else {
			if _, err := hb.sha.Write(b[:]); err != nil {
				return err
			}
		}
	}
	hb.hashStack = hb.hashStack[:len(hb.hashStack)-hashStackStride*digits+hashStackStride]
	hb.hashStack[len(hb.hashStack)-hashStackStride] = 0x80 + common.HashLength
	if _, err := hb.sha.Read(hb.hashStack[len(hb.hashStack)-common.HashLength:]); err != nil {
		return err
	}
	if hashStackStride*len(hb.nodeStack) > len(hb.hashStack) {
		hb.nodeStack = hb.nodeStack[:len(hb.nodeStack)-digits+1]
		if hb.trace {
			fmt.Printf("Setting hb.nodeStack[%d] to nil\n", len(hb.nodeStack)-1)
		}
		hb.nodeStack[len(hb.nodeStack)-1] = nil
	}
	return nil
}

func (hb *HashBuilder) hash(hash common.Hash) error {
	if hb.trace {
		fmt.Printf("HASH\n")
	}
	hb.hashStack = append(hb.hashStack, 0x80+common.HashLength)
	hb.hashStack = append(hb.hashStack, hash[:]...)
	hb.nodeStack = append(hb.nodeStack, nil)
	if hb.trace {
		fmt.Printf("Stack depth: %d\n", len(hb.nodeStack))
	}
	return nil
}

func (hb *HashBuilder) code(code []byte) (common.Hash, error) {
	if hb.trace {
		fmt.Printf("CODE\n")
	}
	codeCopy := common.CopyBytes(code)
	hb.nodeStack = append(hb.nodeStack, nil)
	hb.sha.Reset()
	if _, err := hb.sha.Write(codeCopy); err != nil {
		return common.Hash{}, err
	}
	var hash [hashStackStride]byte // RLP representation of hash (or un-hashes value)
	hash[0] = 0x80 + common.HashLength
	if _, err := hb.sha.Read(hash[1:]); err != nil {
		return common.Hash{}, err
	}
	hb.hashStack = append(hb.hashStack, hash[:]...)
	return common.BytesToHash(hash[1:]), nil
}

func (hb *HashBuilder) emptyRoot() {
	if hb.trace {
		fmt.Printf("EMPTYROOT\n")
	}
	hb.nodeStack = append(hb.nodeStack, nil)
	var hash [hashStackStride]byte // RLP representation of hash (or un-hashes value)
	hash[0] = 0x80 + common.HashLength
	copy(hash[1:], EmptyRoot[:])
	hb.hashStack = append(hb.hashStack, hash[:]...)
}

func (hb *HashBuilder) RootHash() (common.Hash, error) {
	if !hb.hasRoot() {
		return common.Hash{}, fmt.Errorf("no root in the tree")
	}
	return hb.rootHash(), nil
}

func (hb *HashBuilder) rootHash() common.Hash {
	var hash common.Hash
	copy(hash[:], hb.hashStack[1:hashStackStride])
	return hash
}

func (hb *HashBuilder) root() node {
	if hb.trace && len(hb.nodeStack) > 0 {
		fmt.Printf("len(hb.nodeStack)=%d\n", len(hb.nodeStack))
	}
	return hb.nodeStack[len(hb.nodeStack)-1]
}

func (hb *HashBuilder) hasRoot() bool {
	return len(hb.nodeStack) > 0
}
