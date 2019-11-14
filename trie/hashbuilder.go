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
	keyTape     BytesTape           // the source of key sequence
	valueTape   RlpSerializableTape // the source of values (for values that are not accounts or contracts)
	nonceTape   Uint64Tape          // the source of nonces for accounts and contracts (field 0)
	balanceTape BigIntTape          // the source of balances for accounts and contracts (field 1)
	sSizeTape   Uint64Tape          // the source of storage sizes for contracts (field 4)
	hashTape    HashTape            // the source of hashes
	codeTape    BytesTape           // the source of bytecodes

	byteArrayWriter *ByteArrayWriter

	hashStack []byte           // Stack of sub-slices, each 33 bytes each, containing RLP encodings of node hashes (or of nodes themselves, if shorter than 32 bytes)
	nodeStack []node           // Stack of nodes
	acc       accounts.Account // Working account instance (to avoid extra allocations)
	sha       keccakState      // Keccak primitive that can absorb data (Write), and get squeezed to the hash out (Read)
}

// NewHashBuilder creates a new HashBuilder
func NewHashBuilder() *HashBuilder {
	return &HashBuilder{
		sha:             sha3.NewLegacyKeccak256().(keccakState),
		byteArrayWriter: &ByteArrayWriter{},
	}
}

// SetKeyTape sets the key tape to be used by this builder (opcodes leaf, leafHash, accountLeaf, accountLeafHash)
func (hb *HashBuilder) SetKeyTape(keyTape BytesTape) {
	hb.keyTape = keyTape
}

// SetValueTape sets the value tape to be used by this builder (opcodes leaf and leafHash)
func (hb *HashBuilder) SetValueTape(valueTape RlpSerializableTape) {
	hb.valueTape = valueTape
}

// SetNonceTape sets the nonce tape to be used by this builder (opcodes accountLeaf, accountLeafHash)
func (hb *HashBuilder) SetNonceTape(nonceTape Uint64Tape) {
	hb.nonceTape = nonceTape
}

// SetBalanceTape sets the balance tape to be used by this builder (opcodes accountLeaf, accountLeafHash)
func (hb *HashBuilder) SetBalanceTape(balanceTape BigIntTape) {
	hb.balanceTape = balanceTape
}

// SetHashTape sets the hash tape to be used by this builder (opcode hash)
func (hb *HashBuilder) SetHashTape(hashTape HashTape) {
	hb.hashTape = hashTape
}

// SetSSizeTape sets the storage size tape to be used by this builder (opcodes accountLeaf, accountLeafHashs)
func (hb *HashBuilder) SetSSizeTape(sSizeTape Uint64Tape) {
	hb.sSizeTape = sSizeTape
}

// SetCodeTape sets the code tape to be used by this builder (opcode CODE)
func (hb *HashBuilder) SetCodeTape(codeTape BytesTape) {
	hb.codeTape = codeTape
}

// Reset makes the HashBuilder suitable for reuse
func (hb *HashBuilder) Reset() {
	hb.hashStack = hb.hashStack[:0]
	hb.nodeStack = hb.nodeStack[:0]
}

func (hb *HashBuilder) leaf(length int) error {
	//fmt.Printf("LEAF %d\n", length)
	hex, err := hb.keyTape.Next()
	if err != nil {
		return err
	}
	key := hex[len(hex)-length:]
	val, err := hb.valueTape.Next()
	if err != nil {
		return err
	}
	s := &shortNode{Key: common.CopyBytes(key), Val: valueNode(common.CopyBytes(val.RawBytes()))}
	hb.nodeStack = append(hb.nodeStack, s)
	return hb.leafHashWithKeyVal(key, val)
}

// To be called internally
func (hb *HashBuilder) leafHashWithKeyVal(key []byte, val RlpSerializable) error {
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
		keyPrefix[0] = rlp.EmptyStringCode + byte(compactLen)
		kp = 1
		kl = compactLen
	} else {
		kl = 1
	}

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
		hash[0] = rlp.EmptyStringCode + common.HashLength
		if _, err := reader.Read(hash[1:]); err != nil {
			return err
		}
	}

	hb.hashStack = append(hb.hashStack, hash[:]...)
	if len(hb.hashStack) > hashStackStride*len(hb.nodeStack) {
		hb.nodeStack = append(hb.nodeStack, nil)
	}
	return nil
}

func (hb *HashBuilder) leafHash(length int) error {
	//fmt.Printf("LEAFHASH %d\n", length)
	hex, err := hb.keyTape.Next()
	if err != nil {
		return err
	}
	key := hex[len(hex)-length:]
	val, err := hb.valueTape.Next()
	if err != nil {
		return err
	}
	return hb.leafHashWithKeyVal(key, val)
}

func (hb *HashBuilder) accountLeaf(length int, fieldSet uint32) error {
	//fmt.Printf("ACCOUNTLEAF %d\n", length)
	hex, err := hb.keyTape.Next()
	if err != nil {
		return err
	}
	key := hex[len(hex)-length:]
	hb.acc.Root = EmptyRoot
	hb.acc.CodeHash = EmptyCodeHash
	hb.acc.Nonce = 0
	hb.acc.Balance.SetUint64(0)
	hb.acc.Initialised = true
	hb.acc.StorageSize = 0
	hb.acc.HasStorageSize = false
	if fieldSet&uint32(1) != 0 {
		if hb.acc.Nonce, err = hb.nonceTape.Next(); err != nil {
			return err
		}
	}
	if fieldSet&uint32(2) != 0 {
		var balance *big.Int
		if balance, err = hb.balanceTape.Next(); err != nil {
			return err
		}
		hb.acc.Balance.Set(balance)
	}
	popped := 0
	var root node
	if fieldSet&uint32(4) != 0 {
		copy(hb.acc.Root[:], hb.hashStack[len(hb.hashStack)-popped*33-32:len(hb.hashStack)-popped*33])
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
		copy(hb.acc.CodeHash[:], hb.hashStack[len(hb.hashStack)-popped*33-32:len(hb.hashStack)-popped*33])
		popped++
	}
	if fieldSet&uint32(16) != 0 {
		if hb.acc.StorageSize, err = hb.sSizeTape.Next(); err != nil {
			return err
		}
		hb.acc.HasStorageSize = hb.acc.StorageSize > 0
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
	return nil
}

func (hb *HashBuilder) accountLeafHash(length int, fieldSet uint32) error {
	//fmt.Printf("ACCOUNTLEAFHASH %d\n", length)
	hex, err := hb.keyTape.Next()
	if err != nil {
		return err
	}
	key := hex[len(hex)-length:]
	hb.acc.Root = EmptyRoot
	hb.acc.CodeHash = EmptyCodeHash
	hb.acc.Nonce = 0
	hb.acc.Balance.SetUint64(0)
	hb.acc.Initialised = true
	hb.acc.StorageSize = 0
	hb.acc.HasStorageSize = false
	if fieldSet&uint32(1) != 0 {
		if hb.acc.Nonce, err = hb.nonceTape.Next(); err != nil {
			return err
		}
	}
	if fieldSet&uint32(2) != 0 {
		var balance *big.Int
		if balance, err = hb.balanceTape.Next(); err != nil {
			return err
		}
		hb.acc.Balance.Set(balance)
	}
	popped := 0
	if fieldSet&uint32(4) != 0 {
		copy(hb.acc.Root[:], hb.hashStack[len(hb.hashStack)-popped*33-32:len(hb.hashStack)-popped*33])
		popped++
	}
	if fieldSet&uint32(8) != 0 {
		copy(hb.acc.CodeHash[:], hb.hashStack[len(hb.hashStack)-popped*33-32:len(hb.hashStack)-popped*33])
		popped++
	}
	if fieldSet&uint32(16) != 0 {
		if hb.acc.StorageSize, err = hb.sSizeTape.Next(); err != nil {
			return err
		}
		hb.acc.HasStorageSize = hb.acc.StorageSize > 0
	}
	return hb.accountLeafHashWithKey(key, popped)
}

// To be called internally
func (hb *HashBuilder) accountLeafHashWithKey(key []byte, popped int) error {
	var hash [33]byte // RLP representation of hash (or un-hashes value)
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

	totalLen := kp + kl + val.DoubleRLPLen()
	pt := rlphacks.GenerateStructLen(lenPrefix[:], totalLen)

	// FIXME: extract copy-paste

	var writer io.Writer
	var reader io.Reader

	if pt+totalLen < common.HashLength {
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
		hash[0] = byte(128 + 32)
		if _, err := reader.Read(hash[1:]); err != nil {
			return err
		}
	}
	if popped > 0 {
		hb.hashStack = hb.hashStack[:len(hb.hashStack)-popped*33]
		hb.nodeStack = hb.nodeStack[:len(hb.nodeStack)-popped]
	}
	hb.hashStack = append(hb.hashStack, hash[:]...)
	hb.nodeStack = append(hb.nodeStack, nil)
	return nil
}

func (hb *HashBuilder) extension(key []byte) error {
	//fmt.Printf("EXTENSION %x\n", key)
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
	return hb.extensionHash(key)
}

func (hb *HashBuilder) extensionHash(key []byte) error {
	//fmt.Printf("EXTENSIONHASH %x\n", key)
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
		keyPrefix[0] = rlp.EmptyStringCode + byte(compactLen)
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
	if _, err := hb.sha.Write(branchHash); err != nil {
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
	//fmt.Printf("BRANCH %b\n", set)
	f := &fullNode{}
	digits := bits.OnesCount16(set)
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
	return nil
}

func (hb *HashBuilder) branchHash(set uint16) error {
	//fmt.Printf("BRANCHHASH %b\n", set)
	digits := bits.OnesCount16(set)
	hashes := hb.hashStack[len(hb.hashStack)-hashStackStride*digits:]
	// Calculate the size of the resulting RLP
	totalSize := 17 // These are 17 length prefixes
	var i int
	for digit := uint(0); digit < 16; digit++ {
		if ((uint16(1) << digit) & set) != 0 {
			if hashes[hashStackStride*i] == rlp.EmptyStringCode+common.HashLength {
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
			if hashes[hashStackStride*i] == byte(rlp.EmptyStringCode+common.HashLength) {
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
	hb.hashStack[len(hb.hashStack)-hashStackStride] = rlp.EmptyStringCode + common.HashLength
	if _, err := hb.sha.Read(hb.hashStack[len(hb.hashStack)-common.HashLength:]); err != nil {
		return err
	}
	if hashStackStride*len(hb.nodeStack) > len(hb.hashStack) {
		hb.nodeStack = hb.nodeStack[:len(hb.nodeStack)-digits+1]
		hb.nodeStack[len(hb.nodeStack)-1] = nil
	}
	return nil
}

func (hb *HashBuilder) hash(number int) error {
	for i := 0; i < number; i++ {
		hash, err := hb.hashTape.Next()
		if err != nil {
			return err
		}
		hb.hashStack = append(hb.hashStack, rlp.EmptyStringCode+common.HashLength)
		hb.hashStack = append(hb.hashStack, hash[:]...)
		hb.nodeStack = append(hb.nodeStack, nil)
	}
	return nil
}

func (hb *HashBuilder) code() ([]byte, common.Hash, error) {
	code, err := hb.codeTape.Next()
	if err != nil {
		return nil, common.Hash{}, err
	}
	code = common.CopyBytes(code)
	hb.nodeStack = append(hb.nodeStack, nil)
	hb.sha.Reset()
	if _, err := hb.sha.Write(code); err != nil {
		return nil, common.Hash{}, err
	}
	var hash [hashStackStride]byte // RLP representation of hash (or un-hashes value)
	hash[0] = rlp.EmptyStringCode + common.HashLength
	if _, err := hb.sha.Read(hash[1:]); err != nil {
		return nil, common.Hash{}, err
	}
	hb.hashStack = append(hb.hashStack, hash[:]...)
	return code, common.BytesToHash(hash[1:]), nil
}

func (hb *HashBuilder) emptyRoot() {
	hb.nodeStack = append(hb.nodeStack, nil)
	var hash [hashStackStride]byte // RLP representation of hash (or un-hashes value)
	hash[0] = rlp.EmptyStringCode + common.HashLength
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
	return hb.nodeStack[0]
}

func (hb *HashBuilder) hasRoot() bool {
	return len(hb.nodeStack) > 0
}
