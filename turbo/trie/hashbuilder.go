package trie

import (
	"bytes"
	"fmt"
	"io"
	"math/bits"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	length2 "github.com/ledgerwatch/erigon-lib/common/length"
	"golang.org/x/crypto/sha3"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/rlphacks"
)

const hashStackStride = length2.Hash + 1 // + 1 byte for RLP encoding

var EmptyCodeHash = crypto.Keccak256Hash(nil)

// HashBuilder implements the interface `structInfoReceiver` and opcodes that the structural information of the trie
// is comprised of
// DESCRIBED: docs/programmers_guide/guide.md#separation-of-keys-and-the-structure
type HashBuilder struct {
	byteArrayWriter *ByteArrayWriter

	hashStack []byte                // Stack of sub-slices, each 33 bytes each, containing RLP encodings of node hashes (or of nodes themselves, if shorter than 32 bytes)
	nodeStack []node                // Stack of nodes
	acc       accounts.Account      // Working account instance (to avoid extra allocations)
	sha       keccakState           // Keccak primitive that can absorb data (Write), and get squeezed to the hash out (Read)
	hashBuf   [hashStackStride]byte // RLP representation of hash (or un-hashes value)
	keyPrefix [1]byte
	lenPrefix [4]byte
	valBuf    [128]byte // Enough to accommodate hash encoding of any account
	b         [1]byte   // Buffer for single byte
	prefixBuf [8]byte
	trace     bool // Set to true when HashBuilder is required to print trace information for diagnostics

	topHashesCopy []byte

	// proofElement is set when the next element computation should have its RLP
	// encoding retained.  Additionally, the account root storage hash and storage
	// values are stored into this field when set and in the relavent codepath.
	proofElement *proofElement
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
	if len(hb.hashStack) > 0 {
		hb.hashStack = hb.hashStack[:0]
	}
	if len(hb.nodeStack) > 0 {
		hb.nodeStack = hb.nodeStack[:0]
	}
	hb.topHashesCopy = hb.topHashesCopy[:0]
	hb.proofElement = nil
}

// setProofElement sets the proofElement field in which the relevant methods
// will check and additionally write the proof bytes to during trie computation.
func (hb *HashBuilder) setProofElement(pe *proofElement) {
	hb.proofElement = pe
}

func (hb *HashBuilder) leaf(length int, keyHex []byte, val rlphacks.RlpSerializable) error {
	if hb.trace {
		fmt.Printf("LEAF %d\n", length)
	}
	if length < 0 {
		return fmt.Errorf("length %d", length)
	}
	if hb.proofElement != nil {
		hb.proofElement.storageKey = common.CopyBytes(keyHex[:len(keyHex)-1])
		hb.proofElement.storageValue = new(uint256.Int).SetBytes(val.RawBytes())
	}
	key := keyHex[len(keyHex)-length:]
	s := &shortNode{Key: common.CopyBytes(key), Val: valueNode(common.CopyBytes(val.RawBytes()))}
	hb.nodeStack = append(hb.nodeStack, s)
	if err := hb.leafHashWithKeyVal(key, val); err != nil {
		return err
	}
	copy(s.ref.data[:], hb.hashStack[len(hb.hashStack)-length2.Hash:])
	s.ref.len = hb.hashStack[len(hb.hashStack)-length2.Hash-1] - 0x80
	if s.ref.len > 32 {
		s.ref.len = hb.hashStack[len(hb.hashStack)-length2.Hash-1] - 0xc0 + 1
		copy(s.ref.data[:], hb.hashStack[len(hb.hashStack)-length2.Hash-1:])
	}
	if hb.trace {
		fmt.Printf("Stack depth: %d\n", len(hb.nodeStack))
	}
	return nil
}

// To be called internally
func (hb *HashBuilder) leafHashWithKeyVal(key []byte, val rlphacks.RlpSerializable) error {
	// Compute the total length of binary representation
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
		hb.keyPrefix[0] = 0x80 + byte(compactLen)
		kp = 1
		kl = compactLen
	} else {
		kl = 1
	}

	err := hb.completeLeafHash(kp, kl, compactLen, key, compact0, ni, val)
	if err != nil {
		return err
	}
	//fmt.Printf("leafHashWithKeyVal [%x]=>[%x]\nHash [%x]\n", key, val, hb.hashBuf[:])

	hb.hashStack = append(hb.hashStack, hb.hashBuf[:]...)
	if len(hb.hashStack) > hashStackStride*len(hb.nodeStack) {
		hb.nodeStack = append(hb.nodeStack, nil)
	}
	return nil
}

func (hb *HashBuilder) completeLeafHash(kp, kl, compactLen int, key []byte, compact0 byte, ni int, val rlphacks.RlpSerializable) error {
	totalLen := kp + kl + val.DoubleRLPLen()
	pt := rlphacks.GenerateStructLen(hb.lenPrefix[:], totalLen)

	var writer io.Writer
	var reader io.Reader

	if totalLen+pt < length2.Hash {
		// Embedded node
		hb.byteArrayWriter.Setup(hb.hashBuf[:], 0)
		writer = hb.byteArrayWriter
	} else {
		hb.sha.Reset()
		writer = hb.sha
		reader = hb.sha
	}
	// Collect a copy of the hash input if needed for an eth_getProof
	if hb.proofElement != nil {
		writer = io.MultiWriter(writer, &hb.proofElement.proof)
	}

	if _, err := writer.Write(hb.lenPrefix[:pt]); err != nil {
		return err
	}
	if _, err := writer.Write(hb.keyPrefix[:kp]); err != nil {
		return err
	}
	hb.b[0] = compact0
	if _, err := writer.Write(hb.b[:]); err != nil {
		return err
	}
	for i := 1; i < compactLen; i++ {
		hb.b[0] = key[ni]*16 + key[ni+1]
		if _, err := writer.Write(hb.b[:]); err != nil {
			return err
		}
		ni += 2
	}

	if err := val.ToDoubleRLP(writer, hb.prefixBuf[:]); err != nil {
		return err
	}

	if reader != nil {
		hb.hashBuf[0] = 0x80 + length2.Hash
		if _, err := reader.Read(hb.hashBuf[1:]); err != nil {
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

func (hb *HashBuilder) accountLeaf(length int, keyHex []byte, balance *uint256.Int, nonce uint64, incarnation uint64, fieldSet uint32, accountCodeSize int) (err error) {
	if hb.trace {
		fmt.Printf("ACCOUNTLEAF %d (%b)\n", length, fieldSet)
	}
	fullKey := keyHex[:len(keyHex)-1]
	key := keyHex[len(keyHex)-length:]
	copy(hb.acc.Root[:], EmptyRoot[:])
	copy(hb.acc.CodeHash[:], EmptyCodeHash[:])
	hb.acc.Nonce = nonce
	hb.acc.Balance.Set(balance)
	hb.acc.Initialised = true
	hb.acc.Incarnation = incarnation

	popped := 0
	var root node
	if fieldSet&uint32(4) != 0 {
		copy(hb.acc.Root[:], hb.hashStack[len(hb.hashStack)-popped*hashStackStride-length2.Hash:len(hb.hashStack)-popped*hashStackStride])
		if hb.acc.Root != EmptyRoot {
			// Root is on top of the stack
			root = hb.nodeStack[len(hb.nodeStack)-popped-1]
			if root == nil {
				root = hashNode{hash: common.CopyBytes(hb.acc.Root[:])}
			}
		}
		popped++
	}
	var accountCode codeNode
	if fieldSet&uint32(8) != 0 {
		copy(hb.acc.CodeHash[:], hb.hashStack[len(hb.hashStack)-popped*hashStackStride-length2.Hash:len(hb.hashStack)-popped*hashStackStride])
		var ok bool
		if !bytes.Equal(hb.acc.CodeHash[:], EmptyCodeHash[:]) {
			stackTop := hb.nodeStack[len(hb.nodeStack)-popped-1]
			if stackTop != nil { // if we don't have any stack top it might be okay because we didn't resolve the code yet (stateful resolver)
				// but if we have something on top of the stack that isn't `nil`, it has to be a codeNode
				accountCode, ok = stackTop.(codeNode)
				if !ok {
					return fmt.Errorf("unexpected node type on the node stack, wanted codeNode, got %T:%s", stackTop, stackTop)
				}
			}
		}
		popped++
	}

	if hb.proofElement != nil {
		// The storageRoot is not stored with the account info, therefore
		// we capture it with the account proof element.  Note, we also store the
		// full key as this root could be for a different account in the negative
		// case.
		hb.proofElement.storageRootKey = common.CopyBytes(fullKey)
		hb.proofElement.storageRoot = hb.acc.Root
	}
	var accCopy accounts.Account
	accCopy.Copy(&hb.acc)

	if !bytes.Equal(accCopy.CodeHash[:], EmptyCodeHash[:]) && accountCode != nil {
		accountCodeSize = len(accountCode)
	}

	a := &accountNode{accCopy, root, true, accountCode, accountCodeSize}
	s := &shortNode{Key: common.CopyBytes(key), Val: a}
	// this invocation will take care of the popping given number of items from both hash stack and node stack,
	// pushing resulting hash to the hash stack, and nil to the node stack
	if err = hb.accountLeafHashWithKey(key, popped); err != nil {
		return err
	}
	copy(s.ref.data[:], hb.hashStack[len(hb.hashStack)-length2.Hash:])
	s.ref.len = 32
	// Replace top of the stack
	hb.nodeStack[len(hb.nodeStack)-1] = s
	if hb.trace {
		fmt.Printf("Stack depth: %d\n", len(hb.nodeStack))
	}
	return nil
}

func (hb *HashBuilder) accountLeafHash(length int, keyHex []byte, balance *uint256.Int, nonce uint64, incarnation uint64, fieldSet uint32) (err error) {
	if hb.trace {
		fmt.Printf("ACCOUNTLEAFHASH %d (%b)\n", length, fieldSet)
	}
	key := keyHex[len(keyHex)-length:]
	hb.acc.Nonce = nonce
	hb.acc.Balance.Set(balance)
	hb.acc.Initialised = true
	hb.acc.Incarnation = incarnation

	popped := 0
	if fieldSet&AccountFieldStorageOnly != 0 {
		copy(hb.acc.Root[:], hb.hashStack[len(hb.hashStack)-popped*hashStackStride-length2.Hash:len(hb.hashStack)-popped*hashStackStride])
		popped++
	} else {
		copy(hb.acc.Root[:], EmptyRoot[:])
	}

	if fieldSet&AccountFieldCodeOnly != 0 {
		copy(hb.acc.CodeHash[:], hb.hashStack[len(hb.hashStack)-popped*hashStackStride-length2.Hash:len(hb.hashStack)-popped*hashStackStride])
		popped++
	} else {
		copy(hb.acc.CodeHash[:], EmptyCodeHash[:])
	}

	return hb.accountLeafHashWithKey(key, popped)
}

// To be called internally
func (hb *HashBuilder) accountLeafHashWithKey(key []byte, popped int) error {
	// Compute the total length of binary representation
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
		hb.keyPrefix[0] = byte(128 + compactLen)
		kp = 1
		kl = compactLen
	} else {
		kl = 1
	}
	valLen := hb.acc.EncodingLengthForHashing()
	hb.acc.EncodeForHashing(hb.valBuf[:])
	val := rlphacks.RlpEncodedBytes(hb.valBuf[:valLen])
	err := hb.completeLeafHash(kp, kl, compactLen, key, compact0, ni, val)
	if err != nil {
		return err
	}
	if popped > 0 {
		hb.hashStack = hb.hashStack[:len(hb.hashStack)-popped*hashStackStride]
		hb.nodeStack = hb.nodeStack[:len(hb.nodeStack)-popped]
	}
	//fmt.Printf("accountLeafHashWithKey [%x]=>[%x]\nHash [%x]\n", key, val, hb.hashBuf[:])
	hb.hashStack = append(hb.hashStack, hb.hashBuf[:]...)
	hb.nodeStack = append(hb.nodeStack, nil)
	if hb.trace {
		fmt.Printf("Stack depth: %d\n", len(hb.nodeStack))
	}
	return nil
}

func (hb *HashBuilder) extension(key []byte) error {
	if hb.trace {
		fmt.Printf("EXTENSION %x\n", key)
	}
	nd := hb.nodeStack[len(hb.nodeStack)-1]
	var s *shortNode
	switch n := nd.(type) {
	case nil:
		branchHash := common.CopyBytes(hb.hashStack[len(hb.hashStack)-length2.Hash:])
		s = &shortNode{Key: common.CopyBytes(key), Val: hashNode{hash: branchHash}}
	case *fullNode:
		s = &shortNode{Key: common.CopyBytes(key), Val: n}
	default:
		return fmt.Errorf("wrong Val type for an extension: %T", nd)
	}
	hb.nodeStack[len(hb.nodeStack)-1] = s
	if err := hb.extensionHash(key); err != nil {
		return err
	}
	copy(s.ref.data[:], hb.hashStack[len(hb.hashStack)-length2.Hash:])
	s.ref.len = 32
	if hb.trace {
		fmt.Printf("Stack depth: %d\n", len(hb.nodeStack))
	}
	return nil
}

func (hb *HashBuilder) extensionHash(key []byte) error {
	writer := io.Writer(hb.sha)
	if hb.proofElement != nil {
		writer = io.MultiWriter(hb.sha, &hb.proofElement.proof)
	}

	if hb.trace {
		fmt.Printf("EXTENSIONHASH %x\n", key)
	}
	branchHash := hb.hashStack[len(hb.hashStack)-hashStackStride:]
	// Compute the total length of binary representation
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
		hb.keyPrefix[0] = 0x80 + byte(compactLen)
		kp = 1
		kl = compactLen
	} else {
		kl = 1
	}
	totalLen := kp + kl + 33
	pt := rlphacks.GenerateStructLen(hb.lenPrefix[:], totalLen)
	hb.sha.Reset()
	if _, err := writer.Write(hb.lenPrefix[:pt]); err != nil {
		return err
	}
	if _, err := writer.Write(hb.keyPrefix[:kp]); err != nil {
		return err
	}
	hb.b[0] = compact0
	if _, err := writer.Write(hb.b[:]); err != nil {
		return err
	}
	for i := 1; i < compactLen; i++ {
		hb.b[0] = key[ni]*16 + key[ni+1]
		if _, err := writer.Write(hb.b[:]); err != nil {
			return err
		}
		ni += 2
	}
	//capture := common.CopyBytes(branchHash[:length2.Hash+1])
	if _, err := writer.Write(branchHash[:length2.Hash+1]); err != nil {
		return err
	}
	// Replace previous hash with the new one
	if _, err := hb.sha.Read(hb.hashStack[len(hb.hashStack)-length2.Hash:]); err != nil {
		return err
	}

	hb.hashStack[len(hb.hashStack)-hashStackStride] = 0x80 + length2.Hash
	//fmt.Printf("extensionHash [%x]=>[%x]\nHash [%x]\n", key, capture, hb.hashStack[len(hb.hashStack)-hashStackStride:len(hb.hashStack)])
	if _, ok := hb.nodeStack[len(hb.nodeStack)-1].(*fullNode); ok {
		return fmt.Errorf("extensionHash cannot be emitted when a node is on top of the stack")
	}
	return nil
}

func (hb *HashBuilder) branch(set uint16) error {
	if hb.trace {
		fmt.Printf("BRANCH (%b)\n", set)
	}
	if hb.trace {
		fmt.Printf("Stack depth: %d\n", len(hb.nodeStack))
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
		if ((1 << digit) & set) != 0 {
			if nodes[i] == nil {
				f.Children[digit] = hashNode{hash: common.CopyBytes(hashes[hashStackStride*i+1 : hashStackStride*(i+1)])}
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
	copy(f.ref.data[:], hb.hashStack[len(hb.hashStack)-length2.Hash:])
	f.ref.len = 32
	if hb.trace {
		fmt.Printf("Stack depth: %d\n", len(hb.nodeStack))
	}

	return nil
}

func (hb *HashBuilder) branchHash(set uint16) error {
	writer := io.Writer(hb.sha)
	if hb.proofElement != nil {
		writer = io.MultiWriter(hb.sha, &hb.proofElement.proof)
	}

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
		if ((1 << digit) & set) != 0 {
			if hashes[hashStackStride*i] == 0x80+length2.Hash {
				totalSize += length2.Hash
			} else {
				// Embedded node
				totalSize += int(hashes[hashStackStride*i] - rlp.EmptyListCode)
			}
			i++
		}
	}
	hb.sha.Reset()
	pt := rlphacks.GenerateStructLen(hb.lenPrefix[:], totalSize)
	if _, err := writer.Write(hb.lenPrefix[:pt]); err != nil {
		return err
	}
	// Output hasState hashes or embedded RLPs
	i = 0
	//fmt.Printf("branchHash {\n")
	hb.b[0] = rlp.EmptyStringCode
	for digit := uint(0); digit < 17; digit++ {
		if ((1 << digit) & set) != 0 {
			if hashes[hashStackStride*i] == byte(0x80+length2.Hash) {
				if _, err := writer.Write(hashes[hashStackStride*i : hashStackStride*i+hashStackStride]); err != nil {
					return err
				}
				//fmt.Printf("%x: [%x]\n", digit, hashes[hashStackStride*i:hashStackStride*i+hashStackStride])
			} else {
				// Embedded node
				size := int(hashes[hashStackStride*i]) - rlp.EmptyListCode
				if _, err := writer.Write(hashes[hashStackStride*i : hashStackStride*i+size+1]); err != nil {
					return err
				}
				//fmt.Printf("%x: embedded [%x]\n", digit, hashes[hashStackStride*i:hashStackStride*i+size+1])
			}
			i++
		} else {
			if _, err := writer.Write(hb.b[:]); err != nil {
				return err
			}
			//fmt.Printf("%x: empty\n", digit)
		}
	}
	hb.hashStack = hb.hashStack[:len(hb.hashStack)-hashStackStride*digits+hashStackStride]
	hb.hashStack[len(hb.hashStack)-hashStackStride] = 0x80 + length2.Hash
	if _, err := hb.sha.Read(hb.hashStack[len(hb.hashStack)-length2.Hash:]); err != nil {
		return err
	}

	//fmt.Printf("} [%x]\n", hb.hashStack[len(hb.hashStack)-hashStackStride:])

	if hashStackStride*len(hb.nodeStack) > len(hb.hashStack) {
		hb.nodeStack = hb.nodeStack[:len(hb.nodeStack)-digits+1]
		hb.nodeStack[len(hb.nodeStack)-1] = nil
		if hb.trace {
			fmt.Printf("Setting hb.nodeStack[%d] to nil\n", len(hb.nodeStack)-1)
		}
	}
	if hb.trace {
		fmt.Printf("Stack depth: %d\n", len(hb.nodeStack))
	}
	return nil
}

func (hb *HashBuilder) hash(hash []byte) error {
	if hb.trace {
		fmt.Printf("HASH\n")
	}
	hb.hashStack = append(hb.hashStack, 0x80+length2.Hash)
	hb.hashStack = append(hb.hashStack, hash...)
	hb.nodeStack = append(hb.nodeStack, nil)
	if hb.trace {
		fmt.Printf("Stack depth: %d\n", len(hb.nodeStack))
	}

	return nil
}

func (hb *HashBuilder) code(code []byte) error {
	if hb.trace {
		fmt.Printf("CODE\n")
	}
	codeCopy := common.CopyBytes(code)
	n := codeNode(codeCopy)
	hb.nodeStack = append(hb.nodeStack, n)
	hb.sha.Reset()
	if _, err := hb.sha.Write(codeCopy); err != nil {
		return err
	}
	var hash [hashStackStride]byte // RLP representation of hash (or un-hashes value)
	hash[0] = 0x80 + length2.Hash
	if _, err := hb.sha.Read(hash[1:]); err != nil {
		return err
	}
	hb.hashStack = append(hb.hashStack, hash[:]...)
	return nil
}

func (hb *HashBuilder) emptyRoot() {
	if hb.trace {
		fmt.Printf("EMPTYROOT\n")
	}
	hb.nodeStack = append(hb.nodeStack, nil)
	var hash [hashStackStride]byte // RLP representation of hash (or un-hashes value)
	hash[0] = 0x80 + length2.Hash
	copy(hash[1:], EmptyRoot[:])
	hb.hashStack = append(hb.hashStack, hash[:]...)
}

func (hb *HashBuilder) RootHash() (libcommon.Hash, error) {
	if !hb.hasRoot() {
		return libcommon.Hash{}, fmt.Errorf("no root in the tree")
	}
	return hb.rootHash(), nil
}

func (hb *HashBuilder) rootHash() libcommon.Hash {
	var hash libcommon.Hash
	top := hb.topHash()
	if len(top) == 33 {
		copy(hash[:], top[1:])
	} else {
		hb.sha.Reset()
		if _, err := hb.sha.Write(top); err != nil {
			panic(err)
		}
		if _, err := hb.sha.Read(hash[:]); err != nil {
			panic(err)
		}
	}
	return hash
}

func (hb *HashBuilder) topHash() []byte {
	pos := len(hb.hashStack) - hashStackStride
	len := hb.hashStack[pos] - 0x80
	if len > 32 {
		// node itself (RLP list), not its hash
		len = hb.hashStack[pos] - 0xc0
	}
	return hb.hashStack[pos : pos+1+int(len)]
}

func (hb *HashBuilder) printTopHashes(prefix []byte, _, children uint16) {
	digits := bits.OnesCount16(children)
	hashes := hb.hashStack[len(hb.hashStack)-hashStackStride*digits:]
	var i int
	for digit := uint(0); digit < 16; digit++ {
		if ((1 << digit) & children) != 0 {
			fmt.Printf("topHash: %x%02x, %x\n", prefix, digit, hashes[hashStackStride*i+1:hashStackStride*(i+1)])
			i++
		}
	}
}

func (hb *HashBuilder) topHashes(prefix []byte, hasHash, hasState uint16) []byte {
	digits := bits.OnesCount16(hasState)
	hashes := hb.hashStack[len(hb.hashStack)-hashStackStride*digits:]
	hb.topHashesCopy = hb.topHashesCopy[:0]
	for i := 0; hasHash > 0; hasState, hasHash = hasState>>1, hasHash>>1 {
		if 1&hasState == 0 {
			continue
		}

		if 1&hasHash != 0 {
			hb.topHashesCopy = append(hb.topHashesCopy, hashes[hashStackStride*i+1:hashStackStride*(i+1)]...)
		}
		i++
	}
	return hb.topHashesCopy
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
