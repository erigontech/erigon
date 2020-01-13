package trie

import (
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/trie/rlphacks"
	"github.com/ugorji/go/codec"
	"io"
	"math/big"
	"math/bits"
	"sort"
)

type StarkStatsBuilder struct {
	keccakCounter int         // Number of Keccak invocations
	perInputSize  map[int]int // Number of invocation for certain size of input
	sizeStack     []int       // Stack of input sizes
}

func NewStarkStatsBuilder() *StarkStatsBuilder {
	return &StarkStatsBuilder{
		keccakCounter: 0,
		perInputSize:  make(map[int]int),
	}
}

func (hb *StarkStatsBuilder) leafHash(length int, keyHex []byte, val RlpSerializable) error {
	key := keyHex[len(keyHex)-length:]
	var compactLen int
	var kp, kl int
	if hasTerm(key) {
		compactLen = (len(key)-1)/2 + 1
	} else {
		compactLen = len(key)/2 + 1
	}
	if compactLen > 1 {
		kp = 1
		kl = compactLen
	} else {
		kl = 1
	}
	totalLen := kp + kl + val.DoubleRLPLen()
	var lenPrefix [4]byte
	pt := rlphacks.GenerateStructLen(lenPrefix[:], totalLen)
	inputSize := totalLen + pt
	if inputSize > common.HashLength {
		inputSize = 32
	}
	hb.sizeStack = append(hb.sizeStack, inputSize)
	return nil
}

func (hb *StarkStatsBuilder) leaf(length int, keyHex []byte, val RlpSerializable) error {
	return hb.leafHash(length, keyHex, val)
}

func (hb *StarkStatsBuilder) extensionHash(key []byte) error {
	var kp, kl int
	var compactLen int
	if hasTerm(key) {
		compactLen = (len(key)-1)/2 + 1
	} else {
		compactLen = len(key)/2 + 1
	}
	if compactLen > 1 {
		kp = 1
		kl = compactLen
	} else {
		kl = 1
	}
	totalLen := kp + kl + 33
	var lenPrefix [4]byte
	pt := rlphacks.GenerateStructLen(lenPrefix[:], totalLen)
	inputSize := pt + totalLen
	hb.keccakCounter++
	hb.perInputSize[inputSize]++
	hb.sizeStack[len(hb.sizeStack)-1] = 32
	return nil
}

func (hb *StarkStatsBuilder) extension(key []byte) error {
	return hb.extensionHash(key)
}

func (hb *StarkStatsBuilder) branchHash(set uint16) error {
	digits := bits.OnesCount16(set)
	inputSizes := hb.sizeStack[len(hb.sizeStack)-digits:]
	totalLen := 17 // These are 17 length prefixes
	var i int
	for digit := uint(0); digit < 16; digit++ {
		if ((uint16(1) << digit) & set) != 0 {
			totalLen += inputSizes[i]
			i++
		}
	}
	var lenPrefix [4]byte
	pt := rlphacks.GenerateStructLen(lenPrefix[:], totalLen)
	inputSize := pt + totalLen
	hb.keccakCounter++
	hb.perInputSize[inputSize]++
	hb.sizeStack = hb.sizeStack[:len(hb.sizeStack)-digits+1]
	hb.sizeStack[len(hb.sizeStack)-1] = 32
	return nil
}

func (hb *StarkStatsBuilder) branch(set uint16) error {
	return hb.branchHash(set)
}

func (hb *StarkStatsBuilder) hash(hash common.Hash) error {
	hb.sizeStack = append(hb.sizeStack, 32)
	return nil
}

func (hb *StarkStatsBuilder) code(code []byte) (common.Hash, error) {
	hb.sizeStack = append(hb.sizeStack, 32)
	return common.Hash{}, nil
}

func (hb *StarkStatsBuilder) accountLeafHash(length int, keyHex []byte, storageSize uint64, balance *big.Int, nonce uint64, fieldSet uint32) (err error) {
	key := keyHex[len(keyHex)-length:]
	var acc accounts.Account
	acc.Root = EmptyRoot
	acc.CodeHash = EmptyCodeHash
	acc.Nonce = nonce
	acc.Balance.Set(balance)
	acc.Initialised = true
	if fieldSet&uint32(4) == 0 && fieldSet&uint32(8) == 0 {
		// In this case we can precompute the hash of the entire account leaf
		hb.sizeStack = append(hb.sizeStack, 32)
	} else {
		if fieldSet&uint32(4) != 0 {
			hb.sizeStack = hb.sizeStack[:len(hb.sizeStack)-1]
		}
		if fieldSet&uint32(8) != 0 {
			hb.sizeStack = hb.sizeStack[:len(hb.sizeStack)-1]
		}
	}
	var kp, kl int
	var compactLen int
	if hasTerm(key) {
		compactLen = (len(key)-1)/2 + 1
	} else {
		compactLen = len(key)/2 + 1
	}
	if compactLen > 1 {
		kp = 1
		kl = compactLen
	} else {
		kl = 1
	}
	valLen := acc.EncodingLengthForHashing()
	valBuf := make([]byte, valLen)
	acc.EncodeForHashing(valBuf)
	val := rlphacks.RlpEncodedBytes(valBuf)
	totalLen := kp + kl + val.DoubleRLPLen()
	var lenPrefix [4]byte
	pt := rlphacks.GenerateStructLen(lenPrefix[:], totalLen)
	inputSize := pt + totalLen
	hb.keccakCounter++
	hb.perInputSize[inputSize]++
	hb.sizeStack = append(hb.sizeStack, 32)
	return nil
}

func (hb *StarkStatsBuilder) accountLeaf(length int, keyHex []byte, storageSize uint64, balance *big.Int, nonce uint64, fieldSet uint32) (err error) {
	return hb.accountLeafHash(length, keyHex, storageSize, balance, nonce, fieldSet)
}

func (hb *StarkStatsBuilder) emptyRoot() {
	hb.sizeStack = append(hb.sizeStack, 32)
}

func StarkStats(bw []byte, w io.Writer, trace bool) error {
	var lens map[string]int
	var handle codec.CborHandle
	decoder := codec.NewDecoderBytes(bw, &handle)
	if err := decoder.Decode(&lens); err != nil {
		return err
	}
	hb := NewStarkStatsBuilder()
	// It is important to read the tapes in the same order as they were written
	startOffset := decoder.NumBytesRead()
	endOffset := startOffset + lens[KeyTape]

	keyTape := NewCborBytesTape(bw[startOffset:endOffset])

	startOffset = endOffset
	endOffset = startOffset + lens[ValueTape]
	valueTape := NewRlpSerializableBytesTape(NewCborBytesTape(bw[startOffset:endOffset]))

	startOffset = endOffset
	endOffset = startOffset + lens[NonceTape]
	nonceTape := NewCborUint64Tape(bw[startOffset:endOffset])
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
			return err
		}
		hashOnly := false
		switch opcode {
		case OpLeafHash:
			hashOnly = true
			opcode = OpLeaf
			fallthrough
		case OpLeaf:
			if trace {
				if hashOnly {
					fmt.Printf("LEAFHASH ")
				} else {
					fmt.Printf("LEAF ")
				}
			}
			var length int
			if err := decoder.Decode(&length); err != nil {
				return err
			}
			keyHex, err := keyTape.Next()
			if err != nil {
				return err
			}
			val, err := valueTape.Next()
			if err != nil {
				return err
			}
			if hashOnly {
				if err := hb.leafHash(length, keyHex, val); err != nil {
					return err
				}
			} else {
				if err := hb.leaf(length, keyHex, val); err != nil {
					return err
				}
			}
		case OpExtensionHash:
			hashOnly = true
			opcode = OpExtension
			fallthrough
		case OpExtension:
			if trace {
				if hashOnly {
					fmt.Printf("EXTENSIONHASH ")
				} else {
					fmt.Printf("EXTENSION ")
				}
			}
			var key []byte
			if err := decoder.Decode(&key); err != nil {
				return err
			}
			if hashOnly {
				if err := hb.extensionHash(key); err != nil {
					return err
				}
			} else {
				if err := hb.extension(key); err != nil {
					return err
				}
			}

		case OpBranchHash:
			hashOnly = true
			opcode = OpBranch
			fallthrough
		case OpBranch:
			if trace {
				if hashOnly {
					fmt.Printf("BRANCHHASH ")
				} else {
					fmt.Printf("BRANCH ")
				}
			}
			var set uint16
			if err := decoder.Decode(&set); err != nil {
				return err
			}
			if hashOnly {
				if err := hb.branchHash(set); err != nil {
					return err
				}
			} else {
				if err := hb.branch(set); err != nil {
					return err
				}
			}
		case OpHash:
			if trace {
				fmt.Printf("HASH ")
			}
			var number int
			if err := decoder.Decode(&number); err != nil {
				return err
			}
			for i := 0; i < number; i++ {
				hash, err := hashTape.Next()
				if err != nil {
					return err
				}
				if err := hb.hash(hash); err != nil {
					return err
				}
			}
		case OpCode:
			if trace {
				fmt.Printf("CODE ")
			}
			code, err := codeTape.Next()
			if err != nil {
				return err
			}
			if _, err := hb.code(code); err != nil {
				return err
			}

		case OpAccountLeafHash:
			hashOnly = true
			opcode = OpAccountLeaf
			fallthrough
		case OpAccountLeaf:
			var length int
			var fieldSet uint32
			if err := decoder.Decode(&length); err != nil {
				return err
			}
			if err := decoder.Decode(&fieldSet); err != nil {
				return err
			}
			if trace {
				if hashOnly {
					fmt.Printf("ACCOUNTLEAFHASH (%b)", fieldSet)
				} else {
					fmt.Printf("ACCOUNTLEAF(%b) ", fieldSet)
				}
			}
			keyHex, err := keyTape.Next()
			if err != nil {
				return err
			}
			balance := big.NewInt(0)
			if fieldSet&uint32(2) != 0 {
				balance, err = balanceTape.Next()
				if err != nil {
					return err
				}
			}
			nonce := uint64(0)
			if fieldSet&uint32(1) != 0 {
				nonce, err = nonceTape.Next()
				if err != nil {
					return err
				}
			}
			if hashOnly {

				if err := hb.accountLeafHash(length, keyHex, 0, balance, nonce, fieldSet); err != nil {
					return err
				}
			} else {
				if err := hb.accountLeaf(length, keyHex, 0, balance, nonce, fieldSet); err != nil {
					return err
				}
			}
		case OpEmptyRoot:
			if trace {
				fmt.Printf("EMPTYROOT ")
			}
			hb.emptyRoot()
		default:
			return fmt.Errorf("unknown opcode: %d", opcode)
		}
	}
	if trace {
		fmt.Printf("\n")
	}

	inputSizes := make([]int, len(hb.perInputSize))
	i := 0
	for inputSize := range hb.perInputSize {
		inputSizes[i] = inputSize
		i++
	}
	sort.Ints(inputSizes)
	fmt.Fprintf(w, "%d\n", hb.keccakCounter)
	for _, inputSize := range inputSizes {
		fmt.Fprintf(w, "%d %d\n", inputSize, hb.perInputSize[inputSize])
	}
	return nil
}
