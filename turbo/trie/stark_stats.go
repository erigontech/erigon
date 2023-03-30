package trie

import (
	"fmt"
	"io"
	"math/bits"
	"sort"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	len2 "github.com/ledgerwatch/erigon-lib/common/length"

	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/turbo/rlphacks"
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

func (hb *StarkStatsBuilder) leafHash(length int, keyHex []byte, val rlphacks.RlpSerializable) error {
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
	if inputSize > len2.Hash {
		inputSize = 32
	}
	hb.sizeStack = append(hb.sizeStack, inputSize)
	return nil
}

func (hb *StarkStatsBuilder) leaf(length int, keyHex []byte, val rlphacks.RlpSerializable) error {
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
		if ((1 << digit) & set) != 0 {
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

func (hb *StarkStatsBuilder) hash(_ libcommon.Hash) {
	hb.sizeStack = append(hb.sizeStack, 32)
}

func (hb *StarkStatsBuilder) code(_ []byte) libcommon.Hash {
	hb.sizeStack = append(hb.sizeStack, 32)
	return libcommon.Hash{}
}

func (hb *StarkStatsBuilder) accountLeafHash(length int, keyHex []byte, _ uint64, balance *uint256.Int, nonce uint64, fieldSet uint32) (err error) {
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

func (hb *StarkStatsBuilder) accountLeaf(length int, keyHex []byte, storageSize uint64, balance *uint256.Int, nonce uint64, _ uint64, fieldSet uint32) (err error) {
	return hb.accountLeafHash(length, keyHex, storageSize, balance, nonce, fieldSet)
}

func (hb *StarkStatsBuilder) emptyRoot() {
	hb.sizeStack = append(hb.sizeStack, 32)
}

// StarkStats collects Keccak256 stats from the witness and write them into the file
func StarkStats(witness *Witness, w io.Writer, trace bool) error {
	hb := NewStarkStatsBuilder()

	for _, operator := range witness.Operators {
		switch op := operator.(type) {
		case *OperatorLeafValue:
			if trace {
				fmt.Printf("LEAF ")
			}
			keyHex := op.Key
			val := op.Value
			if err := hb.leaf(len(op.Key), keyHex, rlphacks.RlpSerializableBytes(val)); err != nil {
				return err
			}
		case *OperatorExtension:
			if trace {
				fmt.Printf("EXTENSION ")
			}
			if err := hb.extension(op.Key); err != nil {
				return err
			}
		case *OperatorBranch:
			if trace {
				fmt.Printf("BRANCH ")
			}
			if err := hb.branch(uint16(op.Mask)); err != nil {
				return err
			}
		case *OperatorHash:
			if trace {
				fmt.Printf("HASH ")
			}
			hb.hash(op.Hash)
		case *OperatorCode:
			if trace {
				fmt.Printf("CODE ")
			}

			hb.code(op.Code)

		case *OperatorLeafAccount:
			if trace {
				fmt.Printf("ACCOUNTLEAF(code=%v storage=%v) ", op.HasCode, op.HasStorage)
			}
			balance := uint256.NewInt(0)
			balance.SetBytes(op.Balance.Bytes())
			nonce := op.Nonce

			// FIXME: probably not needed, fix hb.accountLeaf
			fieldSet := uint32(3)
			if op.HasCode && op.HasStorage {
				fieldSet = 15
			}

			// Incarnation is always needed for a hashbuilder.
			// but it is just our implementation detail needed for contract self-destruction support with our
			// db structure. Stateless clients don't access the DB so we can just pass 0 here.
			incarnation := uint64(0)

			if err := hb.accountLeaf(len(op.Key), op.Key, 0, balance, nonce, incarnation, fieldSet); err != nil {
				return err
			}
		case *OperatorEmptyRoot:
			if trace {
				fmt.Printf("EMPTYROOT ")
			}
			hb.emptyRoot()
		default:
			return fmt.Errorf("unknown operand type: %T", operator)
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
