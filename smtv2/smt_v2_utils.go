package smtv2

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/smt/pkg/utils"
	"github.com/status-im/keycard-go/hexutils"
)

const (
	KEY_BALANCE              = 0
	KEY_NONCE                = 1
	SC_CODE                  = 2
	SC_STORAGE               = 3
	SC_LENGTH                = 4
	HASH_POSEIDON_ALL_ZEROES = "0xc71603f33a1144ca7953db0ab48808f4c4055e3364a246c33c18a9786cb0b359"
	BYTECODE_ELEMENTS_HASH   = 8
	BYTECODE_BYTES_ELEMENT   = 7
)

var zeroCapacityHash [4]uint64

func init() {
	capacity, err := utils.StringToH4(HASH_POSEIDON_ALL_ZEROES)
	if err != nil {
		panic(err)
	}
	zeroCapacityHash = capacity
}

// lexographhicalCheckPaths checks if path1 is lexicographically less than path2
// it returns -1 if path1 is less than path2, 0 if they are equal, and 1 if path1 is greater than path2
func lexographhicalCheckPaths(path1, path2 []int) int {
	for i := 0; i < len(path1) && i < len(path2); i++ {
		if path1[i] < path2[i] {
			return -1
		} else if path1[i] > path2[i] {
			return 1
		}
	}
	if len(path1) < len(path2) {
		return -1
	} else if len(path1) > len(path2) {
		return 1
	}
	return 0
}

func lexographicalCompareKeys(k1, k2 [4]uint64) int {
	// Compare 4 bits at a time (one from each uint64)
	for bitPos := 0; bitPos < 64; bitPos++ {
		for i := 0; i < 4; i++ {
			bit1 := int((k1[i] >> bitPos) & 1)
			bit2 := int((k2[i] >> bitPos) & 1)
			if bit1 < bit2 {
				return -1
			} else if bit1 > bit2 {
				return 1
			}
		}
	}
	return 0
}

var terminatorPath = [257]int{2}

func InvertChangeSet(changeSet []ChangeSetEntry) []ChangeSetEntry {
	reversed := make([]ChangeSetEntry, len(changeSet))
	for i, entry := range changeSet {
		switch entry.Type {
		case ChangeSetEntryType_Add:
			reversed[i] = ChangeSetEntry{
				Type:  ChangeSetEntryType_Delete,
				Key:   entry.Key,
				Value: entry.Value,
			}
		case ChangeSetEntryType_Delete:
			reversed[i] = ChangeSetEntry{
				Type:  ChangeSetEntryType_Add,
				Key:   entry.Key,
				Value: entry.OriginalValue,
			}
		case ChangeSetEntryType_Change:
			reversed[i] = ChangeSetEntry{
				Type:          ChangeSetEntryType_Change,
				Key:           entry.Key,
				Value:         entry.OriginalValue,
				OriginalValue: entry.Value,
			}
		}
	}
	return reversed
}

func NextFastForwardPath(path []int) ([]int, bool) {
	// Create a copy of the original slice.
	result := make([]int, len(path))
	copy(result, path)

	carry := int(1)
	// Process from the least-significant bit (end of the slice)
	for i := len(result) - 1; i >= 0 && carry > 0; i-- {
		sum := result[i] + carry
		result[i] = sum & 1 // Set the bit to 0 or 1
		carry = sum >> 1    // Carry is 1 if sum was 2, otherwise 0
	}
	return result, carry > 0
}

func AddrKeyBalance(addr common.Address) SmtKey {
	return Key(addr.Hex(), KEY_BALANCE)
}

func AddrKeyNonce(addr common.Address) SmtKey {
	return Key(addr.Hex(), KEY_NONCE)
}

func AddrKeyCode(addr common.Address) SmtKey {
	return Key(addr.Hex(), SC_CODE)
}

func AddrKeyLength(addr common.Address) SmtKey {
	return Key(addr.Hex(), SC_LENGTH)
}

func Key(ethAddr string, c int) SmtKey {
	ethAddr = strings.TrimPrefix(ethAddr, "0x")

	// Convert hex string to bytes
	addrUint64s := [6]uint64{0, 0, 0, 0, 0, 0}

	// Process each byte pair in the hex string
	for i := 0; i < len(ethAddr); i += 2 {
		if i+2 > len(ethAddr) {
			break
		}
		// Convert hex byte pair to uint8
		b, err := strconv.ParseUint(ethAddr[i:i+2], 16, 8)
		if err != nil {
			return SmtKey{}
		}

		// Calculate position in uint64 array
		// When converting hex to uint64s, bytes are interpreted in big-endian order
		byteIndex := (len(ethAddr)/2 - 1 - i/2)
		chunkIndex := byteIndex / 4
		bytePos := byteIndex % 4

		// Place byte in correct position within its uint64 chunk
		addrUint64s[chunkIndex] |= uint64(b) << (bytePos * 8)
	}

	// Create the input array for hashing: [addr(6 uint64s), keyType, 0]
	input := [8]uint64{
		addrUint64s[0],
		addrUint64s[1],
		addrUint64s[2],
		addrUint64s[3],
		addrUint64s[4],
		addrUint64s[5],
		uint64(c),
		0,
	}

	// Hash and return
	return utils.Hash(input, zeroCapacityHash)
}

func KeyContractStorageWithoutBig(addr common.Address, storagePosition common.Hash) SmtKey {
	// First hash the storage position bytes
	// Convert the storage position hash to 8 uint64s for hashing
	spUint64s := [8]uint64{0, 0, 0, 0, 0, 0, 0, 0}
	spBytes := storagePosition.Bytes()

	// Process storage position bytes in reverse order to match ScalarToArrayBig behavior
	for i := 0; i < len(spBytes); i++ {
		// Which 32-bit chunk this byte belongs to
		chunkIndex := (31 - i) / 4
		// Position within the 32-bit chunk
		bytePos := (31 - i) % 4

		// Place byte in correct position within its 32-bit chunk
		spUint64s[chunkIndex] |= uint64(spBytes[i]) << (bytePos * 8)
	}

	// Hash the storage position with zero capacity
	hk0 := utils.Hash(spUint64s, [4]uint64{0, 0, 0, 0})

	// Process the address bytes similar to KeyWithoutBig
	addrBytes := addr.Bytes()
	addrUint64s := [6]uint64{0, 0, 0, 0, 0, 0}

	// Process address bytes in reverse order to match ScalarToArrayBig behavior
	for i := 0; i < 20; i++ {
		// Which 32-bit chunk this byte belongs to
		chunkIndex := (19 - i) / 4
		// Position within the 32-bit chunk
		bytePos := (19 - i) % 4

		// Place byte in correct position within its 32-bit chunk
		addrUint64s[chunkIndex] |= uint64(addrBytes[i]) << (bytePos * 8)
	}

	// Create the input array for hashing: [addr(6 uint64s), SC_STORAGE, 0]
	input := [8]uint64{
		addrUint64s[0],
		addrUint64s[1],
		addrUint64s[2],
		addrUint64s[3],
		addrUint64s[4],
		addrUint64s[5],
		uint64(SC_STORAGE),
		0,
	}

	// Hash with the storage position hash as capacity
	return utils.Hash(input, hk0)
}

func SmtKeyFromPath(path []int) (SmtKey, error) {
	if len(path) != 256 {
		return SmtKey{}, fmt.Errorf("path is not 256 bits")
	}

	res := [4]uint64{0, 0, 0, 0}

	for j := 0; j < 256; j++ {
		i := j % 4

		k := j / 4
		res[i] |= uint64(path[j]) << k
	}

	return res, nil
}

func RemoveKeyBits(k SmtKey, nBits int) SmtKey {
	var auxk SmtKey
	fullLevels := nBits / 4

	for i := 0; i < 4; i++ {
		n := fullLevels
		if fullLevels*4+i < nBits {
			n += 1
		}
		auxk[i] = k[i] >> uint(n)
	}

	return auxk
}

func UintToSmtKey(s uint64) SmtKey {
	var result SmtKey

	// Process each bit of the input uint64
	for bitPos := 0; bitPos < 64; bitPos++ {
		// Extract the current bit
		bit := (s >> uint(bitPos)) & 1

		// Calculate which of the 4 uint64s this bit belongs to
		// and what position within that uint64
		targetIdx := bitPos % 4
		shiftAmount := bitPos / 4

		// Set the bit in the appropriate position
		if bit == 1 {
			result[targetIdx] |= 1 << uint(shiftAmount)
		}
	}

	return result
}

func ScalarToSmtValue8FromBits(scalar *big.Int) SmtValue8 {
	out := [8]uint64{}

	// Since we're extracting 64 bits at a time, we can use Bits()
	// to get the underlying words and extract them directly
	words := scalar.Bits()

	// Each word in words is a uint containing _bits.UintSize bits (usually 64)
	// We need to copy them into our output array
	for i := 0; i < 8 && i < len(words); i++ {
		out[i] = uint64(words[i])
	}

	return out
}

func ScalarToSmtValue8FromBytes(scalar *big.Int) SmtValue8 {
	bytes := scalar.Bytes() // Get big-endian bytes
	return BytesToSmtValue8(bytes)
}

func BytesToSmtValue8(bytes []byte) SmtValue8 {
	var result [8]uint64
	// Start from the end of the bytes array
	bytePos := len(bytes) - 1
	wordIdx := 0

	// Process 4 bytes at a time
	for bytePos >= 0 && wordIdx < 8 {
		var word uint64
		// Build each 32-bit word
		for j := 0; j < 4 && bytePos >= 0; j++ {
			word |= uint64(bytes[bytePos]) << (j * 8)
			bytePos--
		}
		result[wordIdx] = word
		wordIdx++
	}

	return result
}

func HashToBytes(arr [4]uint64) []byte {
	// Allocate maximum possible size (4 uint64s = 32 bytes)
	result := make([]byte, 32)

	// Convert each uint64 to bytes in big-endian order
	for i := 0; i < 4; i++ {
		// Write each uint64 to the appropriate position
		// Starting from most significant (left-most) uint64
		val := arr[3-i]
		pos := i * 8

		// Convert uint64 to 8 bytes in big-endian order
		result[pos] = byte(val >> 56)
		result[pos+1] = byte(val >> 48)
		result[pos+2] = byte(val >> 40)
		result[pos+3] = byte(val >> 32)
		result[pos+4] = byte(val >> 24)
		result[pos+5] = byte(val >> 16)
		result[pos+6] = byte(val >> 8)
		result[pos+7] = byte(val)
	}

	// Trim leading zeros (like big.Int.Bytes() does)
	i := 0
	for i < len(result)-1 && result[i] == 0 {
		i++
	}

	return result[i:]
}

func GetCodeAndLength(code []byte) (SmtValue8, SmtValue8, error) {
	asHex := hexutils.BytesToHex(code)
	codeBig, codeLength, err := convertBytecodeToBigInt(asHex)
	if err != nil {
		return SmtValue8{}, SmtValue8{}, err
	}
	newContractCode := ScalarToSmtValue8FromBytes(codeBig)

	lenBig := big.NewInt(int64(codeLength))
	lenghNode8 := ScalarToSmtValue8FromBits(lenBig)

	return newContractCode, lenghNode8, nil
}

func GetCodeAndLengthNoBig(code []byte) (SmtValue8, SmtValue8, error) {
	if len(code) == 0 {
		return SmtValue8{}, SmtValue8{}, nil
	}

	asHex := hexutils.BytesToHex(code)

	interimHash := utils.CreateInterimBytecodeHash(asHex)
	interimBytes := HashToBytes(interimHash)
	codeValue := BytesToSmtValue8(interimBytes)

	parsedBytecode := strings.TrimPrefix(asHex, "0x")
	if len(parsedBytecode)%2 != 0 {
		parsedBytecode = "0" + parsedBytecode
	}
	codeLength := len(parsedBytecode) / 2

	// Convert codeLength to big-endian bytes
	lengthBytes := make([]byte, 8) // uint64 is 8 bytes
	binary.BigEndian.PutUint64(lengthBytes, uint64(codeLength))
	lengthValue := BytesToSmtValue8(lengthBytes)

	return codeValue, lengthValue, nil
}

func GetAllSharedPaths(path []int) [][]int {
	paths := [][]int{}

	initialSize := len(path)

	// get the initial length of the path
	length := path[len(path)-1]

	for i := 0; i < length; i++ {
		size := length - i
		nextPath := make([]int, initialSize)
		copy(nextPath, path[:size])
		nextPath[initialSize-1] = size
		paths = append(paths, nextPath)
	}

	return paths
}

func PathToKeyBytes(path []int, length int) []byte {
	result := [33]byte{}

	// Pack bits into bytes, 8 bits per byte
	for i := 0; i < 256; i++ {
		if i < len(path) {
			byteIndex := i / 8
			bitPosition := 7 - (i % 8) // Reverse bits within each byte
			if path[i] == 1 {
				result[byteIndex] |= 1 << bitPosition
			}
		}
	}

	result[32] = byte(length)
	return result[:]
}

func KeyBytesToPath(bytes []byte) ([]int, int, error) {
	if len(bytes) != 33 {
		return nil, 0, fmt.Errorf("invalid bytes length: expected 33, got %d", len(bytes))
	}

	length := int(bytes[32])
	path := make([]int, 257)

	// Unpack bits from bytes
	for i := 0; i < 256; i++ {
		byteIndex := i / 8
		bitPosition := 7 - (i % 8) // Reverse bits within each byte
		path[i] = int((bytes[byteIndex] >> bitPosition) & 1)
	}

	path[256] = length
	return path, length, nil
}
