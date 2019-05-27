package accounts

import (
	"fmt"
	"io"
	"math/big"
	"math/bits"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/pool"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

// Account is the Ethereum consensus representation of accounts.
// These objects are stored in the main account trie.
// DESCRIBED: docs/programmers_guide/guide.md#ethereum-state
type Account struct {
	Initialised    bool
	Nonce          uint64
	Balance        big.Int
	Root           common.Hash // merkle root of the storage trie
	CodeHash       common.Hash // hash of the bytecode
	Incarnation    uint64
	HasStorageSize bool
	StorageSize    uint64
}

var emptyCodeHash = crypto.Keccak256Hash(nil)
var emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
var b128 = big.NewInt(128)

// NewAccount creates a new account w/o code nor storage.
func NewAccount() Account {
	a := Account{}
	a.Root = emptyRoot
	a.CodeHash = emptyCodeHash
	return a
}

func (a *Account) encodingLength(forStorage bool) uint {
	var structLength uint
	var nonContract = a.IsEmptyCodeHash() && a.IsEmptyRoot()
	if forStorage && nonContract && a.Balance.Sign() == 0 && a.Nonce == 0 && a.Incarnation == 0 {
		return 1
	}

	if !forStorage || !nonContract || a.Balance.Sign() != 0 || a.Nonce != 0 || a.Incarnation != 0 {
		var balanceBytes int
		if b128.Cmp(&a.Balance) == 1 && a.Balance.Sign() == 1 {
			balanceBytes = 0
		} else {
			balanceBytes = (a.Balance.BitLen() + 7) / 8
		}

		var nonceBytes int
		if a.Nonce < 128 && a.Nonce != 0 {
			nonceBytes = 0
		} else {
			nonceBytes = (bits.Len64(a.Nonce) + 7) / 8
		}

		structLength += uint(balanceBytes + nonceBytes + 2)
	}

	if !forStorage || !nonContract {
		structLength += 66 // Two 32-byte arrays + 2 prefixes
	}

	if a.HasStorageSize {
		var storageSizeBytes int
		if a.StorageSize < 128 && a.StorageSize != 0 {
			storageSizeBytes = 0
		} else {
			storageSizeBytes = (bits.Len64(a.StorageSize) + 7) / 8
		}
		structLength += uint(storageSizeBytes + 1)
	}

	if forStorage {
		var incarnationsBytes int
		if a.Incarnation < 128 && a.Incarnation != 0 {
			incarnationsBytes = 0
		} else {
			incarnationsBytes = (bits.Len64(a.Incarnation) + 7) / 8
		}
		structLength += uint(incarnationsBytes + 1)
	}

	if structLength < 56 {
		return 1 + structLength
	}

	lengthBytes := (bits.Len(structLength) + 7) / 8

	return uint(1+lengthBytes) + structLength
}

func (a *Account) EncodingLengthForStorage() uint {
	return a.encodingLength(true)
}

func (a *Account) EncodingLengthForHashing() uint {
	return a.encodingLength(false)
}

func (a *Account) encode(buffer []byte, forStorage bool) {
	var nonContract = a.IsEmptyCodeHash() && a.IsEmptyRoot()
	if forStorage && nonContract && a.Balance.Sign() == 0 && a.Nonce == 0 && a.Incarnation == 0 {
		buffer[0] = 192
		return
	}

	var balanceBytes int
	if b128.Cmp(&a.Balance) == 1 && a.Balance.Sign() == 1 {
		balanceBytes = 0
	} else {
		balanceBytes = (a.Balance.BitLen() + 7) / 8
	}

	var nonceBytes int
	if a.Nonce < 128 && a.Nonce != 0 {
		nonceBytes = 0
	} else {
		nonceBytes = (bits.Len64(a.Nonce) + 7) / 8
	}

	var structLength = uint(balanceBytes + nonceBytes + 2)
	if !forStorage || !nonContract {
		structLength += 66 // Two 32-byte arrays + 2 prefixes
	}

	var incarnationBytes int
	if forStorage {
		if a.Incarnation < 128 && a.Incarnation != 0 {
			incarnationBytes = 0
		} else {
			incarnationBytes = (bits.Len64(a.Incarnation) + 7) / 8
		}
		structLength += uint(incarnationBytes + 1)
	}

	var storageSizeBytes int
	if a.HasStorageSize {
		if a.StorageSize < 128 && a.StorageSize != 0 {
			storageSizeBytes = 0
		} else {
			storageSizeBytes = (bits.Len64(a.StorageSize) + 7) / 8
		}

		structLength += uint(storageSizeBytes + 1)
	}

	var pos int
	if structLength < 56 {
		buffer[0] = byte(192 + structLength)
		pos = 1
	} else {
		lengthBytes := (bits.Len(structLength) + 7) / 8
		buffer[0] = byte(247 + lengthBytes)

		for i := lengthBytes; i > 0; i-- {
			buffer[i] = byte(structLength)
			structLength >>= 8
		}

		pos = lengthBytes + 1
	}

	// Encoding nonce
	if a.Nonce < 128 && a.Nonce != 0 {
		buffer[pos] = byte(a.Nonce)
	} else {
		buffer[pos] = byte(128 + nonceBytes)
		var nonce = a.Nonce
		for i := nonceBytes; i > 0; i-- {
			buffer[pos+i] = byte(nonce)
			nonce >>= 8
		}
	}
	pos += 1 + nonceBytes

	// Encoding balance
	if b128.Cmp(&a.Balance) == 1 && a.Balance.Sign() == 1 {
		buffer[pos] = byte(a.Balance.Uint64())
		pos++
	} else {
		buffer[pos] = byte(128 + balanceBytes)
		pos++
		balanceWords := a.Balance.Bits()
		i := pos + balanceBytes
		for _, d := range balanceWords {
			for j := 0; j < bits.UintSize/8; j++ {
				if i == pos {
					break
				}
				i--
				buffer[i] = byte(d)
				d >>= 8
			}
		}
		pos += balanceBytes
	}

	if forStorage {
		if a.Incarnation < 128 && a.Incarnation != 0 {
			buffer[pos] = byte(a.Incarnation)
		} else {
			buffer[pos] = byte(128 + incarnationBytes)
			var incarnation = a.Incarnation
			for i := incarnationBytes; i > 0; i-- {
				buffer[pos+i] = byte(incarnation)
				incarnation >>= 8
			}
		}
		pos += 1 + incarnationBytes
	}

	// Encoding Root and CodeHash
	if !forStorage || !nonContract {
		buffer[pos] = 128 + 32
		pos++
		copy(buffer[pos:], a.Root[:])
		pos += 32
		buffer[pos] = 128 + 32
		pos++
		copy(buffer[pos:], a.CodeHash[:])
		pos += 32
	}

	// Encoding StorageSize
	if a.HasStorageSize {
		if a.StorageSize < 128 && a.StorageSize != 0 {
			buffer[pos] = byte(a.StorageSize)
		} else {
			buffer[pos] = byte(128 + storageSizeBytes)
			storageSize := a.StorageSize
			for i := storageSizeBytes; i > 0; i-- {
				buffer[pos+i] = byte(storageSize)
				storageSize >>= 8
			}
		}
		// Commented out because of the ineffectual assignment - uncomment if adding more fields
		//pos += 1 + storageSizeBytes
	}
}

func (a *Account) EncodeRLP(w io.Writer) error {
	len := a.encodingLength(false)
	buffer := pool.GetBuffer(len)
	a.encode(buffer.Bytes(), false)
	_, err := w.Write(buffer.Bytes())
	pool.PutBuffer(buffer)
	return err
}

func (a *Account) EncodeForStorage(buffer []byte) {
	a.encode(buffer, true)
}

func (a *Account) EncodeForHashing(buffer []byte) {
	a.encode(buffer, false)
}

func (a *Account) Copy(image *Account) {
	a.Initialised = image.Initialised
	a.Nonce = image.Nonce
	a.Balance.Set(&image.Balance)
	a.Root = image.Root
	a.CodeHash = image.CodeHash
	a.Incarnation = image.Incarnation
	a.HasStorageSize = image.HasStorageSize
	a.StorageSize = image.StorageSize
	a.Incarnation = image.Incarnation
}

// Decodes length and determines whether it corresponds to a structure of a byte array
func decodeLength(buffer []byte, pos int) (length int, structure bool, newPos int) {
	switch firstByte := int(buffer[pos]); {
	case firstByte < 128:
		return 0, false, pos
	case firstByte < 184:
		return firstByte - 128, false, pos + 1
	case firstByte < 192:
		// Next byte is the length of the length + 183
		lenEnd := pos + 1 + firstByte - 183
		len := 0
		for i := pos + 1; i < lenEnd; i++ {
			len = (len << 8) + int(buffer[i])
		}
		return len, false, lenEnd
	case firstByte < 248:
		return firstByte - 192, true, pos + 1
	default:
		// Next byte is the length of the length + 247
		lenEnd := pos + 1 + firstByte - 247
		len := 0
		for i := pos + 1; i < lenEnd; i++ {
			len = (len << 8) + int(buffer[i])
		}
		return len, true, lenEnd
	}
}

func (a *Account) DecodeForStorage(enc []byte) error {
	return a.decode(enc, true)
}

func (a *Account) DecodeForHashing(enc []byte) error {
	return a.decode(enc, false)
}

func (a *Account) decode(enc []byte, forStorage bool) error {
	length, structure, pos := decodeLength(enc, 0)
	if pos+length != len(enc) {
		return fmt.Errorf(
			"malformed RLP for Account(%x): prefixLength(%d) + dataLength(%d) != sliceLength(%d)",
			enc, pos, length, len(enc))
	}
	if !structure {
		return fmt.Errorf(
			"encoding of Account should be RLP struct, got byte array: %x",
			enc,
		)
	}

	a.Initialised = true
	a.Nonce = 0
	a.Balance.SetInt64(0)
	a.Root = emptyRoot
	a.CodeHash = emptyCodeHash
	a.StorageSize = 0
	a.HasStorageSize = false
	if length == 0 && structure {
		return nil
	}

	if pos < len(enc) {
		nonceBytes, s, newPos := decodeLength(enc, pos)
		if s {
			return fmt.Errorf(
				"encoding of Account.Nonce should be byte array, got RLP struct: %x",
				enc[pos:newPos+nonceBytes],
			)
		}

		if newPos+nonceBytes > len(enc) {
			return fmt.Errorf(
				"malformed RLP for Account.Nonce(%x): prefixLength(%d) + dataLength(%d) >= sliceLength(%d)",
				enc[pos:newPos+nonceBytes],
				newPos-pos, nonceBytes, len(enc)-pos,
			)
		}

		var nonce uint64
		if nonceBytes == 0 && newPos == pos {
			nonce = uint64(enc[newPos])
			pos = newPos + 1
		} else {
			for _, b := range enc[newPos : newPos+nonceBytes] {
				nonce = (nonce << 8) + uint64(b)
			}
			pos = newPos + nonceBytes
		}

		a.Nonce = nonce
	}
	if pos < len(enc) {
		balanceBytes, s, newPos := decodeLength(enc, pos)
		if s {
			return fmt.Errorf(
				"encoding of Account.Balance should be byte array, got RLP struct: %x",
				enc[pos:newPos+balanceBytes],
			)
		}

		if newPos+balanceBytes > len(enc) {
			return fmt.Errorf("malformed RLP for Account.Balance(%x): prefixLength(%d) + dataLength(%d) >= sliceLength(%d)",
				enc[pos],
				newPos-pos, balanceBytes, len(enc)-pos,
			)
		}

		var bigB big.Int
		if balanceBytes == 0 && newPos == pos {
			a.Balance.SetInt64(int64(enc[newPos]))
			pos = newPos + 1
		} else {
			for _, b := range enc[newPos : newPos+balanceBytes] {
				a.Balance.Lsh(&a.Balance, 8)
				bigB.SetUint64(uint64(b))
				a.Balance.Add(&a.Balance, &bigB)
			}
			pos = newPos + balanceBytes
		}
	}

	// Skip incarnation decoding if this is not read from storage
	if forStorage && pos < len(enc) {
		incarnationBytes, s, newPos := decodeLength(enc, pos)
		if s {
			return fmt.Errorf(
				"encoding of Account.Incarnation should be byte array, got RLP struct: %x",
				enc[pos:newPos+incarnationBytes],
			)
		}

		if newPos+incarnationBytes > len(enc) {
			return fmt.Errorf(
				"malformed RLP for Account.Incarnation(%x): prefixLength(%d) + dataLength(%d) >= sliceLength(%d)",
				enc[pos:newPos+incarnationBytes],
				newPos-pos, incarnationBytes, len(enc)-pos,
			)
		}

		var incarnation uint64
		if incarnationBytes == 0 && newPos == pos {
			incarnation = uint64(enc[newPos])
			pos = newPos + 1
		} else {
			for _, b := range enc[newPos : newPos+incarnationBytes] {
				incarnation = (incarnation << 8) + uint64(b)
			}
			pos = newPos + incarnationBytes
		}

		a.Incarnation = incarnation
	}

	if pos < len(enc) {
		rootBytes, s, newPos := decodeLength(enc, pos)
		if s {
			return fmt.Errorf(
				"encoding of Account.Root should be byte array, got RLP struct: %x",
				enc[pos:newPos+rootBytes],
			)
		}

		if rootBytes != 32 {
			return fmt.Errorf(
				"encoding of Account.Root should have size 32, got %d",
				rootBytes,
			)
		}

		if newPos+rootBytes > len(enc) {
			return fmt.Errorf("malformed RLP for Account.Root(%x): prefixLength(%d) + dataLength(%d) >= sliceLength(%d)",
				enc[pos],
				newPos-pos, rootBytes, len(enc)-pos,
			)
		}

		copy(a.Root[:], enc[newPos:newPos+rootBytes])
		pos = newPos + rootBytes
	}

	if pos < len(enc) {
		codeHashBytes, s, newPos := decodeLength(enc, pos)
		if s {
			return fmt.Errorf(
				"encoding of Account.CodeHash should be byte array, got RLP struct: %x",
				enc[pos:newPos+codeHashBytes],
			)
		}

		if codeHashBytes != 32 {
			return fmt.Errorf(
				"encoding of Account.CodeHash should have size 32, got %d",
				codeHashBytes,
			)
		}

		if newPos+codeHashBytes > len(enc) {
			return fmt.Errorf("malformed RLP for Account.CodeHash(%x): prefixLength(%d) + dataLength(%d) >= sliceLength(%d)",
				enc[pos:newPos+codeHashBytes],
				newPos-pos, codeHashBytes, len(enc)-pos,
			)
		}

		copy(a.CodeHash[:], enc[newPos:newPos+codeHashBytes])
		pos = newPos + codeHashBytes
	}

	if pos < len(enc) {
		storageSizeBytes, s, newPos := decodeLength(enc, pos)
		if s {
			return fmt.Errorf(
				"encoding of Account.StorageSize should be byte array, got RLP struct: %x",
				enc[pos:newPos+storageSizeBytes],
			)
		}

		if newPos+storageSizeBytes > len(enc) {
			return fmt.Errorf(
				"malformed RLP for Account.StorageSize(%x): prefixLength(%d) + dataLength(%d) >= sliceLength(%d)",
				enc[pos:newPos+storageSizeBytes],
				newPos-pos, storageSizeBytes, len(enc)-pos,
			)
		}

		var storageSize uint64
		if storageSizeBytes == 0 && newPos == pos {
			storageSize = uint64(enc[newPos])
			// Commented out because of the ineffectual assignment - uncomment if adding more fields
			//pos = newPos + 1
		} else {
			for _, b := range enc[newPos : newPos+storageSizeBytes] {
				storageSize = (storageSize << 8) + uint64(b)
			}
			// Commented out because of the ineffectual assignment - uncomment if adding more fields
			//pos = newPos + storageSizeBytes
		}

		a.StorageSize = storageSize
		a.HasStorageSize = true
	}
	return nil
}

func (a *Account) DecodeRLP(s *rlp.Stream) error {
	raw, err := s.Raw()
	if err != nil {
		return err
	}

	err = a.DecodeForHashing(raw)
	return err
}

func (a *Account) IsEmptyCodeHash() bool {
	return a.CodeHash == emptyCodeHash || a.CodeHash == (common.Hash{})
}

func (a *Account) IsEmptyRoot() bool {
	return a.Root == emptyRoot || a.Root == common.Hash{}
}

func (a *Account) GetIncarnation() uint64 {
	return a.Incarnation
}

func (a *Account) SetIncarnation(v uint64) {
	a.Incarnation = v
}

func (a *Account) Equals(acc *Account) bool {
	return a.Nonce == acc.Nonce &&
		a.CodeHash == acc.CodeHash &&
		a.Root == acc.Root &&
		a.Balance.Cmp(&acc.Balance) == 0 &&
		a.Incarnation == acc.Incarnation &&
		a.HasStorageSize == acc.HasStorageSize &&
		a.StorageSize == acc.StorageSize

}
