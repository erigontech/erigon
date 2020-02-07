package accounts

import (
	"fmt"
	"io"
	"math/big"
	"math/bits"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/pool"
	"github.com/ledgerwatch/turbo-geth/crypto"
)

// Account is the Ethereum consensus representation of accounts.
// These objects are stored in the main account trie.
// DESCRIBED: docs/programmers_guide/guide.md#ethereum-state
type Account struct {
	Initialised    bool
	HasStorageSize bool
	Nonce          uint64
	Balance        big.Int
	Root           common.Hash // merkle root of the storage trie
	CodeHash       common.Hash // hash of the bytecode
	Incarnation    uint64
	StorageSize    uint64
}

var emptyCodeHash = crypto.Keccak256Hash(nil)
var emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
var b128 = big.NewInt(128)
var b0 = big.NewInt(0)

// NewAccount creates a new account w/o code nor storage.
func NewAccount() Account {
	a := Account{}
	a.Root = emptyRoot
	a.CodeHash = emptyCodeHash
	return a
}

func bytesToUint64(buf []byte) (x uint64) {
	for i, b := range buf {
		x = x<<8 + uint64(b)
		if i == 7 {
			return
		}
	}
	return
}

func (a *Account) EncodingLengthForStorage() uint {
	var structLength uint = 1 // 1 byte for fieldset

	if b0.Cmp(&a.Balance) == -1 {
		structLength += uint((a.Balance.BitLen()+7)/8) + 1
	}

	if a.Nonce > 0 {
		structLength += uint((bits.Len64(a.Nonce)+7)/8) + 1
	}

	if !a.IsEmptyRoot() {
		structLength += 33 // 32-byte array + 1 bytes for length
	}

	if !a.IsEmptyCodeHash() {
		structLength += 33 // 32-byte array + 1 bytes for length
	}

	if a.HasStorageSize {
		structLength += uint((bits.Len64(a.StorageSize)+7)/8) + 1
	}

	if a.Incarnation > 0 {
		structLength += uint((bits.Len64(a.Incarnation)+7)/8) + 1
	}

	return structLength
}

func (a *Account) EncodingLengthForHashing() uint {
	var structLength uint

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

	structLength += 66 // Two 32-byte arrays + 2 prefixes

	if a.HasStorageSize {
		var storageSizeBytes int
		if a.StorageSize < 128 && a.StorageSize != 0 {
			storageSizeBytes = 0
		} else {
			storageSizeBytes = (bits.Len64(a.StorageSize) + 7) / 8
		}
		structLength += uint(storageSizeBytes + 1)
	}

	if structLength < 56 {
		return 1 + structLength
	}

	lengthBytes := (bits.Len(structLength) + 7) / 8

	return uint(1+lengthBytes) + structLength
}

func (a *Account) EncodeForStorage(buffer []byte) {
	var fieldSet = 0 // start with first bit set to 0
	var pos = 1
	if a.Nonce > 0 {
		fieldSet = 1
		nonceBytes := (bits.Len64(a.Nonce) + 7) / 8
		buffer[pos] = byte(nonceBytes)
		var nonce = a.Nonce
		for i := nonceBytes; i > 0; i-- {
			buffer[pos+i] = byte(nonce)
			nonce >>= 8
		}
		pos += nonceBytes + 1
	}

	// Encoding balance
	if b0.Cmp(&a.Balance) == -1 {
		fieldSet |= 2
		balanceBytes := (a.Balance.BitLen() + 7) / 8
		buffer[pos] = byte(balanceBytes)
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

	if a.Incarnation > 0 {
		fieldSet |= 4
		incarnationBytes := (bits.Len64(a.Incarnation) + 7) / 8
		buffer[pos] = byte(incarnationBytes)
		var incarnation = a.Incarnation
		for i := incarnationBytes; i > 0; i-- {
			buffer[pos+i] = byte(incarnation)
			incarnation >>= 8
		}
		pos += incarnationBytes + 1
	}

	// Encoding Root
	if !a.IsEmptyRoot() {
		fieldSet |= 8
		buffer[pos] = 32
		copy(buffer[pos+1:], a.Root.Bytes())
		pos += 33
	}

	// Encoding CodeHash
	if !a.IsEmptyCodeHash() {
		fieldSet |= 16
		buffer[pos] = 32
		copy(buffer[pos+1:], a.CodeHash.Bytes())
		pos += 33
	}
	// Encoding StorageSize
	if a.HasStorageSize {
		fieldSet |= 32
		storageSizeBytes := (bits.Len64(a.StorageSize) + 7) / 8
		buffer[pos] = byte(storageSizeBytes)
		var storageSize = a.StorageSize
		for i := storageSizeBytes; i > 0; i-- {
			buffer[pos+i] = byte(storageSize)
			storageSize >>= 8
		}
		// pos += storageSizeBytes + 1
	}

	buffer[0] = byte(fieldSet)
}

func (a *Account) EncodeRLP(w io.Writer) error {
	len := a.EncodingLengthForHashing()
	buffer := pool.GetBuffer(len)
	a.EncodeForHashing(buffer.Bytes())
	_, err := w.Write(buffer.Bytes())
	pool.PutBuffer(buffer)
	return err
}

func (a *Account) EncodeForHashing(buffer []byte) {

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
	structLength += 66 // Two 32-byte arrays + 2 prefixes

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

	// Encoding Root and CodeHash
	buffer[pos] = 128 + 32
	pos++
	copy(buffer[pos:], a.Root[:])
	pos += 32
	buffer[pos] = 128 + 32
	pos++
	copy(buffer[pos:], a.CodeHash[:])
	pos += 32

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

// Copy makes `a` a full, independent (meaning that if the `image` changes in any way, it does not affect `a`) copy of the account `image`.
func (a *Account) Copy(image *Account) {
	a.Initialised = image.Initialised
	a.Nonce = image.Nonce
	a.Balance.Set(&image.Balance)
	copy(a.Root[:], image.Root[:])
	copy(a.CodeHash[:], image.CodeHash[:])
	a.Incarnation = image.Incarnation
	a.HasStorageSize = image.HasStorageSize
	a.StorageSize = image.StorageSize
	a.Incarnation = image.Incarnation
}

func (a *Account) Reset() {
	a.Initialised = true
	a.Nonce = 0
	a.Incarnation = 0
	a.Balance.SetInt64(0)
	copy(a.Root[:], emptyRoot[:])
	copy(a.CodeHash[:], emptyCodeHash[:])
	a.StorageSize = 0
	a.HasStorageSize = false
}

func (a *Account) DecodeForStorage(enc []byte) error {
	a.Reset()

	var fieldSet = enc[0]
	var pos = 1
	if len(enc) == 0 {
		return nil
	}

	if fieldSet&1 > 0 {
		decodeLength := int(enc[pos])

		if len(enc) < pos+decodeLength+1 {
			return fmt.Errorf(
				"malformed CBOR for Account.Nonce: %s, Length %d",
				enc[pos+1:], decodeLength)
		}

		a.Nonce = bytesToUint64(enc[pos+1 : pos+decodeLength+1])
		pos += decodeLength + 1
	}

	if fieldSet&2 > 0 {
		decodeLength := int(enc[pos])

		if len(enc) < pos+decodeLength+1 {
			return fmt.Errorf(
				"malformed CBOR for Account.Nonce: %s, Length %d",
				enc[pos+1:], decodeLength)
		}

		a.Balance.SetBytes(enc[pos+1 : pos+decodeLength+1])
		pos += decodeLength + 1
	}

	if fieldSet&4 > 0 {
		decodeLength := int(enc[pos])

		if len(enc) < pos+decodeLength+1 {
			return fmt.Errorf(
				"malformed CBOR for Account.Balance: %s, Length %d",
				enc[pos+1:], decodeLength)
		}

		a.Incarnation = bytesToUint64(enc[pos+1 : pos+decodeLength+1])
		pos += decodeLength + 1
	}

	if fieldSet&8 > 0 {
		decodeLength := int(enc[pos])

		if decodeLength != 32 {
			return fmt.Errorf("root should be 32 bytes long, got %d instead",
				decodeLength)
		}

		if len(enc) < pos+decodeLength+1 {
			return fmt.Errorf(
				"malformed CBOR for Account.Root: %s, Length %d",
				enc[pos+1:], decodeLength)
		}

		a.Root.SetBytes(enc[pos+1 : pos+decodeLength+1])
		pos += decodeLength + 1
	}

	if fieldSet&16 > 0 {
		decodeLength := int(enc[pos])

		if decodeLength != 32 {
			return fmt.Errorf("codehash should be 32 bytes long, got %d instead",
				decodeLength)
		}

		if len(enc) < pos+decodeLength+1 {
			return fmt.Errorf(
				"malformed CBOR for Account.CodeHash: %s, Length %d",
				enc[pos+1:], decodeLength)
		}

		a.CodeHash.SetBytes(enc[pos+1 : pos+decodeLength+1])
		pos += decodeLength + 1
	}

	if fieldSet&32 > 0 {
		decodeLength := int(enc[pos])

		if len(enc) < pos+decodeLength+1 {
			return fmt.Errorf(
				"malformed CBOR for Account.StorageSize: %s, Length %d",
				enc[pos+1:], decodeLength)
		}

		a.StorageSize = bytesToUint64(enc[pos+1 : pos+decodeLength+1])
		a.HasStorageSize = true
		// pos += decodeLength + 1
	}

	return nil
}

func (a *Account) SelfCopy() *Account {
	newAcc := NewAccount()
	newAcc.Copy(a)
	return &newAcc
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
