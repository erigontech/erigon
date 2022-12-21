package accounts

import (
	"fmt"
	"io"
	"math/bits"
	"sync"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/rlp"
)

// Account is the Ethereum consensus representation of accounts.
// These objects are stored in the main account trie.
// DESCRIBED: docs/programmers_guide/guide.md#ethereum-state
type Account struct {
	Initialised bool
	Nonce       uint64
	Balance     uint256.Int
	Root        common.Hash // merkle root of the storage trie
	CodeHash    common.Hash // hash of the bytecode
	Incarnation uint64
}

const (
	MimetypeDataWithValidator = "data/validator"
	MimetypeTypedData         = "data/typed"
	MimetypeClique            = "application/x-clique-header"
	MimetypeParlia            = "application/x-parlia-header"
	MimetypeBor               = "application/x-bor-header"
	MimetypeTextPlain         = "text/plain"
)

var emptyCodeHash = crypto.Keccak256Hash(nil)
var emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

// NewAccount creates a new account w/o code nor storage.
func NewAccount() Account {
	return Account{
		Root:     emptyRoot,
		CodeHash: emptyCodeHash,
	}
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

	if !a.Balance.IsZero() {
		structLength += uint(a.Balance.ByteLen()) + 1
	}

	if a.Nonce > 0 {
		structLength += uint((bits.Len64(a.Nonce)+7)/8) + 1
	}

	if !a.IsEmptyCodeHash() {
		structLength += 33 // 32-byte array + 1 bytes for length
	}

	if a.Incarnation > 0 {
		structLength += uint((bits.Len64(a.Incarnation)+7)/8) + 1
	}

	return structLength
}

func (a *Account) EncodingLengthForHashing() uint {
	var structLength uint

	balanceBytes := 0
	if !a.Balance.LtUint64(128) {
		balanceBytes = a.Balance.ByteLen()
	}

	nonceBytes := rlp.IntLenExcludingHead(a.Nonce)

	structLength += uint(balanceBytes + nonceBytes + 2)

	structLength += 66 // Two 32-byte arrays + 2 prefixes

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
	if !a.Balance.IsZero() {
		fieldSet |= 2
		balanceBytes := a.Balance.ByteLen()
		buffer[pos] = byte(balanceBytes)
		pos++
		a.Balance.WriteToSlice(buffer[pos : pos+balanceBytes])
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

	// Encoding CodeHash
	if !a.IsEmptyCodeHash() {
		fieldSet |= 8
		buffer[pos] = 32
		copy(buffer[pos+1:], a.CodeHash.Bytes())
		//pos += 33
	}

	buffer[0] = byte(fieldSet)
}

// Decodes length and determines whether it corresponds to a structure of a byte array
func decodeLengthForHashing(buffer []byte, pos int) (length int, structure bool, newPos int) {
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

var rlpEncodingBufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 128)
		return &buf
	},
}

func (a *Account) EncodeRLP(w io.Writer) error {
	var buf []byte
	l := a.EncodingLengthForHashing()
	if l > 128 {
		buf = make([]byte, l)
	} else {
		bp := rlpEncodingBufPool.Get().(*[]byte)
		defer rlpEncodingBufPool.Put(bp)
		buf = *bp
		buf = buf[:l]
	}

	a.EncodeForHashing(buf)
	_, err := w.Write(buf)
	return err
}

func (a *Account) EncodeForHashing(buffer []byte) {
	balanceBytes := 0
	if !a.Balance.LtUint64(128) {
		balanceBytes = a.Balance.ByteLen()
	}

	nonceBytes := rlp.IntLenExcludingHead(a.Nonce)

	var structLength = uint(balanceBytes + nonceBytes + 2)
	structLength += 66 // Two 32-byte arrays + 2 prefixes

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
	if a.Balance.LtUint64(128) && !a.Balance.IsZero() {
		buffer[pos] = byte(a.Balance.Uint64())
		pos++
	} else {
		buffer[pos] = byte(128 + balanceBytes)
		pos++
		a.Balance.WriteToSlice(buffer[pos : pos+balanceBytes])
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
	//pos += 32
}

// Copy makes `a` a full, independent (meaning that if the `image` changes in any way, it does not affect `a`) copy of the account `image`.
func (a *Account) Copy(image *Account) {
	a.Initialised = image.Initialised
	a.Nonce = image.Nonce
	a.Balance.Set(&image.Balance)
	copy(a.Root[:], image.Root[:])
	copy(a.CodeHash[:], image.CodeHash[:])
	a.Incarnation = image.Incarnation
}

func (a *Account) DecodeForHashing(enc []byte) error {
	length, structure, pos := decodeLengthForHashing(enc, 0)
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
	a.Balance.Clear()
	a.Root = emptyRoot
	a.CodeHash = emptyCodeHash
	if length == 0 && structure {
		return nil
	}

	if pos < len(enc) {
		nonceBytes, s, newPos := decodeLengthForHashing(enc, pos)
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
		balanceBytes, s, newPos := decodeLengthForHashing(enc, pos)
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

		if balanceBytes == 0 && newPos == pos {
			a.Balance.SetUint64(uint64(enc[newPos]))
			pos = newPos + 1
		} else {
			a.Balance.SetBytes(enc[newPos : newPos+balanceBytes])
			pos = newPos + balanceBytes
		}
	}
	if pos < len(enc) {
		rootBytes, s, newPos := decodeLengthForHashing(enc, pos)
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
		codeHashBytes, s, newPos := decodeLengthForHashing(enc, pos)
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
		storageSizeBytes, s, newPos := decodeLengthForHashing(enc, pos)
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

		// Commented out because of the ineffectual assignment - uncomment if adding more fields
		var storageSize uint64
		if storageSizeBytes == 0 && newPos == pos {
			storageSize = uint64(enc[newPos])
			pos = newPos + 1
		} else {
			for _, b := range enc[newPos : newPos+storageSizeBytes] {
				storageSize = (storageSize << 8) + uint64(b)
			}
			pos = newPos + storageSizeBytes
		}
		_ = storageSize
	}
	_ = pos

	return nil
}

func (a *Account) Reset() {
	a.Initialised = true
	a.Nonce = 0
	a.Incarnation = 0
	a.Balance.Clear()
	copy(a.Root[:], emptyRoot[:])
	copy(a.CodeHash[:], emptyCodeHash[:])
}

func (a *Account) DecodeForStorage(enc []byte) error {
	a.Reset()

	if len(enc) == 0 {
		return nil
	}

	var fieldSet = enc[0]
	var pos = 1

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
				"malformed CBOR for Account.Incarnation: %s, Length %d",
				enc[pos+1:], decodeLength)
		}

		a.Incarnation = bytesToUint64(enc[pos+1 : pos+decodeLength+1])
		pos += decodeLength + 1
	}

	if fieldSet&8 > 0 {

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

	_ = pos

	return nil
}

func DecodeIncarnationFromStorage(enc []byte) (uint64, error) {
	if len(enc) == 0 {
		return 0, nil
	}

	var fieldSet = enc[0]
	var pos = 1

	//looks for the position incarnation is at
	if fieldSet&1 > 0 {
		decodeLength := int(enc[pos])
		if len(enc) < pos+decodeLength+1 {
			return 0, fmt.Errorf(
				"malformed CBOR for Account.Nonce: %s, Length %d",
				enc[pos+1:], decodeLength)
		}
		pos += decodeLength + 1
	}

	if fieldSet&2 > 0 {
		decodeLength := int(enc[pos])
		if len(enc) < pos+decodeLength+1 {
			return 0, fmt.Errorf(
				"malformed CBOR for Account.Nonce: %s, Length %d",
				enc[pos+1:], decodeLength)
		}
		pos += decodeLength + 1
	}

	if fieldSet&4 > 0 {
		decodeLength := int(enc[pos])

		//checks if the ending position is correct if not returns 0
		if len(enc) < pos+decodeLength+1 {
			return 0, fmt.Errorf(
				"malformed CBOR for Account.Incarnation: %s, Length %d",
				enc[pos+1:], decodeLength)
		}

		incarnation := bytesToUint64(enc[pos+1 : pos+decodeLength+1])
		return incarnation, nil
	}

	return 0, nil

}

func (a *Account) SelfCopy() *Account {
	newAcc := NewAccount()
	newAcc.Copy(a)
	return &newAcc
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
	return IsEmptyCodeHash(a.CodeHash)
}

func IsEmptyCodeHash(codeHash common.Hash) bool {
	return codeHash == emptyCodeHash || codeHash == (common.Hash{})
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
		a.Balance.Cmp(&acc.Balance) == 0 &&
		a.Incarnation == acc.Incarnation
}

func ConvertV3toV2(v []byte) ([]byte, error) {
	var a Account
	if err := DeserialiseV3(&a, v); err != nil {
		return nil, fmt.Errorf("ConvertV3toV2(%x): %w", v, err)
	}
	v = make([]byte, a.EncodingLengthForStorage())
	a.EncodeForStorage(v)
	return v, nil
}

// DeserialiseV3 - method to deserialize accounts in Erigon22 history
func DeserialiseV3(a *Account, enc []byte) error {
	a.Reset()
	pos := 0
	nonceBytes := int(enc[pos])
	pos++
	if nonceBytes > 0 {
		a.Nonce = bytesToUint64(enc[pos : pos+nonceBytes])
		pos += nonceBytes
	}
	balanceBytes := int(enc[pos])
	pos++
	if balanceBytes > 0 {
		a.Balance.SetBytes(enc[pos : pos+balanceBytes])
		pos += balanceBytes
	}
	codeHashBytes := int(enc[pos])
	pos++
	if codeHashBytes > 0 {
		copy(a.CodeHash[:], enc[pos:pos+codeHashBytes])
		pos += codeHashBytes
	}
	if pos >= len(enc) {
		return fmt.Errorf("deserialse2: %d >= %d ", pos, len(enc))
	}
	incBytes := int(enc[pos])
	pos++
	if incBytes > 0 {
		a.Incarnation = bytesToUint64(enc[pos : pos+incBytes])
	}
	return nil
}

func SerialiseV3(a *Account) []byte {
	var l int
	l++
	if a.Nonce > 0 {
		l += (bits.Len64(a.Nonce) + 7) / 8
	}
	l++
	if !a.Balance.IsZero() {
		l += a.Balance.ByteLen()
	}
	l++
	if !a.IsEmptyCodeHash() {
		l += 32
	}
	l++
	if a.Incarnation > 0 {
		l += (bits.Len64(a.Incarnation) + 7) / 8
	}
	value := make([]byte, l)
	pos := 0
	if a.Nonce == 0 {
		value[pos] = 0
		pos++
	} else {
		nonceBytes := (bits.Len64(a.Nonce) + 7) / 8
		value[pos] = byte(nonceBytes)
		var nonce = a.Nonce
		for i := nonceBytes; i > 0; i-- {
			value[pos+i] = byte(nonce)
			nonce >>= 8
		}
		pos += nonceBytes + 1
	}
	if a.Balance.IsZero() {
		value[pos] = 0
		pos++
	} else {
		balanceBytes := a.Balance.ByteLen()
		value[pos] = byte(balanceBytes)
		pos++
		a.Balance.WriteToSlice(value[pos : pos+balanceBytes])
		pos += balanceBytes
	}
	if a.IsEmptyCodeHash() {
		value[pos] = 0
		pos++
	} else {
		value[pos] = 32
		pos++
		copy(value[pos:pos+32], a.CodeHash[:])
		pos += 32
	}
	if a.Incarnation == 0 {
		value[pos] = 0
	} else {
		incBytes := (bits.Len64(a.Incarnation) + 7) / 8
		value[pos] = byte(incBytes)
		var inc = a.Incarnation
		for i := incBytes; i > 0; i-- {
			value[pos+i] = byte(inc)
			inc >>= 8
		}
	}
	return value
}
