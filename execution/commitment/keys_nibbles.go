package commitment

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	ecrypto "github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/length"
)

// KeyToHexNibbleHash hashes plain key with respect to plain key size (part < 20 bytes for account, part >= 20 bytes for storage)
// and returns the hashed key in nibblized form suitable for hex trie (each byte represented by 2 nibbles).
func KeyToHexNibbleHash(key []byte) []byte {
	// `nibblized`, `hashed` - are the same array
	// but `hashed` is 2nd half of `nibblized`
	// will use 1st half of `nibblized` in the end
	var nibblized, hashed []byte
	if len(key) > length.Addr { // storage
		nibblized = make([]byte, 128)
		hashed = nibblized[64:]
		copy(hashed[:32], ecrypto.Keccak256(key[:length.Addr]))
		copy(hashed[32:], ecrypto.Keccak256(key[length.Addr:]))
	} else {
		nibblized = make([]byte, 64)
		hashed = nibblized[32:]
		copy(hashed, ecrypto.Keccak256(key))
	}

	for i, b := range hashed {
		nibblized[i*2] = (b >> 4) & 0xf
		nibblized[i*2+1] = b & 0xf
	}
	return nibblized
}

func KeyToNibblizedHash(key []byte) []byte {
	nibblized := make([]byte, 64) // nibblized hash
	hashed := nibblized[32:]
	copy(hashed, ecrypto.Keccak256(key))
	for i, b := range hashed {
		nibblized[i*2] = (b >> 4) & 0xf
		nibblized[i*2+1] = b & 0xf
	}
	return nibblized
}

// HexNibblesToCompactBytes Converts slice of hex nibbles into regular bytes form, combining two nibbles into one byte.
func HexNibblesToCompactBytes(key []byte) []byte {
	var compactZeroByte byte
	keyLen := len(key)
	if HasTerm(key) { // trim terminator if needed
		keyLen--
		compactZeroByte = 0x20
	}
	var firstNibble byte
	if len(key) > 0 {
		firstNibble = key[0]
	}

	// decode first byte
	var keyIndex int
	if keyLen&1 == 1 { // check if key length is odd
		compactZeroByte |= 0x10 | firstNibble // Odd: (1<<4) + first nibble
		keyIndex++
	}

	bufLen := keyLen/2 + 1 // always > 0
	buf := make([]byte, bufLen)
	buf[0] = compactZeroByte

	// decode the rest of the key
	for bufIndex := 1; keyIndex < keyLen; keyIndex, bufIndex = keyIndex+2, bufIndex+1 {
		if keyIndex == keyLen-1 {
			buf[bufIndex] = buf[bufIndex] & 0xf
		} else {
			buf[bufIndex] = key[keyIndex+1]
		}
		buf[bufIndex] |= key[keyIndex] << 4
	}
	return buf
}

// uncompactNibbles converts a slice of bytes representing nibbles in regular form into 1-nibble-per-byte form.
func uncompactNibbles(key []byte) []byte {
	if len(key) == 0 {
		return nil
	}
	terminating := key[0]&0x20 == 0x20
	odd := key[0]&0x10 == 0x10
	buf := make([]byte, 0, len(key)*2)
	if odd {
		buf = append(buf, key[0]&0x0f)
	}
	key = key[1:]
	for _, b := range key {
		buf = append(buf, b>>4, b&0x0f)
	}
	if terminating {
		buf = append(buf, terminatorHexByte)
	}
	return buf
}

// HasTerm returns whether a hex nibble key has the terminator flag.
func HasTerm(s []byte) bool {
	return len(s) > 0 && s[len(s)-1] == terminatorHexByte
}

// returns the length of the common prefix of two byte slices
func commonPrefixLen(b1, b2 []byte) int {
	var i int
	for i = 0; i < len(b1) && i < len(b2); i++ {
		if b1[i] != b2[i] {
			break
		}
	}
	return i
}

// CompactKey takes a slice of nibbles and compacts them into the original byte slice.
// It returns an error if the input contains invalid nibbles (values > 0xF).
func CompactKey(nibbles []byte) ([]byte, error) {
	// If the number of nibbles is odd, you might decide to handle it differently.
	// For this example, we'll return an error.
	if len(nibbles)%2 != 0 {
		return nil, errors.New("nibbles slice has an odd length")
	}

	key := make([]byte, len(nibbles)/2)
	for i := 0; i < len(key); i++ {
		highNibble := nibbles[i*2]
		lowNibble := nibbles[i*2+1]

		// Validate that each nibble is indeed a nibble
		if highNibble > 0xF || lowNibble > 0xF {
			return nil, fmt.Errorf("invalid nibble at position %d or %d: 0x%X, 0x%X", i*2, i*2+1, highNibble, lowNibble)
		}

		key[i] = (highNibble << 4) | (lowNibble & 0x0F)
	}
	return key, nil
}

// updatedNibs returns a string of nibbles that are set in the given number.
func updatedNibs(num uint16) string {
	var nibbles []string
	for i := 0; i < 16; i++ {
		if num&(1<<i) != 0 {
			nibbles = append(nibbles, fmt.Sprintf("%X", i))
		}
	}
	return strings.Join(nibbles, ",")
}

func PrefixStringToNibbles(hexStr string) ([]byte, error) {
	nibbles := make([]byte, len(hexStr))

	for i, char := range hexStr {
		nibble, err := strconv.ParseUint(string(char), 16, 8)
		if err != nil {
			return nil, err
		}
		nibbles[i] = byte(nibble)
	}

	return nibbles, nil
}
