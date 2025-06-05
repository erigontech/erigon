package commitment

import (
	"errors"
	"fmt"
	"strings"

	"github.com/erigontech/erigon-lib/common/length"
	ecrypto "github.com/erigontech/erigon-lib/crypto"
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

// hexNibblesToCompactBytes Converts slice of hex nibbles into regular bytes form, combining two nibbles into one byte.
func hexNibblesToCompactBytes(key []byte) []byte {
	var compactZeroByte byte
	keyLen := len(key)
	if hasTerm(key) { // trim terminator if needed
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

// hasTerm returns whether a hex nibble key has the terminator flag.
func hasTerm(s []byte) bool {
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

// splits each byte in key slice onto 2 nibbles in the resulting slice
func splitOntoHexNibbles(key, nibblized []byte) []byte { // nolint:unused
	return nibblized
}

// compactKey takes a slice of nibbles and compacts them into the original byte slice.
// It returns an error if the input contains invalid nibbles (values > 0xF).
func compactKey(nibbles []byte) ([]byte, error) {
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

// hashes plainKey using keccakState and writes the hashed key nibbles to dest with respect to hashedKeyOffset.
// Note that this function does not respect plainKey length so hashing it at once without splitting to account/storage part.
func hashKey(keccak keccakState, plainKey []byte, dest []byte, hashedKeyOffset int, hashBuf []byte) error {
	_, _ = hashBuf[length.Hash-1], dest[length.Hash*2-1] // bounds checks elimination
	keccak.Reset()
	if _, err := keccak.Write(plainKey); err != nil {
		return err
	}
	if _, err := keccak.Read(hashBuf); err != nil {
		return err
	}
	hashBuf = hashBuf[hashedKeyOffset/2:]
	var k int
	if hashedKeyOffset%2 == 1 { // write zero byte as compacted since hashedKeyOffset is odd
		dest[0] = hashBuf[0] & 0xf
		k++
		hashBuf = hashBuf[1:]
	}
	// write each byte as 2 hex nibbles
	for _, c := range hashBuf {
		dest[k] = (c >> 4) & 0xf
		k++
		dest[k] = c & 0xf
		k++
	}
	return nil
}
