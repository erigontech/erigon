package hex

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

const (
	// Base represents the hexadecimal base, which is 16
	Base = 16

	// BitSize64 64 bits
	BitSize64 = 64
)

// DecError represents an error when decoding a hex value
type DecError struct{ msg string }

func (err DecError) Error() string { return err.msg }

// EncodeToHex generates a hex string based on the byte representation, with the '0x' prefix
func EncodeToHex(str []byte) string {
	return "0x" + hex.EncodeToString(str)
}

// EncodeToString is a wrapper method for hex.EncodeToString
func EncodeToString(str []byte) string {
	return hex.EncodeToString(str)
}

// DecodeString returns the byte representation of the hexadecimal string
func DecodeString(str string) ([]byte, error) {
	return hex.DecodeString(str)
}

// DecodeHex converts a hex string to a byte array
func DecodeHex(str string) ([]byte, error) {
	str = strings.TrimPrefix(str, "0x")

	return hex.DecodeString(str)
}

// MustDecodeHex type-checks and converts a hex string to a byte array
func MustDecodeHex(str string) []byte {
	buf, err := DecodeHex(str)
	if err != nil {
		panic(fmt.Errorf("could not decode hex: %v", err))
	}

	return buf
}

// DecodeUint64 type-checks and converts a hex string to a uint64
func DecodeUint64(str string) uint64 {
	i := DecodeBig(str)
	return i.Uint64()
}

// EncodeUint64 encodes a number as a hex string with 0x prefix.
func EncodeUint64(i uint64) string {
	enc := make([]byte, 2, 10) //nolint:gomnd
	copy(enc, "0x")
	return string(strconv.AppendUint(enc, i, Base))
}

// BadNibble is a nibble that is bad
const BadNibble = ^uint64(0)

// DecodeNibble decodes a byte into a uint64
func DecodeNibble(in byte) uint64 {
	switch {
	case in >= '0' && in <= '9':
		return uint64(in - '0')
	case in >= 'A' && in <= 'F':
		return uint64(in - 'A' + 10) //nolint:gomnd
	case in >= 'a' && in <= 'f':
		return uint64(in - 'a' + 10) //nolint:gomnd
	default:
		return BadNibble
	}
}

// EncodeBig encodes bigint as a hex string with 0x prefix.
// The sign of the integer is ignored.
func EncodeBig(bigint *big.Int) string {
	numBits := bigint.BitLen()
	if numBits == 0 {
		return "0x0"
	}

	return fmt.Sprintf("%#x", bigint)
}

// DecodeBig converts a hex number to a big.Int value
func DecodeBig(hexNum string) *big.Int {
	str := strings.TrimPrefix(hexNum, "0x")
	createdNum := new(big.Int)
	createdNum.SetString(str, Base)

	return createdNum
}

// IsValid checks if the provided string is a valid hexadecimal value
func IsValid(s string) bool {
	str := strings.TrimPrefix(s, "0x")
	for _, b := range []byte(str) {
		if !(b >= '0' && b <= '9' || b >= 'a' && b <= 'f' || b >= 'A' && b <= 'F') {
			return false
		}
	}
	return true
}
