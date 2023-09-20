package common

import (
	"bytes"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"reflect"

	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
)

var (
	bytes64T = reflect.TypeOf(Bytes64{})
)

type Bytes64 [length.Bytes64]byte

// Hex converts a hash to a hex string.
func (b Bytes64) Hex() string { return hexutility.Encode(b[:]) }

// UnmarshalJSON parses a hash in hex syntax.
func (b *Bytes64) UnmarshalJSON(input []byte) error {
	return hexutility.UnmarshalFixedJSON(bytes64T, input, b[:])
}

// UnmarshalText parses a hash in hex syntax.
func (b *Bytes64) UnmarshalText(input []byte) error {
	return hexutility.UnmarshalFixedText("Bytes64", input, b[:])
}

// MarshalText returns the hex representation of a.
func (b Bytes64) MarshalText() ([]byte, error) {
	bl := b[:]
	result := make([]byte, len(b)*2+2)
	copy(result, hexPrefix)
	hex.Encode(result[2:], bl)
	return result, nil
}

// Format implements fmt.Formatter.
// Hash supports the %v, %s, %v, %x, %X and %d format verbs.
func (b Bytes64) Format(s fmt.State, c rune) {
	hexb := make([]byte, 2+len(b)*2)
	copy(hexb, "0x")
	hex.Encode(hexb[2:], b[:])

	switch c {
	case 'x', 'X':
		if !s.Flag('#') {
			hexb = hexb[2:]
		}
		if c == 'X' {
			hexb = bytes.ToUpper(hexb)
		}
		fallthrough
	case 'v', 's':
		s.Write(hexb)
	case 'q':
		q := []byte{'"'}
		s.Write(q)
		s.Write(hexb)
		s.Write(q)
	case 'd':
		fmt.Fprint(s, ([len(b)]byte)(b))
	default:
		fmt.Fprintf(s, "%%!%c(hash=%x)", c, b)
	}
}

// String implements the stringer interface and is used also by the logger when
// doing full logging into a file.
func (b Bytes64) String() string {
	return b.Hex()
}

// SetBytes sets the hash to the value of i.
// If b is larger than len(h), b will be cropped from the left.
func (b *Bytes64) SetBytes(i []byte) {
	if len(i) > len(b) {
		i = i[len(i)-length.Hash:]
	}

	copy(b[length.Hash-len(i):], i)
}

// Value implements valuer for database/sql.
func (b Bytes64) Value() (driver.Value, error) {
	return b[:], nil
}

// TerminalString implements log.TerminalStringer, formatting a string for console
// output during logging.
func (b Bytes64) TerminalString() string {
	return fmt.Sprintf("%x…%x", b[:3], b[len(b)-3:])
}
