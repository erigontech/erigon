// Copyright 2021 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package common

import (
	"bytes"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"math/big"
	"unique"

	"golang.org/x/crypto/sha3"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
)

// address represents the 20 byte address of an Ethereum account.
type address [length.Addr]byte

type Address unique.Handle[address]

var ZeroAddress Address = Address(unique.Make(address{}))

func NewAddress(b ...byte) Address {
	var a address
	switch {
	case len(b) == 0:
		return ZeroAddress
	case len(b) < length.Addr:
		copy(a[:], b)
	default:
		copy(a[:], b[0:length.Addr])
	}

	return Address(unique.Make(a))
}

func AsAddress(b [length.Addr]byte) Address {
	return Address(unique.Make(address(b)))
}

// BytesToAddress returns Address with value b.
// If b is larger than len(h), b will be cropped from the left.
func BytesToAddress(b []byte) Address {
	return NewAddress(b...)
}

// BigToAddress returns Address with byte values of b.
// If b is larger than len(h), b will be cropped from the left.
func BigToAddress(b *big.Int) Address { return NewAddress(b.Bytes()...) }

// HexToAddress returns Address with byte values of s.
// If s is larger than len(h), s will be cropped from the left.
func HexToAddress(s string) Address { return NewAddress(hexutil.FromHex(s)...) }

// IsHexAddress verifies whether a string can represent a valid hex-encoded
// Ethereum address or not.
func IsHexAddress(s string) bool {
	if hexutil.Has0xPrefix(s) {
		s = s[2:]
	}
	return len(s) == 2*length.Addr && hexutil.IsHex(s)
}

// AsArray gets the byte array representation of the underlying address.
func (a Address) AsArray() [length.Addr]byte { return unique.Handle[address](a).Value() }

// AsBytes gets a slice backed by the byte array representation of the underlying address.
func (a Address) AsSlice() []byte {
	b := unique.Handle[address](a).Value()
	return b[:]
}

// Hash converts an address to a hash by left-padding it with zeros.
func (a Address) Hash() Hash {
	b := a.AsArray()
	return BytesToHash(b[:])
}

// Hex returns an EIP55-compliant hex string representation of the address.
func (a Address) Hex() string {
	return string(a.checksumHex())
}

// String implements fmt.Stringer.
func (a Address) String() string {
	return a.Hex()
}

func (a *Address) checksumHex() []byte {
	buf := a.hex()

	// compute checksum
	sha := sha3.NewLegacyKeccak256()
	//nolint:errcheck
	sha.Write(buf[2:])
	hash := sha.Sum(nil)

	for i := 2; i < len(buf); i++ {
		hashByte := hash[(i-2)/2]
		if i%2 == 0 {
			hashByte = hashByte >> 4
		} else {
			hashByte &= 0xf
		}
		if buf[i] > '9' && hashByte > 7 {
			buf[i] -= 32
		}
	}
	return buf
}

func (a Address) hex() []byte {
	var buf [length.Addr*2 + 2]byte
	copy(buf[:2], "0x")
	b := a.AsArray()
	hex.Encode(buf[2:], b[:])
	return buf[:]
}

// Format implements fmt.Formatter.
// Address supports the %v, %s, %v, %x, %X and %d format verbs.
func (a Address) Format(s fmt.State, c rune) {
	switch c {
	case 'v', 's':
		s.Write(a.checksumHex())
	case 'q':
		q := []byte{'"'}
		s.Write(q)
		s.Write(a.checksumHex())
		s.Write(q)
	case 'x', 'X':
		// %x disables the checksum.
		hex := a.hex()
		if !s.Flag('#') {
			hex = hex[2:]
		}
		if c == 'X' {
			hex = bytes.ToUpper(hex)
		}
		s.Write(hex)
	case 'd':
		fmt.Fprint(s, a.AsArray())
	default:
		fmt.Fprintf(s, "%%!%c(address=%x)", c, a)
	}
}

// SetBytes sets the address to the value of b.
// If b is larger than len(a), b will be cropped from the left.
func (a *Address) SetBytes(b []byte) {
	*a = NewAddress(b...)
}

// MarshalText returns the hex representation of a.
func (a Address) MarshalText() ([]byte, error) {
	b := a.AsArray()
	result := make([]byte, len(b)*2+2)
	copy(result, hexPrefix)
	hex.Encode(result[2:], b[:])
	return result, nil
}

// UnmarshalText parses a hash in hex syntax.
func (a *Address) UnmarshalText(input []byte) error {
	var i address
	if err := hexutil.UnmarshalFixedText("Address", input, i[:]); err != nil {
		return err
	}
	*a = Address(unique.Make(i))
	return nil
}

// UnmarshalJSON parses a hash in hex syntax.
func (a *Address) UnmarshalJSON(input []byte) error {
	var i address
	if err := hexutil.UnmarshalFixedJSON(addressT, input, i[:]); err != nil {
		return err
	}
	*a = Address(unique.Make(i))
	return nil
}

// Scan implements Scanner for database/sql.
func (a *Address) Scan(src interface{}) error {
	srcB, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("can't scan %T into Address", src)
	}
	if len(srcB) != length.Addr {
		return fmt.Errorf("can't scan []byte of len %d into Address, want %d", len(srcB), length.Addr)
	}
	*a = NewAddress(srcB...)
	return nil
}

// Value implements valuer for database/sql.
func (a Address) Value() (driver.Value, error) {
	b := a.AsArray()
	return b[:], nil
}

// Cmp compares two addresses.
func (a Address) Cmp(other Address) int {
	ab := a.AsArray()
	ob := other.AsArray()
	return bytes.Compare(ab[:], ob[:])
}
