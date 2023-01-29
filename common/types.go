// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package common

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/crypto/cryptopool"

	"github.com/ledgerwatch/erigon/common/hexutil"
)

// Lengths of hashes and addresses in bytes.
const (
	// BlockNumberLength length of uint64 big endian
	BlockNumberLength = 8
	// IncarnationLength length of uint64 for contract incarnations
	IncarnationLength = 8
	// Address32Length is the expected length of the Starknet address (in bytes)
	Address32Length = 32
)

var (
	addressSt = reflect.TypeOf(Address32{})
)

// UnprefixedHash allows marshaling a Hash without 0x prefix.
type UnprefixedHash libcommon.Hash

// UnmarshalText decodes the hash from hex. The 0x prefix is optional.
func (h *UnprefixedHash) UnmarshalText(input []byte) error {
	return hexutil.UnmarshalFixedUnprefixedText("UnprefixedHash", input, h[:])
}

// MarshalText encodes the hash as hex.
func (h UnprefixedHash) MarshalText() ([]byte, error) {
	return []byte(hex.EncodeToString(h[:])), nil
}

/////////// Address

var addressT = reflect.TypeOf(libcommon.Address{})

// UnprefixedAddress allows marshaling an Address without 0x prefix.
type UnprefixedAddress libcommon.Address

// UnmarshalText decodes the address from hex. The 0x prefix is optional.
func (a *UnprefixedAddress) UnmarshalText(input []byte) error {
	return hexutil.UnmarshalFixedUnprefixedText("UnprefixedAddress", input, a[:])
}

// MarshalText encodes the address as hex.
func (a UnprefixedAddress) MarshalText() ([]byte, error) {
	return []byte(hex.EncodeToString(a[:])), nil
}

// MixedcaseAddress retains the original string, which may or may not be
// correctly checksummed
type MixedcaseAddress struct {
	addr     libcommon.Address
	original string
}

// NewMixedcaseAddress constructor (mainly for testing)
func NewMixedcaseAddress(addr libcommon.Address) MixedcaseAddress {
	return MixedcaseAddress{addr: addr, original: addr.Hex()}
}

// NewMixedcaseAddressFromString is mainly meant for unit-testing
func NewMixedcaseAddressFromString(hexaddr string) (*MixedcaseAddress, error) {
	if !libcommon.IsHexAddress(hexaddr) {
		return nil, errors.New("invalid address")
	}
	a := FromHex(hexaddr)
	return &MixedcaseAddress{addr: libcommon.BytesToAddress(a), original: hexaddr}, nil
}

// UnmarshalJSON parses MixedcaseAddress
func (ma *MixedcaseAddress) UnmarshalJSON(input []byte) error {
	if err := hexutility.UnmarshalFixedJSON(addressT, input, ma.addr[:]); err != nil {
		return err
	}
	return json.Unmarshal(input, &ma.original)
}

// MarshalJSON marshals the original value
func (ma *MixedcaseAddress) MarshalJSON() ([]byte, error) {
	if strings.HasPrefix(ma.original, "0x") || strings.HasPrefix(ma.original, "0X") {
		return json.Marshal(fmt.Sprintf("0x%s", ma.original[2:]))
	}
	return json.Marshal(fmt.Sprintf("0x%s", ma.original))
}

// Address returns the address
func (ma *MixedcaseAddress) Address() libcommon.Address {
	return ma.addr
}

// String implements fmt.Stringer
func (ma *MixedcaseAddress) String() string {
	if ma.ValidChecksum() {
		return fmt.Sprintf("%s [chksum ok]", ma.original)
	}
	return fmt.Sprintf("%s [chksum INVALID]", ma.original)
}

// ValidChecksum returns true if the address has valid checksum
func (ma *MixedcaseAddress) ValidChecksum() bool {
	return ma.original == ma.addr.Hex()
}

// Original returns the mixed-case input string
func (ma *MixedcaseAddress) Original() string {
	return ma.original
}

// Addresses is a slice of libcommon.Address, implementing sort.Interface
type Addresses []libcommon.Address

func (addrs Addresses) Len() int {
	return len(addrs)
}
func (addrs Addresses) Less(i, j int) bool {
	return bytes.Compare(addrs[i][:], addrs[j][:]) == -1
}
func (addrs Addresses) Swap(i, j int) {
	addrs[i], addrs[j] = addrs[j], addrs[i]
}

// Hashes is a slice of libcommon.Hash, implementing sort.Interface
type Hashes []libcommon.Hash

func (hashes Hashes) Len() int {
	return len(hashes)
}
func (hashes Hashes) Less(i, j int) bool {
	return bytes.Compare(hashes[i][:], hashes[j][:]) == -1
}
func (hashes Hashes) Swap(i, j int) {
	hashes[i], hashes[j] = hashes[j], hashes[i]
}

const StorageKeyLen = 2*length.Hash + IncarnationLength

// StorageKey is representation of address of a contract storage item
// It consists of two parts, each of which are 32-byte hashes:
// 1. Hash of the contract's address
// 2. Hash of the item's key
type StorageKey [StorageKeyLen]byte

// StorageKeys is a slice of StorageKey, implementing sort.Interface
type StorageKeys []StorageKey

func (keys StorageKeys) Len() int {
	return len(keys)
}
func (keys StorageKeys) Less(i, j int) bool {
	return bytes.Compare(keys[i][:], keys[j][:]) == -1
}
func (keys StorageKeys) Swap(i, j int) {
	keys[i], keys[j] = keys[j], keys[i]
}

/////////// Address32

// Address32 represents the 32 byte address.
type Address32 [Address32Length]byte

// BytesToAddress32 returns Address32 with value b.
// If b is larger than len(h), b will be cropped from the left.
func BytesToAddress32(b []byte) Address32 {
	var a Address32
	a.SetBytes(b)
	return a
}

// HexToAddress32 returns Address32 with byte values of s.
// If s is larger than len(h), s will be cropped from the left.
func HexToAddress32(s string) Address32 { return BytesToAddress32(FromHex(s)) }

// IsHexAddress32 verifies whether a string can represent a valid hex-encoded
// Starknet address or not.
func IsHexAddress32(s string) bool {
	if has0xPrefix(s) {
		s = s[2:]
	}
	return len(s) == 2*Address32Length && isHex(s)
}

// Bytes gets the string representation of the underlying address.
func (a Address32) Bytes() []byte { return a[:] }

// Hash converts an address to a hash by left-padding it with zeros.
func (a Address32) Hash() libcommon.Hash { return libcommon.BytesToHash(a[:]) }

// Hex returns an EIP55-compliant hex string representation of the address.
func (a Address32) Hex() string {
	return string(a.checksumHex())
}

// String implements fmt.Stringer.
func (a Address32) String() string {
	return a.Hex()
}

func (a *Address32) checksumHex() []byte {
	buf := a.hex()

	// compute checksum
	sha := cryptopool.GetLegacyKeccak256()
	//nolint:errcheck
	sha.Write(buf[2:])
	hash := sha.Sum(nil)
	cryptopool.ReturnLegacyKeccak256(sha)
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

func (a Address32) hex() []byte {
	var buf [len(a)*2 + 2]byte
	copy(buf[:2], "0x")
	hex.Encode(buf[2:], a[:])
	return buf[:]
}

// SetBytes sets the address to the value of b.
// If b is larger than len(a), b will be cropped from the left.
func (a *Address32) SetBytes(b []byte) {
	if len(b) > len(a) {
		b = b[len(b)-Address32Length:]
	}
	copy(a[Address32Length-len(b):], b)
}

// MarshalText returns the hex representation of a.
func (a Address32) MarshalText() ([]byte, error) {
	return hexutil.Bytes(a[:]).MarshalText()
}

// UnmarshalText parses a hash in hex syntax.
func (a *Address32) UnmarshalText(input []byte) error {
	return hexutility.UnmarshalFixedText("Address", input, a[:])
}

// UnmarshalJSON parses a hash in hex syntax.
func (a *Address32) UnmarshalJSON(input []byte) error {
	return hexutility.UnmarshalFixedJSON(addressSt, input, a[:])
}

// ToCommonAddress converts Address32 to Address
func (a *Address32) ToCommonAddress() libcommon.Address {
	ad := libcommon.Address{}
	ad.SetBytes(a.Bytes())
	return ad
}

// Format implements fmt.Formatter.
// Address32 supports the %v, %s, %v, %x, %X and %d format verbs.
func (a Address32) Format(s fmt.State, c rune) {
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
		fmt.Fprint(s, ([len(a)]byte)(a))
	default:
		fmt.Fprintf(s, "%%!%c(address=%x)", c, a)
	}
}
