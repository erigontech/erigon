package util

import (
	"crypto/rand"
	"fmt"
	"io"
)

var (
	byteGroups = []int{8, 4, 4, 4, 12}

	byteGroupSeparatorOffsets = []int{8, 12, 16, 20}

	hextable = [16]byte{
		'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
		'a', 'b', 'c', 'd', 'e', 'f',
	}
)

func newUUID() UUID {
	return UUID(make([]byte, 16))
}

// V4 Create a new UUID version 4.
func V4() UUID {
	uuid := newUUID()
	rand.Read(uuid)
	uuid[6] = (uuid[6] & 0x0f) | 0x40 // set version 4
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // set variant 2
	return uuid
}

// Parse parses a uuidv4 from a given string.
// valid forms are:
// - {xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx}
// - xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
// - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
func Parse(corpus string) (UUID, error) {
	if len(corpus) == 0 {
		return nil, fmt.Errorf("parse uuid: input is empty")
	}
	if len(corpus)%2 == 1 {
		return nil, fmt.Errorf("parse uuid: input is an invalid length")
	}

	uuid := newUUID()
	var data = []byte(corpus)
	var c byte
	hex := [2]byte{}
	var hexChar byte
	var isHexChar bool
	var hexIndex, uuidIndex, di int

	for i := 0; i < len(data); i++ {
		c = data[i]
		if c == '{' && i == 0 {
			continue
		}
		if c == '{' {
			return nil, fmt.Errorf("parse uuid: illegal character at %d: %v", i, string(c))
		}
		if c == '}' && i != len(data)-1 {
			return nil, fmt.Errorf("parse uuid: illegal character at %d: %v", i, string(c))
		}
		if c == '}' {
			continue
		}

		if c == '-' && !(di == 8 || di == 12 || di == 16 || di == 20) {
			return nil, fmt.Errorf("parse uuid: illegal character at %d: %v", i, string(c))
		}
		if c == '-' {
			continue
		}

		hexChar, isHexChar = fromHexChar(c)
		if !isHexChar {
			return nil, fmt.Errorf("parse uuid: illegal character at %d: %v", i, string(c))
		}

		hex[hexIndex] = hexChar
		if hexIndex == 1 {
			uuid[uuidIndex] = hex[0]<<4 | hex[1]
			uuidIndex++
			hexIndex = 0
		} else {
			hexIndex++
		}
		di++
	}
	if uuidIndex != 16 {
		return nil, fmt.Errorf("parse uuid: input is an invalid length")
	}
	return uuid, nil
}

func fromHexChar(c byte) (byte, bool) {
	switch {
	case '0' <= c && c <= '9':
		return c - '0', true
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10, true
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10, true
	}

	return 0, false
}

// UUID represents a unique identifier conforming to the RFC 4122 standard.
// UUIDs are a fixed 128bit (16 byte) binary blob.
type UUID []byte

// ToFullString returns a "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" hex representation of a uuid.
func (uuid UUID) ToFullString() string {
	b := []byte(uuid)
	return fmt.Sprintf(
		"%08x-%04x-%04x-%04x-%012x",
		b[:4], b[4:6], b[6:8], b[8:10], b[10:],
	)
}

// ToShortString returns a hex representation of the uuid.
func (uuid UUID) ToShortString() string {
	b := []byte(uuid)
	return fmt.Sprintf("%x", b[:])
}

// String is an alias for `ToShortString`.
func (uuid UUID) String() string {
	return uuid.ToShortString()
}

// Version returns the version byte of a uuid.
func (uuid UUID) Version() byte {
	return uuid[6] >> 4
}

// Format allows for conditional expansion in printf statements
// based on the token and flags used.
func (uuid UUID) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			io.WriteString(s, uuid.ToFullString())
			return
		}
		io.WriteString(s, uuid.ToShortString())
	case 's':
		io.WriteString(s, uuid.ToShortString())
	case 'q':
		fmt.Fprintf(s, "%b", uuid.Version())
	}
}

// IsV4 returns true iff uuid has version number 4, variant number 2, and length 16 bytes
func (uuid UUID) IsV4() bool {
	if len(uuid) != 16 {
		return false
	}
	// check that version number is 4
	if (uuid[6]&0xf0)^0x40 != 0 {
		return false
	}
	// check that variant is 2
	return (uuid[8]&0xc0)^0x80 == 0
}
