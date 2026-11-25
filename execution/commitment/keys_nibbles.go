package commitment

import (
	"strconv"
	"strings"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func KeyToHexNibbleHash(key []byte) []byte {
	if len(key) <= length.Addr {
		return KeyToNibblizedHash(key)
	}
	nibblized := make([]byte, 128)
	h := crypto.Keccak256Hash(key[:length.Addr])
	nibbles.Expand(h[:], nibblized)
	h = crypto.Keccak256Hash(key[length.Addr:])
	nibbles.Expand(h[:], nibblized[64:])
	return nibblized
}

// AddrHashCache memoizes the nibblized keccak(addr) prefix of the most recent
// storage key's address, so a run of slots under one address (whale storage)
// reuses the 64-nibble prefix instead of re-hashing the address. keccak(addr)
// is immutable, so a hit is always correct and a miss simply recomputes.
type AddrHashCache struct {
	addr  [20]byte
	nibs  [64]byte
	valid bool
}

func (c *AddrHashCache) reset() { c.valid = false }

func KeyToHexNibbleHashCached(key []byte, c *AddrHashCache) []byte {
	if len(key) <= length.Addr {
		return KeyToHexNibbleHash(key)
	}
	nibblized := make([]byte, 128)
	addr := [20]byte(key[:length.Addr])
	if c.valid && c.addr == addr {
		copy(nibblized[:64], c.nibs[:])
	} else {
		h := crypto.Keccak256Hash(key[:length.Addr])
		nibbles.Expand(h[:], nibblized[:64])
		c.addr = addr
		copy(c.nibs[:], nibblized[:64])
		c.valid = true
	}
	h := crypto.Keccak256Hash(key[length.Addr:])
	nibbles.Expand(h[:], nibblized[64:])
	return nibblized
}

func KeyToNibblizedHash(key []byte) []byte {
	nibblized := make([]byte, 64)
	h := crypto.Keccak256Hash(key)
	nibbles.Expand(h[:], nibblized)
	return nibblized
}

func NibblesToString(nibbles []byte) string {
	var b strings.Builder
	b.Grow(len(nibbles))
	for _, n := range nibbles {
		b.WriteByte("0123456789abcdef"[n&0x0F])
	}
	return b.String()
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
