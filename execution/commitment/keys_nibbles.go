package commitment

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common/length"
)

func KeyToHexNibbleHash(key []byte) []byte {
	var nibblized, hashed []byte
	if len(key) > length.Addr {
		nibblized = make([]byte, 128)
		hashed = nibblized[64:]
		h := keccak.Sum256(key[:length.Addr])
		copy(hashed[:32], h[:])
		h = keccak.Sum256(key[length.Addr:])
		copy(hashed[32:], h[:])
	} else {
		nibblized = make([]byte, 64)
		hashed = nibblized[32:]
		h := keccak.Sum256(key)
		copy(hashed, h[:])
	}

	for i, b := range hashed {
		nibblized[i*2] = (b >> 4) & 0xf
		nibblized[i*2+1] = b & 0xf
	}
	return nibblized
}

// src and dst must not overlap.
func expandNibbles(src, dst []byte) {
	_ = dst[len(src)*2-1]
	for i, b := range src {
		dst[i*2] = (b >> 4) & 0xf
		dst[i*2+1] = b & 0xf
	}
}

type addrHashCache struct {
	addr  [20]byte
	nibs  [64]byte
	valid bool
}

func (c *addrHashCache) reset() { c.valid = false }

func keyToHexNibbleHashCached(key []byte, c *addrHashCache) []byte {
	if len(key) <= length.Addr {
		return KeyToHexNibbleHash(key)
	}
	nibblized := make([]byte, 128)
	addr := [20]byte(key[:length.Addr])
	if c.valid && c.addr == addr {
		copy(nibblized[:64], c.nibs[:])
	} else {
		h := keccak.Sum256(key[:length.Addr])
		expandNibbles(h[:], nibblized[:64])
		c.addr = addr
		copy(c.nibs[:], nibblized[:64])
		c.valid = true
	}
	h := keccak.Sum256(key[length.Addr:])
	expandNibbles(h[:], nibblized[64:])
	return nibblized
}

func KeyToNibblizedHash(key []byte) []byte {
	nibblized := make([]byte, 64)
	hashed := nibblized[32:]
	h := keccak.Sum256(key)
	copy(hashed, h[:])
	for i, b := range hashed {
		nibblized[i*2] = (b >> 4) & 0xf
		nibblized[i*2+1] = b & 0xf
	}
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

func CompactKey(nibbles []byte) ([]byte, error) {
	if len(nibbles)%2 != 0 {
		return nil, errors.New("nibbles slice has an odd length")
	}

	key := make([]byte, len(nibbles)/2)
	for i := range key {
		highNibble := nibbles[i*2]
		lowNibble := nibbles[i*2+1]

		if highNibble > 0xF || lowNibble > 0xF {
			return nil, fmt.Errorf("invalid nibble at position %d or %d: 0x%X, 0x%X", i*2, i*2+1, highNibble, lowNibble)
		}

		key[i] = (highNibble << 4) | (lowNibble & 0x0F)
	}
	return key, nil
}

func updatedNibs(num uint16) string {
	var nibbles []string
	for i := range 16 {
		if num&(1<<i) != 0 {
			nibbles = append(nibbles, fmt.Sprintf("%X", i))
		}
	}
	return strings.Join(nibbles, ",")
}

func hashKey(hasher keccak.KeccakState, plainKey []byte, dest []byte, hashedKeyOffset int16, hashBuf []byte) error {
	_, _ = hashBuf[length.Hash-1], dest[length.Hash*2-1]
	hasher.Reset()
	if _, err := hasher.Write(plainKey); err != nil {
		return err
	}
	if _, err := hasher.Read(hashBuf); err != nil {
		return err
	}
	hb := hashBuf[hashedKeyOffset/2:]
	var k int
	if hashedKeyOffset%2 == 1 { // write zero byte as compacted since hashedKeyOffset is odd
		dest[0] = hb[0] & 0xf
		k++
		hb = hb[1:]
	}
	for _, c := range hb {
		dest[k] = (c >> 4) & 0xf
		k++
		dest[k] = c & 0xf
		k++
	}
	return nil
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
