package v2

import (
	"fmt"
	"strings"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func updatedNibs(num uint16) string {
	var nibbles []string
	for i := range 16 {
		if num&(1<<i) != 0 {
			nibbles = append(nibbles, fmt.Sprintf("%X", i))
		}
	}
	return strings.Join(nibbles, ",")
}

func hashKey(plainKey []byte, dest []byte, hashedKeyOffset int16, hashBuf []byte) {
	_, _ = hashBuf[length.Hash-1], dest[length.Hash*2-1]
	h := crypto.Keccak256Hash(plainKey)
	copy(hashBuf, h[:])
	hb := hashBuf[hashedKeyOffset/2:]
	if hashedKeyOffset%2 == 1 {
		dest[0] = hb[0] & 0xf
		dest, hb = dest[1:], hb[1:]
	}
	nibbles.Expand(hb, dest)
}
