package dbutils

import (
	"github.com/ledgerwatch/erigon-lib/common/length"
)

func CompositeKeyWithoutIncarnation(key []byte) []byte {
	if len(key) == length.Hash*2+length.Incarnation {
		kk := make([]byte, length.Hash*2)
		copy(kk, key[:length.Hash])
		copy(kk[length.Hash:], key[length.Hash+length.Incarnation:])
		return kk
	}
	if len(key) == length.Addr+length.Hash+length.Incarnation {
		kk := make([]byte, length.Addr+length.Hash)
		copy(kk, key[:length.Addr])
		copy(kk[length.Addr:], key[length.Addr+length.Incarnation:])
		return kk
	}
	return key
}
