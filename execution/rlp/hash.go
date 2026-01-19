package rlp

import (
	"github.com/erigontech/erigon/common"
	"golang.org/x/crypto/sha3"
)

func RlpHash(x interface{}) (h common.Hash) {
	hw := sha3.NewLegacyKeccak256()
	Encode(hw, x) //nolint:errcheck
	hw.Sum(h[:0])
	return h
}
