package gointerfaces

import (
	"encoding/binary"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/gointerfaces/types"
)

func ConvertH256toHash(h256 *types.H256) common.Hash {
	var hash common.Hash
	binary.BigEndian.PutUint64(hash[0:], h256.Lo.Lo)
	binary.BigEndian.PutUint64(hash[8:], h256.Lo.Hi)
	binary.BigEndian.PutUint64(hash[16:], h256.Hi.Lo)
	binary.BigEndian.PutUint64(hash[24:], h256.Hi.Hi)
	return hash 
}
