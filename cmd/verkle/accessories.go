package main

import (
	"encoding/binary"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
)

func ReadVerkleIncarnation(tx kv.Tx, address common.Address) (uint64, error) {
	inc, err := tx.GetOne(VerkleIncarnation, address[:])
	if err != nil {
		return 0, err
	}
	if len(inc) == 0 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(inc), nil
}
