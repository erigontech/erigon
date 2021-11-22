package changeset

import (
	"bytes"
	"encoding/binary"
	"sort"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
)

type Encoder func(blockN uint64, s *ChangeSet, f func(k, v []byte) error) error
type Decoder func(dbKey, dbValue []byte) (blockN uint64, k, v []byte)

func NewAccountChangeSet() *ChangeSet {
	return &ChangeSet{
		Changes: make([]Change, 0),
		keyLen:  length.Addr,
	}
}

func EncodeAccounts(blockN uint64, s *ChangeSet, f func(k, v []byte) error) error {
	sort.Sort(s)
	newK := dbutils.EncodeBlockNumber(blockN)
	for _, cs := range s.Changes {
		newV := make([]byte, len(cs.Key)+len(cs.Value))
		copy(newV, cs.Key)
		copy(newV[len(cs.Key):], cs.Value)
		if err := f(newK, newV); err != nil {
			return err
		}
	}
	return nil
}

func DecodeAccounts(dbKey, dbValue []byte) (uint64, []byte, []byte) {
	blockN := binary.BigEndian.Uint64(dbKey)
	k := dbValue[:length.Addr]
	v := dbValue[length.Addr:]

	return blockN, k, v
}

func FindAccount(c kv.CursorDupSort, blockNumber uint64, key []byte) ([]byte, error) {
	k := dbutils.EncodeBlockNumber(blockNumber)
	v, err := c.SeekBothRange(k, key)
	if err != nil {
		return nil, err
	}
	_, k, v = DecodeAccounts(k, v)
	if !bytes.HasPrefix(k, key) {
		return nil, nil
	}
	return v, nil
}

// GetModifiedAccounts returns a list of addresses that were modified in the block range
// [startNum:endNum)
func GetModifiedAccounts(db kv.Tx, startNum, endNum uint64) ([]common.Address, error) {
	changedAddrs := make(map[common.Address]struct{})
	if err := ForRange(db, kv.AccountChangeSet, startNum, endNum, func(blockN uint64, k, v []byte) error {
		changedAddrs[common.BytesToAddress(k)] = struct{}{}
		return nil
	}); err != nil {
		return nil, err
	}

	if len(changedAddrs) == 0 {
		return nil, nil
	}

	idx := 0
	result := make([]common.Address, len(changedAddrs))
	for addr := range changedAddrs {
		copy(result[idx][:], addr[:])
		idx++
	}

	return result, nil
}
