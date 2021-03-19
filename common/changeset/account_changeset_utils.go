package changeset

import (
	"bytes"
	"sort"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func findInAccountChangeSet(c ethdb.CursorDupSort, blockNumber uint64, key []byte, keyLen int) ([]byte, error) {
	fromDBFormat := FromDBFormat(keyLen)
	k := dbutils.EncodeBlockNumber(blockNumber)
	v, err := c.SeekBothRange(k, key)
	if err != nil {
		return nil, err
	}
	_, k, v = fromDBFormat(k, v)
	if !bytes.HasPrefix(k, key) {
		return nil, nil
	}
	return v, nil
}

func encodeAccounts2(blockN uint64, s *ChangeSet, f func(k, v []byte) error) error {
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
