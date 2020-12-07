package changeset

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func findInAccountChangeSet(c ethdb.CursorDupSort, blockNumber uint64, key []byte, keyLen int) ([]byte, error) {
	fromDBFormat := FromDBFormat(keyLen)
	k, v, err := c.SeekBothRange(dbutils.EncodeBlockNumber(blockNumber), key)
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
		addrKey:=[]byte{91,181,142,163,243,235,238,244,207,200,157,89,244,152,99,31,229,13,63,145}
		if bytes.Equal(addrKey, cs.Key) {
			fmt.Println("f")
		}
		newV := make([]byte, len(cs.Key)+len(cs.Value))
		copy(newV, cs.Key)
		copy(newV[len(cs.Key):], cs.Value)
		if err := f(newK, newV); err != nil {
			return err
		}
	}
	return nil
}
