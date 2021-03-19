package changeset

import (
	"bytes"
	"encoding/binary"
	"sort"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func findInStorageChangeSet2(c ethdb.CursorDupSort, blockNumber uint64, keyPrefixLen int, k []byte) ([]byte, error) {
	return doSearch2(
		c, blockNumber,
		keyPrefixLen,
		k[:keyPrefixLen],
		k[keyPrefixLen+common.IncarnationLength:keyPrefixLen+common.HashLength+common.IncarnationLength],
		binary.BigEndian.Uint64(k[keyPrefixLen:]), /* incarnation */
	)
}

func findWithoutIncarnationInStorageChangeSet2(c ethdb.CursorDupSort, blockNumber uint64, keyPrefixLen int, addrBytesToFind []byte, keyBytesToFind []byte) ([]byte, error) {
	return doSearch2(
		c, blockNumber,
		keyPrefixLen,
		addrBytesToFind,
		keyBytesToFind,
		0, /* incarnation */
	)
}

func doSearch2(
	c ethdb.CursorDupSort,
	blockNumber uint64,
	keyPrefixLen int,
	addrBytesToFind []byte,
	keyBytesToFind []byte,
	incarnation uint64,
) ([]byte, error) {
	fromDBFormat := FromDBFormat(keyPrefixLen)
	if incarnation == 0 {
		seek := make([]byte, 8+keyPrefixLen)
		binary.BigEndian.PutUint64(seek, blockNumber)
		copy(seek[8:], addrBytesToFind)
		for k, v, err := c.Seek(seek); k != nil; k, v, err = c.Next() {
			if err != nil {
				return nil, err
			}
			_, k, v = fromDBFormat(k, v)
			if !bytes.HasPrefix(k, addrBytesToFind) {
				return nil, ErrNotFound
			}

			stHash := k[keyPrefixLen+common.IncarnationLength:]
			if bytes.Equal(stHash, keyBytesToFind) {
				return v, nil
			}
		}
		return nil, ErrNotFound
	}

	seek := make([]byte, 8+keyPrefixLen+common.IncarnationLength)
	binary.BigEndian.PutUint64(seek, blockNumber)
	copy(seek[8:], addrBytesToFind)
	binary.BigEndian.PutUint64(seek[8+keyPrefixLen:], incarnation)
	k := seek
	v, err := c.SeekBothRange(seek, keyBytesToFind)
	if err != nil {
		return nil, err
	}
	if !bytes.HasPrefix(v, keyBytesToFind) {
		return nil, ErrNotFound
	}
	_, _, v = fromDBFormat(k, v)
	return v, nil
}

func encodeStorage2(blockN uint64, s *ChangeSet, keyPrefixLen uint32, f func(k, v []byte) error) error {
	sort.Sort(s)
	keyPart := keyPrefixLen + common.IncarnationLength
	for _, cs := range s.Changes {
		newK := make([]byte, 8+keyPart)
		binary.BigEndian.PutUint64(newK, blockN)
		copy(newK[8:], cs.Key[:keyPart])
		newV := make([]byte, 0, common.HashLength+len(cs.Value))
		newV = append(append(newV, cs.Key[keyPart:]...), cs.Value...)
		if err := f(newK, newV); err != nil {
			return err
		}
	}
	return nil
}
