package changeset

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

type Encoder func(blockN uint64, s *ChangeSet, f func(k, v []byte) error) error
type Decoder func(dbKey, dbValue []byte) (blockN uint64, k, v []byte)

/* Plain changesets (key is a common.Address) */

func NewAccountChangeSetPlain() *ChangeSet {
	return &ChangeSet{
		Changes: make([]Change, 0),
		keyLen:  common.AddressLength,
	}
}

func EncodeAccountsPlain(blockN uint64, s *ChangeSet, f func(k, v []byte) error) error {
	return encodeAccounts2(blockN, s, f)
}

type AccountChangeSetPlain struct{ c ethdb.CursorDupSort }

func (b AccountChangeSetPlain) Find(blockNumber uint64, k []byte) ([]byte, error) {
	return findInAccountChangeSet(b.c, blockNumber, k, common.AddressLength)
}

// GetModifiedAccounts returns a list of addresses that were modified in the block range
func GetModifiedAccounts(db ethdb.Database, startNum, endNum uint64) ([]common.Address, error) {
	changedAddrs := make(map[common.Address]struct{})
	if err := Walk(db, dbutils.PlainAccountChangeSetBucket, dbutils.EncodeBlockNumber(startNum), 0, func(blockN uint64, k, v []byte) (bool, error) {
		if blockN > endNum {
			return false, nil
		}
		changedAddrs[common.BytesToAddress(k)] = struct{}{}
		return true, nil
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
