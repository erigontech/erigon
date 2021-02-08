package integrity

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

// AssertSubset a & b == a - checks whether a is subset of b
func AssertSubset(a, b uint16) {
	if (a & b) != a {
		panic(fmt.Errorf("invariant 'is subset' failed: %b, %b", a, b))
	}
}

func Trie(db ethdb.Database) {
	if err := db.Walk(dbutils.TrieOfAccountsBucket, nil, 0, func(k, v []byte) (bool, error) {
		if len(k) == 1 {
			return true, nil
		}
		hasState := binary.BigEndian.Uint16(v)
		hasBranch := binary.BigEndian.Uint16(v[2:])
		hasHash := binary.BigEndian.Uint16(v[4:])
		AssertSubset(hasBranch, hasState)
		AssertSubset(hasHash, hasState)
		if bits.OnesCount16(hasHash) != len(v[6:])/common.HashLength {
			panic(fmt.Errorf("invariant bits.OnesCount16(hasHash) == len(hashes) failed: %d, %d", bits.OnesCount16(hasHash), len(v[6:])/common.HashLength))
		}
		found := false
		for parentK := k[:len(k)-1]; len(parentK) > 0; parentK = parentK[:len(parentK)-1] {
			parent, err := db.Get(dbutils.TrieOfAccountsBucket, parentK)
			if err != nil {
				if errors.Is(err, ethdb.ErrKeyNotFound) {
					continue
				}
				return false, err
			}
			found = true
			parentHasBranch := binary.BigEndian.Uint16(parent[2:])
			parentHasBit := uint16(1)<<uint16(k[len(parentK)])&parentHasBranch != 0
			if !parentHasBit {
				panic(fmt.Errorf("for %x found parent %x, but it has no branchBit: %016b", k, parentK, parentHasBranch))
			}
		}
		if !found {
			return true, fmt.Errorf("trie hash %x has no parent", k)
		}

		return true, nil
	}); err != nil {
		panic(err)
	}
	if err := db.Walk(dbutils.TrieOfStorageBucket, nil, 0, func(k, v []byte) (bool, error) {
		if len(k) == 40 {
			return true, nil
		}
		hasState := binary.BigEndian.Uint16(v)
		hasBranch := binary.BigEndian.Uint16(v[2:])
		hasHash := binary.BigEndian.Uint16(v[4:])
		AssertSubset(hasBranch, hasState)
		AssertSubset(hasHash, hasState)
		if bits.OnesCount16(hasHash) != len(v[6:])/common.HashLength {
			panic(fmt.Errorf("invariant bits.OnesCount16(hasHash) == len(hashes) failed: %d, %d", bits.OnesCount16(hasHash), len(v[6:])/common.HashLength))
		}

		found := false
		parentK := k
		for i := len(k) - 1; i >= 40; i-- {
			parentK = k[:i]
			parent, err := db.Get(dbutils.TrieOfStorageBucket, parentK)
			if err != nil {
				if errors.Is(err, ethdb.ErrKeyNotFound) {
					continue
				}
				panic(err)
			}
			found = true
			parentBranches := binary.BigEndian.Uint16(parent[2:])
			parentHasBit := uint16(1)<<uint16(k[len(parentK)])&parentBranches != 0
			if !parentHasBit {
				panic(fmt.Errorf("for %x found parent %x, but it has no branchBit for child: %016b", k, parentK, parentBranches))
			}
		}
		if !found {
			panic(fmt.Errorf("trie hash %x has no parent. Last checked: %x", k, parentK))
		}

		return true, nil
	}); err != nil {
		panic(err)
	}
}
