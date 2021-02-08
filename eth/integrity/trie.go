package integrity

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/bits"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

// AssertSubset a & b == a - checks whether a is subset of b
func AssertSubset(a, b uint16) {
	if (a & b) != a {
		panic(fmt.Errorf("invariant 'is subset' failed: %b, %b", a, b))
	}
}

func Trie(tx ethdb.Tx, quit <-chan struct{}) {
	logEvery := time.NewTicker(10 * time.Second)
	defer logEvery.Stop()

	{
		c, parentC := tx.Cursor(dbutils.TrieOfAccountsBucket), tx.Cursor(dbutils.TrieOfAccountsBucket)
		defer c.Close()
		defer parentC.Close()
		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				panic(err)
			}
			if len(k) == 1 {
				continue
			}
			select {
			default:
			case <-quit:
				return
			case <-logEvery.C:
				log.Info("trie account integrity", "key", k)
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
			var parentK []byte
			for i := len(k) - 1; i > 0; i-- {
				parentK = k[:i]
				kParent, vParent, err := parentC.SeekExact(parentK)
				if err != nil {
					panic(err)
				}
				if kParent == nil {
					continue
				}
				if bytes.HasPrefix(k, common.FromHex("00090c08")) {
					fmt.Printf("k: %x, parent: %x, %d,%d\n", k, kParent, len(kParent), len(vParent))
				}
				found = true
				parentHasBranch := binary.BigEndian.Uint16(vParent[2:])
				parentHasBit := uint16(1)<<uint16(k[len(parentK)])&parentHasBranch != 0
				if !parentHasBit {
					panic(fmt.Errorf("for %x found parent %x, but it has no branchBit: %016b", k, parentK, parentHasBranch))
				}
			}
			if !found {
				panic(fmt.Errorf("trie hash %x has no parent", k))
			}
		}
	}
	{
		c, parentC := tx.Cursor(dbutils.TrieOfStorageBucket), tx.Cursor(dbutils.TrieOfStorageBucket)
		defer c.Close()
		defer parentC.Close()
		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				panic(err)
			}
			if len(k) == 40 {
				continue
			}
			select {
			default:
			case <-quit:
				return
			case <-logEvery.C:
				log.Info("trie storage integrity", "key", k)
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
			var parentK []byte
			for i := len(k) - 1; i >= 40; i-- {
				parentK = k[:i]
				kParent, vParent, err := parentC.SeekExact(parentK)
				if err != nil {
					panic(err)
				}
				if kParent == nil {
					continue
				}
				found = true
				parentBranches := binary.BigEndian.Uint16(vParent[2:])
				parentHasBit := uint16(1)<<uint16(k[len(parentK)])&parentBranches != 0
				if !parentHasBit {
					panic(fmt.Errorf("for %x found parent %x, but it has no branchBit for child: %016b", k, parentK, parentBranches))
				}
			}
			if !found {
				panic(fmt.Errorf("trie hash %x has no parent. Last checked: %x", k, parentK))
			}
		}
	}
}
