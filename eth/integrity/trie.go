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
	"github.com/ledgerwatch/turbo-geth/turbo/trie"
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
	seek := make([]byte, 256)

	{
		c, c2 := tx.Cursor(dbutils.TrieOfAccountsBucket), tx.Cursor(dbutils.TrieOfAccountsBucket)
		defer c.Close()
		defer c2.Close()
		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				panic(err)
			}
			hasState, hasBranch, hasHash, hashes, _ := trie.UnmarshalIH(v)
			AssertSubset(hasBranch, hasState)
			AssertSubset(hasHash, hasState)
			if bits.OnesCount16(hasHash) != len(hashes)/common.HashLength {
				panic(fmt.Errorf("invariant bits.OnesCount16(hasHash) == len(hashes) failed: %d, %d", bits.OnesCount16(hasHash), len(v[6:])/common.HashLength))
			}
			found := false
			var parentK []byte

			// must have parent with right hasBranch bit
			for i := len(k) - 1; i > 0 && !found; i-- {
				parentK = k[:i]
				kParent, vParent, err := c2.SeekExact(parentK)
				if err != nil {
					panic(err)
				}
				if kParent == nil {
					continue
				}
				found = true
				parentHasBranch := binary.BigEndian.Uint16(vParent[2:])
				parentHasBit := uint16(1)<<uint16(k[len(parentK)])&parentHasBranch != 0
				if !parentHasBit {
					panic(fmt.Errorf("for %x found parent %x, but it has no branchBit: %016b", k, parentK, parentHasBranch))
				}
			}
			if !found && len(k) > 1 {
				panic(fmt.Errorf("trie hash %x has no parent", k))
			}

			// must have all children
			seek = seek[:len(k)+1]
			copy(seek, k)
			for i := uint16(0); i < 16; i++ {
				if 1<<i&hasBranch == 0 {
					continue
				}
				seek[len(seek)-1] = uint8(i)
				k2, _, err := c2.Seek(seek)
				if err != nil {
					panic(err)
				}
				if k2 == nil {
					panic(fmt.Errorf("key %x has branches %016b, but there is no child %d in db; last seen key: %x->nil", k, hasBranch, i, seek))
				}
				if !bytes.HasPrefix(k2, seek) {
					panic(fmt.Errorf("key %x has branches %016b, but there is no child %d in db; last seen key: %x->%x", k, hasBranch, i, seek, k2))
				}
			}
			select {
			default:
			case <-quit:
				return
			case <-logEvery.C:
				log.Info("trie account integrity", "key", fmt.Sprintf("%x", k))
			}
		}
	}
	{
		c, c2 := tx.Cursor(dbutils.TrieOfStorageBucket), tx.Cursor(dbutils.TrieOfStorageBucket)
		defer c.Close()
		defer c2.Close()
		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				panic(err)
			}

			hasState, hasBranch, hasHash, hashes, _ := trie.UnmarshalIH(v)
			AssertSubset(hasBranch, hasState)
			AssertSubset(hasHash, hasState)
			if bits.OnesCount16(hasHash) != len(hashes)/common.HashLength {
				panic(fmt.Errorf("invariant bits.OnesCount16(hasHash) == len(hashes) failed: %d, %d", bits.OnesCount16(hasHash), len(hashes)/common.HashLength))
			}

			found := false
			var parentK []byte

			// must have parent with right hasBranch bit
			for i := len(k) - 1; i >= 40 && !found; i-- {
				parentK = k[:i]
				kParent, vParent, err := c2.SeekExact(parentK)
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
			if !found && len(k) > 40 {
				panic(fmt.Errorf("trie hash %x has no parent. Last checked: %x", k, parentK))
			}

			// must have all children
			seek = seek[:len(k)+1]
			copy(seek, k)
			for i := uint16(0); i < 16; i++ {
				if 1<<i&hasBranch == 0 {
					continue
				}
				seek[len(seek)-1] = uint8(i)
				k2, _, err := c2.Seek(seek)
				if err != nil {
					panic(err)
				}
				if !bytes.HasPrefix(k2, seek) {
					panic(fmt.Errorf("key %x has branches %016b, but there is no child %d in db", k, hasBranch, i))
				}
			}

			select {
			default:
			case <-quit:
				return
			case <-logEvery.C:
				log.Info("trie storage integrity", "key", fmt.Sprintf("%x", k))
			}
		}
	}
}
