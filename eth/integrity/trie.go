package integrity

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/bits"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/trie"
)

// AssertSubset a & b == a - checks whether a is subset of b
func AssertSubset(prefix []byte, a, b uint16) {
	if (a & b) != a {
		panic(fmt.Errorf("invariant 'is subset' failed: %x, %b, %b", prefix, a, b))
	}
}

func Trie(tx ethdb.Tx, slowChecks bool, quit <-chan struct{}) {
	logEvery := time.NewTicker(10 * time.Second)
	defer logEvery.Stop()
	seek := make([]byte, 256)
	buf := make([]byte, 256)
	buf2 := make([]byte, 256)

	{
		c, trieAcc2, accC := tx.Cursor(dbutils.TrieOfAccountsBucket), tx.Cursor(dbutils.TrieOfAccountsBucket), tx.Cursor(dbutils.HashedAccountsBucket)
		defer c.Close()
		defer trieAcc2.Close()
		defer accC.Close()
		for k, v, errc := c.First(); k != nil; k, v, errc = c.Next() {
			if errc != nil {
				panic(errc)
			}
			hasState, hasBranch, hasHash, hashes, _ := trie.UnmarshalIH(v)
			AssertSubset(k, hasBranch, hasState)
			AssertSubset(k, hasHash, hasState)
			if bits.OnesCount16(hasHash) != len(hashes)/common.HashLength {
				panic(fmt.Errorf("invariant bits.OnesCount16(hasHash) == len(hashes) failed: %d, %d", bits.OnesCount16(hasHash), len(v[6:])/common.HashLength))
			}
			found := false
			var parentK []byte

			// must have parent with right hasBranch bit
			for i := len(k) - 1; i > 0 && !found; i-- {
				parentK = k[:i]
				kParent, vParent, err := trieAcc2.SeekExact(parentK)
				if err != nil {
					panic(err)
				}
				if kParent == nil {
					continue
				}
				found = true
				parentHasBranch := binary.BigEndian.Uint16(vParent[2:])
				parentHasBit := 1<<uint16(k[len(parentK)])&parentHasBranch != 0
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
				k2, _, err := trieAcc2.Seek(seek)
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

			if !slowChecks {
				continue
			}
			// each IH must cover some state
			buf = buf[:len(k)+1]
			copy(buf, k)
			for i := uint16(0); i < 16; i++ {
				if 1<<i&hasState == 0 {
					continue
				}

				found := false
				buf = buf[:len(k)+1]
				buf[len(buf)-1] = uint8(i)
				bitsToMatch := len(buf) * 4
				if len(buf)%2 == 1 {
					buf = append(buf, 0)
				}
				hexutil.CompressNibbles(buf, &seek)
				if err := ethdb.Walk(accC, seek, bitsToMatch, func(k, v []byte) (bool, error) {
					found = true
					return false, nil
				}); err != nil {
					panic(err)
				}
				if !found {
					panic(fmt.Errorf("key %x has state %016b, but there is no child %d,%x in state", k, hasState, i, seek))
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
		c, trieStorage, storageC := tx.Cursor(dbutils.TrieOfStorageBucket), tx.Cursor(dbutils.TrieOfStorageBucket), tx.Cursor(dbutils.HashedStorageBucket)
		defer c.Close()
		defer trieStorage.Close()
		defer storageC.Close()
		for k, v, errc := c.First(); k != nil; k, v, errc = c.Next() {
			if errc != nil {
				panic(errc)
			}

			hasState, hasBranch, hasHash, hashes, _ := trie.UnmarshalIH(v)
			AssertSubset(k, hasBranch, hasState)
			AssertSubset(k, hasHash, hasState)
			if bits.OnesCount16(hasHash) != len(hashes)/common.HashLength {
				panic(fmt.Errorf("invariant bits.OnesCount16(hasHash) == len(hashes) failed: %d, %d", bits.OnesCount16(hasHash), len(hashes)/common.HashLength))
			}

			found := false
			var parentK []byte

			// must have parent with right hasBranch bit
			for i := len(k) - 1; i >= 40 && !found; i-- {
				parentK = k[:i]
				kParent, vParent, err := trieStorage.SeekExact(parentK)
				if err != nil {
					panic(err)
				}
				if kParent == nil {
					continue
				}

				found = true
				parentBranches := binary.BigEndian.Uint16(vParent[2:])
				parentHasBit := 1<<uint16(k[len(parentK)])&parentBranches != 0
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
				k2, _, err := trieStorage.Seek(seek)
				if err != nil {
					panic(err)
				}
				if !bytes.HasPrefix(k2, seek) {
					panic(fmt.Errorf("key %x has branches %016b, but there is no child %d in db", k, hasBranch, i))
				}
			}

			if !slowChecks {
				continue
			}
			// each IH must cover some state
			buf = buf[:len(k)-40+1]
			copy(buf, k[40:])
			for i := uint16(0); i < 16; i++ {
				if 1<<i&hasState == 0 {
					continue
				}
				found := false
				buf = buf[:len(k)-40+1]
				buf[len(buf)-1] = uint8(i)
				bitsToMatch := 40*8 + len(buf)*4
				if len(buf)%2 == 1 {
					buf = append(buf, 0)
				}
				hexutil.CompressNibbles(buf, &buf2)
				seek = seek[:40+len(buf2)]
				copy(seek, k[:40])
				copy(seek[40:], buf2)
				if bytes.HasPrefix(k, common.FromHex("94537c5bb46d62873557759260e8aebff5e3048f362d7bf90705cda631af38210000000000000001")) {
					fmt.Printf("testing: %x,%d\n", seek, bitsToMatch)
				}
				if err := ethdb.Walk(storageC, seek, bitsToMatch, func(k, v []byte) (bool, error) {
					if bytes.HasPrefix(k, common.FromHex("94537c5bb46d62873557759260e8aebff5e3048f362d7bf90705cda631af38210000000000000001")) {
						fmt.Printf("testing2: %x\n", k)
					}
					found = true
					return false, nil
				}); err != nil {
					panic(err)
				}
				if !found {
					panic(fmt.Errorf("key %x has state %016b, but there is no child %d,%x in state", k, hasState, i, seek))
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
