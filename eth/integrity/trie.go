package integrity

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/bits"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/ledgerwatch/log/v3"
)

// AssertSubset a & b == a - checks whether a is subset of b
func AssertSubset(prefix []byte, a, b uint16) {
	if (a & b) != a {
		panic(fmt.Errorf("invariant 'is subset' failed: %x, %b, %b", prefix, a, b))
	}
}

func Trie(db kv.RoDB, tx kv.Tx, slowChecks bool, ctx context.Context) {
	quit := ctx.Done()
	readAheadCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	logEvery := time.NewTicker(10 * time.Second)
	defer logEvery.Stop()
	seek := make([]byte, 256)
	buf := make([]byte, 256)
	buf2 := make([]byte, 256)

	{
		c, err := tx.Cursor(kv.TrieOfAccounts)
		if err != nil {
			panic(err)
		}
		defer c.Close()
		clear := kv.ReadAhead(readAheadCtx, db, &atomic.Bool{}, kv.TrieOfAccounts, nil, math.MaxInt32)
		defer clear()

		trieAcc2, err := tx.Cursor(kv.TrieOfAccounts)
		if err != nil {
			panic(err)
		}
		defer trieAcc2.Close()

		accC, err := tx.Cursor(kv.HashedAccounts)
		if err != nil {
			panic(err)
		}
		defer accC.Close()
		clear2 := kv.ReadAhead(readAheadCtx, db, &atomic.Bool{}, kv.HashedAccounts, nil, math.MaxInt32)
		defer clear2()

		for k, v, errc := c.First(); k != nil; k, v, errc = c.Next() {
			if errc != nil {
				panic(errc)
			}
			select {
			default:
			case <-quit:
				return
			case <-logEvery.C:
				log.Info("trie account integrity", "key", hex.EncodeToString(k))
			}

			hasState, hasTree, hasHash, hashes, _ := trie.UnmarshalTrieNode(v)
			AssertSubset(k, hasTree, hasState)
			AssertSubset(k, hasHash, hasState)
			if bits.OnesCount16(hasHash) != len(hashes)/length.Hash {
				panic(fmt.Errorf("invariant bits.OnesCount16(hasHash) == len(hashes) failed: %d, %d", bits.OnesCount16(hasHash), len(v[6:])/length.Hash))
			}
			found := false
			var parentK []byte

			// must have parent with right hasTree bit
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
				parenthasTree := binary.BigEndian.Uint16(vParent[2:])
				parentHasBit := 1<<uint16(k[len(parentK)])&parenthasTree != 0
				if !parentHasBit {
					panic(fmt.Errorf("for %x found parent %x, but it has no branchBit: %016b", k, parentK, parenthasTree))
				}
			}
			if !found && len(k) > 1 {
				panic(fmt.Errorf("trie hash %x has no parent", k))
			}

			// must have all children
			seek = seek[:len(k)+1]
			copy(seek, k)
			for i := uint16(0); i < 16; i++ {
				if 1<<i&hasTree == 0 {
					continue
				}
				seek[len(seek)-1] = uint8(i)
				k2, _, err := trieAcc2.Seek(seek)
				if err != nil {
					panic(err)
				}
				if k2 == nil {
					panic(fmt.Errorf("key %x has branches %016b, but there is no child %d in db; last seen key: %x->nil", k, hasTree, i, seek))
				}
				if !bytes.HasPrefix(k2, seek) {
					panic(fmt.Errorf("key %x has branches %016b, but there is no child %d in db; last seen key: %x->%x", k, hasTree, i, seek, k2))
				}
			}

			if !slowChecks {
				continue
			}
			// each AccTrie must cover some state
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
		}
	}
	{
		c, err := tx.Cursor(kv.TrieOfStorage)
		if err != nil {
			panic(err)
		}
		defer c.Close()
		clear := kv.ReadAhead(readAheadCtx, db, &atomic.Bool{}, kv.TrieOfStorage, nil, math.MaxInt32)
		defer clear()

		trieStorage, err := tx.Cursor(kv.TrieOfStorage)
		if err != nil {
			panic(err)
		}
		defer trieStorage.Close()

		storageC, err := tx.Cursor(kv.HashedStorage)
		if err != nil {
			panic(err)
		}
		defer storageC.Close()
		clear2 := kv.ReadAhead(readAheadCtx, db, &atomic.Bool{}, kv.HashedStorage, nil, math.MaxInt32)
		defer clear2()

		for k, v, errc := c.First(); k != nil; k, v, errc = c.Next() {
			if errc != nil {
				panic(errc)
			}
			select {
			default:
			case <-quit:
				return
			case <-logEvery.C:
				log.Info("trie storage integrity", "key", hex.EncodeToString(k))
			}

			hasState, hasTree, hasHash, hashes, _ := trie.UnmarshalTrieNode(v)
			AssertSubset(k, hasTree, hasState)
			AssertSubset(k, hasHash, hasState)
			if bits.OnesCount16(hasHash) != len(hashes)/length.Hash {
				panic(fmt.Errorf("invariant bits.OnesCount16(hasHash) == len(hashes) failed: %d, %d", bits.OnesCount16(hasHash), len(hashes)/length.Hash))
			}

			found := false
			var parentK []byte

			// must have parent with right hasTree bit
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
				if 1<<i&hasTree == 0 {
					continue
				}
				seek[len(seek)-1] = uint8(i)
				k2, _, err := trieStorage.Seek(seek)
				if err != nil {
					panic(err)
				}
				if !bytes.HasPrefix(k2, seek) {
					panic(fmt.Errorf("key %x has branches %016b, but there is no child %d in db", k, hasTree, i))
				}
			}

			if !slowChecks {
				continue
			}
			// each AccTrie must cover some state
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
				if err := ethdb.Walk(storageC, seek, bitsToMatch, func(k, v []byte) (bool, error) {
					found = true
					return false, nil
				}); err != nil {
					panic(err)
				}
				if !found {
					panic(fmt.Errorf("key %x has state %016b, but there is no child %d,%x in state", k, hasState, i, seek))
				}
			}
		}
	}
}
