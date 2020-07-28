// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package ethdb defines the interfaces for an Ethereum data store.
package ethdb

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/pprof"
	"os"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
)

func init() {
	// go tool pprof -http=:8081 http://localhost:6060/
	_ = pprof.Handler // just to avoid adding manually: import _ "net/http/pprof"
	go func() {
		r := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
		randomPort := func(min, max int) int {
			return r.Intn(max-min) + min
		}
		port := randomPort(6000, 7000)

		fmt.Fprintf(os.Stderr, "go tool pprof -lines -http=: :%d/%s\n", port, "\\?seconds\\=20")
		fmt.Fprintf(os.Stderr, "go tool pprof -lines -http=: :%d/%s\n", port, "debug/pprof/heap")
		fmt.Fprintf(os.Stderr, "%s\n", http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", port), nil))
	}()
}

// Type which expecting sequence of triplets: dbi, key, value, ....
// It sorts entries by dbi name, then inside dbi clusters sort by keys
type MultiPutTuples [][]byte

func (t MultiPutTuples) Len() int { return len(t) / 3 }

func (t MultiPutTuples) Less(i, j int) bool {
	cmp := bytes.Compare(t[i*3], t[j*3])
	if cmp == -1 {
		return true
	}
	if cmp == 0 {
		return bytes.Compare(t[i*3+1], t[j*3+1]) == -1
	}
	return false
}

func (t MultiPutTuples) Swap(i, j int) {
	t[i*3], t[j*3] = t[j*3], t[i*3]
	t[i*3+1], t[j*3+1] = t[j*3+1], t[i*3+1]
	t[i*3+2], t[j*3+2] = t[j*3+2], t[i*3+2]
}

func Get(db KV, bucket, key []byte) ([]byte, error) {
	// Retrieve the key and increment the miss counter if not found
	var dat []byte
	err := db.View(context.Background(), func(tx Tx) error {
		b := tx.Bucket(bucket)
		if b != nil {
			v, _ := b.Get(key)
			if v != nil {
				dat = make([]byte, len(v))
				copy(dat, v)
			}
		}
		return nil
	})
	if dat == nil {
		return nil, ErrKeyNotFound
	}
	return dat, err
}

func HackAddRootToAccountBytes(accNoRoot []byte, root []byte) (accWithRoot []byte, err error) {
	var acc accounts.Account
	if err := acc.DecodeForStorage(accNoRoot); err != nil {
		return nil, err
	}
	acc.Root = common.BytesToHash(root)
	accWithRoot = make([]byte, acc.EncodingLengthForStorage())
	acc.EncodeForStorage(accWithRoot)
	return accWithRoot, nil
}

func Bytesmask(fixedbits int) (fixedbytes int, mask byte) {
	fixedbytes = (fixedbits + 7) / 8
	shiftbits := fixedbits & 7
	mask = byte(0xff)
	if shiftbits != 0 {
		mask = 0xff << (8 - shiftbits)
	}
	return fixedbytes, mask
}

func InspectDatabase(db Database) error {
	// FIXME: implement in Turbo-Geth
	// see https://github.com/ethereum/go-ethereum/blob/f5d89cdb72c1e82e9deb54754bef8dd20bf12591/core/rawdb/database.go#L224
	return errNotSupported
}

func NewDatabaseWithFreezer(db *ObjectDatabase, dir, suffix string) (*ObjectDatabase, error) {
	// FIXME: implement freezer in Turbo-Geth
	return db, nil
}
