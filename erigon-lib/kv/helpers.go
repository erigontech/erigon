// Copyright 2022 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package kv

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/mdbx-go/mdbx"

	"github.com/erigontech/erigon-lib/common/hexutility"

	"github.com/erigontech/erigon-lib/common"
)

func DefaultPageSize() uint64 {
	osPageSize := os.Getpagesize()
	if osPageSize < 4096 { // reduce further may lead to errors (because some data is just big)
		osPageSize = 4096
	} else if osPageSize > mdbx.MaxPageSize {
		osPageSize = mdbx.MaxPageSize
	}
	osPageSize = osPageSize / 4096 * 4096 // ensure it's rounded
	return uint64(osPageSize)
}

// BigChunks - read `table` by big chunks - restart read transaction after each 1 minutes
func BigChunks(db RoDB, table string, from []byte, walker func(tx Tx, k, v []byte) (bool, error)) error {
	rollbackEvery := time.NewTicker(1 * time.Minute)

	var stop bool
	for !stop {
		if err := db.View(context.Background(), func(tx Tx) error {
			c, err := tx.Cursor(table)
			if err != nil {
				return err
			}
			defer c.Close()

			k, v, err := c.Seek(from)
		Loop:
			for ; k != nil; k, v, err = c.Next() {
				if err != nil {
					return err
				}

				// break loop before walker() call, to make sure all keys are received by walker() exactly once
				select {
				case <-rollbackEvery.C:

					break Loop
				default:
				}

				ok, err := walker(tx, k, v)
				if err != nil {
					return err
				}
				if !ok {
					stop = true
					break
				}
			}

			if k == nil {
				stop = true
			}

			from = common.Copy(k) // next transaction will start from this key

			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

var (
	bytesTrue  = []byte{1}
	bytesFalse = []byte{0}
)

func bytes2bool(in []byte) bool {
	if len(in) < 1 {
		return false
	}
	return in[0] == 1
}

var ErrChanged = errors.New("key must not change")

// EnsureNotChangedBool - used to store immutable config flags in db. protects from human mistakes
func EnsureNotChangedBool(tx GetPut, bucket string, k []byte, value bool) (ok, enabled bool, err error) {
	vBytes, err := tx.GetOne(bucket, k)
	if err != nil {
		return false, enabled, err
	}
	if vBytes == nil {
		if value {
			vBytes = bytesTrue
		} else {
			vBytes = bytesFalse
		}
		if err := tx.Put(bucket, k, vBytes); err != nil {
			return false, enabled, err
		}
	}

	enabled = bytes2bool(vBytes)
	return value == enabled, enabled, nil
}

func GetBool(tx Getter, bucket string, k []byte) (enabled bool, err error) {
	vBytes, err := tx.GetOne(bucket, k)
	if err != nil {
		return false, err
	}
	return bytes2bool(vBytes), nil
}

func ReadAhead(ctx context.Context, db RoDB, progress *atomic.Bool, table string, from []byte, amount uint32) (clean func()) {
	if db == nil {
		return func() {}
	}
	if ok := progress.CompareAndSwap(false, true); !ok {
		return func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	wg := sync.WaitGroup{}
	clean = func() {
		cancel()
		wg.Wait()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer progress.Store(false)
		_ = db.View(ctx, func(tx Tx) error {
			c, err := tx.Cursor(table)
			if err != nil {
				return err
			}
			defer c.Close()

			for k, v, err := c.Seek(from); k != nil && amount > 0; k, v, err = c.Next() {
				if err != nil {
					return err
				}
				if len(v) > 0 {
					_, _ = v[0], v[len(v)-1]
				}
				amount--
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}
			return nil
		})
	}()
	return clean
}

// FirstKey - candidate on move to kv.Tx interface
func FirstKey(tx Tx, table string) ([]byte, error) {
	c, err := tx.Cursor(table)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	k, _, err := c.First()
	if err != nil {
		return nil, err
	}
	return k, nil
}

// LastKey - candidate on move to kv.Tx interface
func LastKey(tx Tx, table string) ([]byte, error) {
	c, err := tx.Cursor(table)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	k, _, err := c.Last()
	if err != nil {
		return nil, err
	}
	return k, nil
}

// NextSubtree does []byte++. Returns false if overflow.
// nil is marker of the table end, while []byte{} is in the table beginning
func NextSubtree(in []byte) ([]byte, bool) {
	r := make([]byte, len(in))
	copy(r, in)
	for i := len(r) - 1; i >= 0; i-- {
		if r[i] != 255 {
			r[i]++
			return r, true
		}

		r = r[:i] // make it shorter, because in tries after 11ff goes 12, but not 1200
	}
	return nil, false
}

func IncrementKey(tx RwTx, table string, k []byte) error {
	v, err := tx.GetOne(table, k)
	if err != nil {
		return err
	}
	var version uint64
	if len(v) == 8 {
		version = binary.BigEndian.Uint64(v)
	}
	version++
	return tx.Put(table, k, hexutility.EncodeTs(version))
}
