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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"maps"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/mdbx-go/mdbx"

	"github.com/erigontech/erigon-lib/common/hexutil"

	"github.com/erigontech/erigon-lib/common"
)

// Adapts an RoDB to the RwDB interface (invoking write operations results in error)
type RwWrapper struct {
	RoDB
}

func (w RwWrapper) Update(ctx context.Context, f func(tx RwTx) error) error {
	return errors.New("Update not implemented")
}
func (w RwWrapper) UpdateNosync(ctx context.Context, f func(tx RwTx) error) error {
	return errors.New("UpdateNosync not implemented")
}
func (w RwWrapper) BeginRw(ctx context.Context) (RwTx, error) {
	return nil, errors.New("BeginRw not implemented")
}
func (w RwWrapper) BeginRwNosync(ctx context.Context) (RwTx, error) {
	return nil, errors.New("BeginRwNosync not implemented")
}

func DefaultPageSize() datasize.ByteSize {
	osPageSize := os.Getpagesize()
	if osPageSize < 4096 { // reduce further may lead to errors (because some data is just big)
		osPageSize = 4096
	} else if osPageSize > mdbx.MaxPageSize {
		osPageSize = mdbx.MaxPageSize
	}
	osPageSize = osPageSize / 4096 * 4096 // ensure it's rounded
	return datasize.ByteSize(osPageSize)
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
func EnsureNotChangedBool(tx GetPut, bucket string, k []byte, value bool) (notChanged, enabled bool, err error) {
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
	return tx.Put(table, k, hexutil.EncodeTs(version))
}

type DomainEntryDiff struct {
	Key           string
	Value         []byte
	PrevStepBytes []byte
}

// DomainDiff represents a domain of state changes.
type DomainDiff struct {
	// We can probably flatten these into single slices for GC/cache optimization
	keys          map[string][]byte
	prevValues    map[string][]byte
	prevValsSlice []DomainEntryDiff

	prevStepBuf, currentStepBuf, keyBuf []byte
}

func (d *DomainDiff) Copy() *DomainDiff {
	return &DomainDiff{keys: maps.Clone(d.keys), prevValues: maps.Clone(d.prevValues)}
}

// RecordDelta records a state change.
func (d *DomainDiff) DomainUpdate(k []byte, step Step, prevValue []byte, prevStep Step) {
	if d.keys == nil {
		d.keys = make(map[string][]byte, 16)
		d.prevValues = make(map[string][]byte, 16)
		d.prevStepBuf = make([]byte, 8)
		d.currentStepBuf = make([]byte, 8)
	}
	binary.BigEndian.PutUint64(d.prevStepBuf, ^uint64(prevStep))
	binary.BigEndian.PutUint64(d.currentStepBuf, ^uint64(step))

	d.keyBuf = append(append(d.keyBuf[:0], k...), d.currentStepBuf...)
	key := toStringZeroCopy(d.keyBuf[:len(k)])
	if _, ok := d.keys[key]; !ok {
		d.keys[strings.Clone(key)] = common.Copy(d.prevStepBuf)
	}

	valsKey := toStringZeroCopy(d.keyBuf)
	if _, ok := d.prevValues[valsKey]; !ok {
		valsKeySCopy := strings.Clone(valsKey)
		if bytes.Equal(d.currentStepBuf, d.prevStepBuf) {
			d.prevValues[valsKeySCopy] = common.Copy(prevValue)
		} else {
			d.prevValues[valsKeySCopy] = []byte{} // We need to delete the current step but restore the previous one
		}
		d.prevValsSlice = nil
	}
}

func (d *DomainDiff) GetDiffSet() (keysToValue []DomainEntryDiff) {
	if len(d.prevValsSlice) != 0 {
		return d.prevValsSlice
	}
	d.prevValsSlice = make([]DomainEntryDiff, len(d.prevValues))
	i := 0
	for k, v := range d.prevValues {
		d.prevValsSlice[i].Key = k
		d.prevValsSlice[i].Value = v
		d.prevValsSlice[i].PrevStepBytes = d.keys[k[:len(k)-8]]
		i++
	}
	sort.Slice(d.prevValsSlice, func(i, j int) bool {
		return d.prevValsSlice[i].Key < d.prevValsSlice[j].Key
	})
	return d.prevValsSlice
}
func toStringZeroCopy(v []byte) string {
	if len(v) == 0 {
		return ""
	}
	return unsafe.String(&v[0], len(v))
}
