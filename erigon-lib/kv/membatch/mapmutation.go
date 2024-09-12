// Copyright 2024 The Erigon Authors
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

package membatch

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/erigontech/erigon-lib/common"

	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/log/v3"
)

type Mapmutation struct {
	puts   map[string]map[string][]byte // table -> key -> value ie. blocks -> hash -> blockBod
	db     kv.Tx
	quit   <-chan struct{}
	clean  func()
	mu     sync.RWMutex
	size   int
	count  uint64
	tmpdir string
	logger log.Logger
}

func (m *Mapmutation) Count(bucket string) (uint64, error) {
	panic("not implemented")
}

func (m *Mapmutation) BucketSize(table string) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Mapmutation) ListBuckets() ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Mapmutation) ViewID() uint64 {
	//TODO implement me
	panic("implement me")
}

func (m *Mapmutation) Cursor(table string) (kv.Cursor, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Mapmutation) CursorDupSort(table string) (kv.CursorDupSort, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Mapmutation) DBSize() (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Mapmutation) Range(table string, fromPrefix, toPrefix []byte) (stream.KV, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Mapmutation) RangeAscend(table string, fromPrefix, toPrefix []byte, limit int) (stream.KV, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Mapmutation) RangeDescend(table string, fromPrefix, toPrefix []byte, limit int) (stream.KV, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Mapmutation) Prefix(table string, prefix []byte) (stream.KV, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Mapmutation) RangeDupSort(table string, key []byte, fromPrefix, toPrefix []byte, asc order.By, limit int) (stream.KV, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Mapmutation) DropBucket(s string) error {
	//TODO implement me
	panic("implement me")
}

func (m *Mapmutation) CreateBucket(s string) error {
	//TODO implement me
	panic("implement me")
}

func (m *Mapmutation) ExistsBucket(s string) (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Mapmutation) ClearBucket(s string) error {
	//TODO implement me
	panic("implement me")
}

func (m *Mapmutation) RwCursor(table string) (kv.RwCursor, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Mapmutation) RwCursorDupSort(table string) (kv.RwCursorDupSort, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Mapmutation) CollectMetrics() {
	//TODO implement me
	panic("implement me")
}
func (m *Mapmutation) CHandle() unsafe.Pointer { return m.db.CHandle() }

// NewBatch - starts in-mem batch
//
// Common pattern:
//
// batch := db.NewBatch()
// defer batch.Close()
// ... some calculations on `batch`
// batch.Commit()
func NewHashBatch(tx kv.Tx, quit <-chan struct{}, tmpdir string, logger log.Logger) *Mapmutation {
	clean := func() {}
	if quit == nil {
		ch := make(chan struct{})
		clean = func() { close(ch) }
		quit = ch
	}

	return &Mapmutation{
		db:     tx,
		puts:   make(map[string]map[string][]byte),
		quit:   quit,
		clean:  clean,
		tmpdir: tmpdir,
		logger: logger,
	}
}

func (m *Mapmutation) getMem(table string, key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.puts[table]; !ok {
		return nil, false
	}
	if value, ok := m.puts[table][*(*string)(unsafe.Pointer(&key))]; ok {
		return value, ok
	}

	return nil, false
}

func (m *Mapmutation) IncrementSequence(bucket string, amount uint64) (res uint64, err error) {
	v, ok := m.getMem(kv.Sequence, []byte(bucket))
	if !ok && m.db != nil {
		v, err = m.db.GetOne(kv.Sequence, []byte(bucket))
		if err != nil {
			return 0, err
		}
	}

	var currentV uint64 = 0
	if len(v) > 0 {
		currentV = binary.BigEndian.Uint64(v)
	}

	newVBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(newVBytes, currentV+amount)
	if err = m.Put(kv.Sequence, []byte(bucket), newVBytes); err != nil {
		return 0, err
	}

	return currentV, nil
}
func (m *Mapmutation) ReadSequence(bucket string) (res uint64, err error) {
	v, ok := m.getMem(kv.Sequence, []byte(bucket))
	if !ok && m.db != nil {
		v, err = m.db.GetOne(kv.Sequence, []byte(bucket))
		if err != nil {
			return 0, err
		}
	}
	var currentV uint64 = 0
	if len(v) > 0 {
		currentV = binary.BigEndian.Uint64(v)
	}

	return currentV, nil
}

// Can only be called from the worker thread
func (m *Mapmutation) GetOne(table string, key []byte) ([]byte, error) {
	if value, ok := m.getMem(table, key); ok {
		return value, nil
	}
	if m.db != nil {
		// TODO: simplify when tx can no longer be parent of mutation
		value, err := m.db.GetOne(table, key)
		if err != nil {
			return nil, err
		}
		return value, nil
	}
	return nil, nil
}

func (m *Mapmutation) Last(table string) ([]byte, []byte, error) {
	c, err := m.db.Cursor(table)
	if err != nil {
		return nil, nil, err
	}
	defer c.Close()
	return c.Last()
}

func (m *Mapmutation) Has(table string, key []byte) (bool, error) {
	if _, ok := m.getMem(table, key); ok {
		return ok, nil
	}
	if m.db != nil {
		return m.db.Has(table, key)
	}
	return false, nil
}

// puts a table key with a value and if the table is not found then it appends a table
func (m *Mapmutation) Put(table string, k, v []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.puts[table]; !ok {
		m.puts[table] = make(map[string][]byte)
	}

	stringKey := string(k)

	var ok bool
	if _, ok = m.puts[table][stringKey]; ok {
		m.size += len(v) - len(m.puts[table][stringKey])
		m.puts[table][stringKey] = v
		return nil
	}
	m.puts[table][stringKey] = v
	m.size += len(k) + len(v)
	m.count++

	return nil
}

func (m *Mapmutation) Append(table string, key []byte, value []byte) error {
	return m.Put(table, key, value)
}

func (m *Mapmutation) AppendDup(table string, key []byte, value []byte) error {
	return m.Put(table, key, value)
}

func (m *Mapmutation) BatchSize() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

func (m *Mapmutation) ForEach(bucket string, fromPrefix []byte, walker func(k, v []byte) error) error {
	m.panicOnEmptyDB()
	return m.db.ForEach(bucket, fromPrefix, walker)
}

func (m *Mapmutation) ForAmount(bucket string, prefix []byte, amount uint32, walker func(k, v []byte) error) error {
	m.panicOnEmptyDB()
	return m.db.ForAmount(bucket, prefix, amount, walker)
}

func (m *Mapmutation) Delete(table string, k []byte) error {
	return m.Put(table, k, nil)
}

func (m *Mapmutation) doCommit(tx kv.RwTx) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	keyCount, total := 0, m.count
	for table, bucket := range m.puts {
		collector := etl.NewCollector("", m.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize/2), m.logger)
		defer collector.Close()
		collector.SortAndFlushInBackground(true)
		for key, value := range bucket {
			if err := collector.Collect([]byte(key), value); err != nil {
				return err
			}
			keyCount++
			select {
			default:
			case <-logEvery.C:
				progress := fmt.Sprintf("%s/%s", common.PrettyCounter(keyCount), common.PrettyCounter(total))
				m.logger.Info("Write to db", "progress", progress, "current table", table)
				tx.CollectMetrics()
			}
		}
		if err := collector.Load(tx, table, etl.IdentityLoadFunc, etl.TransformArgs{Quit: m.quit}); err != nil {
			return err
		}
		collector.Close()
	}

	tx.CollectMetrics()
	return nil
}

func (m *Mapmutation) Flush(ctx context.Context, tx kv.RwTx) error {
	if tx == nil {
		return errors.New("rwTx needed")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.doCommit(tx); err != nil {
		return err
	}

	m.puts = map[string]map[string][]byte{}
	m.size = 0
	m.count = 0
	return nil
}

func (m *Mapmutation) Close() {
	if m.clean == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.puts = map[string]map[string][]byte{}
	m.size = 0
	m.count = 0
	m.size = 0

	m.clean()
	m.clean = nil

}
func (m *Mapmutation) Commit() error { panic("not db txn, use .Flush method") }
func (m *Mapmutation) Rollback()     { panic("not db txn, use .Close method") }

func (m *Mapmutation) panicOnEmptyDB() {
	if m.db == nil {
		panic("Not implemented")
	}
}
