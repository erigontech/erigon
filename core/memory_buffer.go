package core

import (
	"bytes"

	"github.com/ledgerwatch/erigon-lib/kv"
)

type bufferEntry struct {
	key   []byte
	value []byte
}

const defaultSize = 8388608 // This is the maximum size before doubling down

type MemoryBuffer map[string][]bufferEntry

func NewMemoryBuffer() MemoryBuffer {
	return MemoryBuffer{
		kv.AccountChangeSet: make([]bufferEntry, 0, defaultSize),
		kv.StorageChangeSet: make([]bufferEntry, 0, defaultSize),
		kv.Receipts:         make([]bufferEntry, 0, defaultSize),
		kv.CallTraceSet:     make([]bufferEntry, 0, defaultSize),
		kv.Log:              make([]bufferEntry, 0, defaultSize),
	}
}

// Put add a new entry to the memory buffer for a specific bucket
func (cb MemoryBuffer) Put(bucket string, key []byte, value []byte) error {
	// If we run out of capacity, we can just double it.
	if len(cb[bucket]) == cap(cb[bucket]) {
		tmp := make([]bufferEntry, len(cb[bucket]))
		copy(tmp, cb[bucket])
		cb[bucket] = make([]bufferEntry, len(cb[bucket]), cap(cb[bucket])*2)
		copy(cb[bucket], tmp)
	}
	cb[bucket] = append(cb[bucket], bufferEntry{
		key:   make([]byte, len(key)),
		value: make([]byte, len(value)),
	})
	index := len(cb[bucket]) - 1
	copy(cb[bucket][index].key, key)
	copy(cb[bucket][index].value, value)
	return nil
}

// Put add a new entry to the memory buffer for a specific bucket
func (cb MemoryBuffer) writeBucketToDb(tx kv.RwTx, bucket string, dup bool) error {
	c, err := tx.RwCursor(bucket)
	if err != nil {
		return err
	}
	lastK, lastV, err := c.Last()
	if err != nil {
		return err
	}
	for _, entry := range cb[bucket] {
		if dup {
			if bytes.Compare(append(entry.key, entry.value...), append(lastK, lastV...)) > 0 {
				if err := tx.AppendDup(bucket, entry.key, entry.value); err != nil {
					return err
				}
			}
		} else {
			if bytes.Compare(entry.key, lastK) > 0 {
				if err := tx.Append(bucket, entry.key, entry.value); err != nil {
					return err
				}
			}
		}
	}
	cb[bucket] = cb[bucket][:0] // We keep the capacity
	return nil
}

// Put add a new entry to the memory buffer for a specific bucket
func (cb MemoryBuffer) WriteToDb(tx kv.RwTx) error {
	// Account changeset
	if err := cb.writeBucketToDb(tx, kv.AccountChangeSet, true); err != nil {
		return err
	}
	// Storage changeset
	if err := cb.writeBucketToDb(tx, kv.StorageChangeSet, true); err != nil {
		return err
	}
	// Call traces
	if err := cb.writeBucketToDb(tx, kv.CallTraceSet, true); err != nil {
		return err
	}
	// Receipts
	if err := cb.writeBucketToDb(tx, kv.Receipts, false); err != nil {
		return err
	}
	// Logs
	if err := cb.writeBucketToDb(tx, kv.Log, false); err != nil {
		return err
	}
	return nil
}

func (cb MemoryBuffer) Append(bucket string, k, v []byte) error {
	return cb.Put(bucket, k, v)
}

func (cb MemoryBuffer) AppendDup(bucket string, k, v []byte) error {
	return cb.Put(bucket, k, v)
}

func (cb MemoryBuffer) IncrementSequence(bucket string, amount uint64) (uint64, error) {
	panic("not implemented")
}

func (cb MemoryBuffer) Delete(bucket string, k, v []byte) error {
	panic("not implemented")
}
