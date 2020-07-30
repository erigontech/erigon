package etl

import (
	"bytes"
	"sort"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

// MemoryCollector collects key-value pairs into memory and then pushes them to database sorted by key
type MemoryCollector struct {
	buffer    []byte
	lenK      int
	lenV      int
	pos       int
	entrySize int
}

// NewMemoryCollector creates a MemoryCollector
func NewMemoryCollector(lenK int, lenV int, maxEntries int) *MemoryCollector {
	return &MemoryCollector{
		buffer:    make([]byte, maxEntries*(lenK+lenV)),
		lenK:      lenK,
		lenV:      lenV,
		entrySize: lenK + lenV,
	}
}

func (m MemoryCollector) Len() int { return len(m.buffer) / m.entrySize }

func (m MemoryCollector) Less(i, j int) bool {
	return bytes.Compare(m.buffer[m.entrySize*i:m.entrySize*i+m.entrySize], m.buffer[m.entrySize*j:m.entrySize*j+m.entrySize]) < 0
}

func (m MemoryCollector) Swap(i, j int) {
	tmp := common.CopyBytes(m.buffer[m.entrySize*i : m.entrySize*i+m.entrySize])
	copy(m.buffer[m.entrySize*i:m.entrySize*i+m.entrySize], m.buffer[m.entrySize*j:m.entrySize*j+m.entrySize])
	copy(m.buffer[m.entrySize*j:m.entrySize*j+m.entrySize], tmp)
}

// Put puts key-value pairs into the buffer
func (m *MemoryCollector) Put(k, v []byte) {
	copy(m.buffer[m.pos:], k)
	copy(m.buffer[m.pos+m.lenK:], v)
	m.pos += m.entrySize
}

// Last retrieves the last key-value put
func (m *MemoryCollector) Last() (k []byte, v []byte) {
	k = m.buffer[m.pos : m.pos+m.lenK]
	v = m.buffer[m.pos+m.lenK : m.pos+m.entrySize]
	return
}

// Commit commits what the collector collected in a bucket in ordered sequence to the Database
func (m *MemoryCollector) Commit(db ethdb.Database, bucket []byte) error {
	batch := db.NewBatch()
	sort.Sort(m)
	zero := make([]byte, 40)
	w := 0
	for i := 0; i < len(m.buffer); i += m.entrySize {
		if bytes.Equal(zero, m.buffer[i:i+m.entrySize]) {
			continue
		}
		w++
		if err := batch.Put(bucket, common.CopyBytes(m.buffer[i:i+m.lenK]), common.CopyBytes(m.buffer[i+m.lenK:i+m.entrySize])); err != nil {
			return err
		}
		if batch.BatchSize() >= batch.IdealBatchSize() {
			_, err := batch.Commit()
			if err != nil {
				return err
			}
		}
	}
	_, err := batch.Commit()
	return err
}
