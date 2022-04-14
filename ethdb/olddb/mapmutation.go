package olddb

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/log/v3"
)

type mutationKey [61]byte

type mapmutation struct {
	puts              map[string]map[mutationKey][]byte
	whitelistedTables map[string]byte
	whitelistCache    *lru.Cache
	db                kv.RwTx
	quit              <-chan struct{}
	clean             func()
	mu                sync.RWMutex
	size              int
	count             uint64
	tmpdir            string
}

// NewBatch - starts in-mem batch
//
// Common pattern:
//
// batch := db.NewBatch()
// defer batch.Rollback()
// ... some calculations on `batch`
// batch.Commit()
func NewHashBatch(tx kv.RwTx, quit <-chan struct{}, tmpdir string, whitelistedTables []string, whitelistCache *lru.Cache) *mapmutation {
	clean := func() {}
	if quit == nil {
		ch := make(chan struct{})
		clean = func() { close(ch) }
		quit = ch
	}

	whitelistedTablesMap := make(map[string]byte)
	for idx, table := range whitelistedTables {
		whitelistedTablesMap[table] = byte(idx)
	}
	return &mapmutation{
		db:                tx,
		puts:              make(map[string]map[mutationKey][]byte),
		whitelistCache:    whitelistCache,
		quit:              quit,
		clean:             clean,
		tmpdir:            tmpdir,
		whitelistedTables: make(map[string]byte),
	}
}

func makeKey(key []byte) mutationKey {
	var k mutationKey
	copy(k[:], key)
	k[len(k)-1] = byte(len(key))
	return k
}

func (m *mapmutation) makeCacheKey(table string, key []byte) string {
	return string(append(key, m.whitelistedTables[table]))
}

func (m *mapmutation) RwKV() kv.RwDB {
	if casted, ok := m.db.(ethdb.HasRwKV); ok {
		return casted.RwKV()
	}
	return nil
}

func (m *mapmutation) isWhitelisted(table string) bool {
	_, ok := m.whitelistedTables[table]
	return ok
}

func (m *mapmutation) getMem(table string, key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.puts[table]; !ok {
		return nil, false
	}
	if value, ok := m.puts[table][makeKey(key)]; ok {
		return value, ok
	}

	if m.whitelistCache != nil && m.isWhitelisted(table) {
		if value, ok := m.whitelistCache.Get(m.makeCacheKey(table, key)); ok {
			return value.([]byte), ok
		}
	}
	return nil, false
}

func (m *mapmutation) IncrementSequence(bucket string, amount uint64) (res uint64, err error) {
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
func (m *mapmutation) ReadSequence(bucket string) (res uint64, err error) {
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
func (m *mapmutation) GetOne(table string, key []byte) ([]byte, error) {
	if value, ok := m.getMem(table, key); ok {
		if value == nil {
			return nil, nil
		}
		return value, nil
	}
	if m.db != nil {
		// TODO: simplify when tx can no longer be parent of mutation
		value, err := m.db.GetOne(table, key)
		if err != nil {
			return nil, err
		}
		if m.whitelistCache != nil && m.isWhitelisted(table) {
			m.whitelistCache.Add(m.makeCacheKey(table, key), value)
		}

		return value, nil
	}
	return nil, nil
}

// Can only be called from the worker thread
func (m *mapmutation) Get(table string, key []byte) ([]byte, error) {
	value, err := m.GetOne(table, key)
	if err != nil {
		return nil, err
	}

	if value == nil {
		return nil, ethdb.ErrKeyNotFound
	}

	return value, nil
}

func (m *mapmutation) Last(table string) ([]byte, []byte, error) {
	c, err := m.db.Cursor(table)
	if err != nil {
		return nil, nil, err
	}
	defer c.Close()
	return c.Last()
}

func (m *mapmutation) Has(table string, key []byte) (bool, error) {
	if _, ok := m.getMem(table, key); ok {
		return ok, nil
	}
	if m.db != nil {
		return m.db.Has(table, key)
	}
	return false, nil
}

func (m *mapmutation) Put(table string, key []byte, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.puts[table]; !ok {
		m.puts[table] = make(map[mutationKey][]byte)
	}

	formattedKey := makeKey(key)

	var ok bool
	if _, ok = m.puts[table][formattedKey]; !ok {
		m.size += len(value) - len(m.puts[table][formattedKey])
		m.puts[table][formattedKey] = value
		return nil
	}
	m.puts[table][formattedKey] = value
	m.size += len(formattedKey) + len(value)

	m.count++
	return nil
}

func (m *mapmutation) Append(table string, key []byte, value []byte) error {
	return m.Put(table, key, value)
}

func (m *mapmutation) AppendDup(table string, key []byte, value []byte) error {
	return m.Put(table, key, value)
}

func (m *mapmutation) BatchSize() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

func (m *mapmutation) ForEach(bucket string, fromPrefix []byte, walker func(k, v []byte) error) error {
	m.panicOnEmptyDB()
	return m.db.ForEach(bucket, fromPrefix, walker)
}

func (m *mapmutation) ForPrefix(bucket string, prefix []byte, walker func(k, v []byte) error) error {
	m.panicOnEmptyDB()
	return m.db.ForPrefix(bucket, prefix, walker)
}

func (m *mapmutation) ForAmount(bucket string, prefix []byte, amount uint32, walker func(k, v []byte) error) error {
	m.panicOnEmptyDB()
	return m.db.ForAmount(bucket, prefix, amount, walker)
}

func (m *mapmutation) Delete(table string, k, v []byte) error {
	if v != nil {
		return m.db.Delete(table, k, v) // TODO: mutation to support DupSort deletes
	}
	return m.Put(table, k, nil)
}

func (m *mapmutation) doCommit(tx kv.RwTx) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	count := 0
	total := float64(m.count)
	for table, bucket := range m.puts {
		collector := etl.NewCollector("", m.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
		defer collector.Close()
		for key, value := range bucket {
			formattedKey := make([]byte, key[len(key)-1])
			copy(formattedKey, key[:])

			collector.Collect(formattedKey, value)
			// Update cache on commits
			if m.isWhitelisted(table) {
				m.whitelistCache.Add(m.makeCacheKey(table, formattedKey), value)
			}
			count++
			select {
			default:
			case <-logEvery.C:
				progress := fmt.Sprintf("%.1fM/%.1fM", float64(count)/1_000_000, total/1_000_000)
				log.Info("Write to db", "progress", progress, "current table", table)
				tx.CollectMetrics()
			}
		}
		if err := collector.Load(m.db, table, etl.IdentityLoadFunc, etl.TransformArgs{Quit: m.quit}); err != nil {
			return err
		}
	}

	tx.CollectMetrics()
	return nil
}

func (m *mapmutation) Commit() error {
	if m.db == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.doCommit(m.db); err != nil {
		return err
	}

	m.puts = map[string]map[mutationKey][]byte{}
	m.size = 0
	m.count = 0
	m.clean()
	return nil
}

func (m *mapmutation) Rollback() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.puts = map[string]map[mutationKey][]byte{}
	m.size = 0
	m.count = 0
	m.size = 0
	m.clean()
}

func (m *mapmutation) Close() {
	m.Rollback()
}

func (m *mapmutation) Begin(ctx context.Context, flags ethdb.TxFlags) (ethdb.DbWithPendingMutations, error) {
	panic("mutation can't start transaction, because doesn't own it")
}

func (m *mapmutation) panicOnEmptyDB() {
	if m.db == nil {
		panic("Not implemented")
	}
}

func (m *mapmutation) SetRwKV(kv kv.RwDB) {
	m.db.(ethdb.HasRwKV).SetRwKV(kv)
}
