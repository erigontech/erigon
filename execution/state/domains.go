package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
	"math"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	"unique"
	"unsafe"
	"weak"

	"github.com/elastic/go-freelru"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/assert"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/holiman/uint256"
	cmap "github.com/orcaman/concurrent-map/v2"
)

func init() {
	if maxprocs := runtime.GOMAXPROCS(0); maxprocs > cmap.SHARD_COUNT {
		cmap.SHARD_COUNT = maxprocs
	}
}

type iodir int

const (
	Get iodir = iota
	Put
)

type AtomicDuration atomic.Uint64

// Load atomically loads and returns the value stored in x.
func (x *AtomicDuration) Load() time.Duration {
	return time.Duration((*atomic.Uint64)(x).Load())
}

// Store atomically stores val into x.
func (x *AtomicDuration) Store(val time.Duration) { (*atomic.Uint64)(x).Store(uint64(val)) }

// Swap atomically stores new into x and returns the previous value.
func (x *AtomicDuration) Swap(new uint64) (old time.Duration) {
	return time.Duration((*atomic.Uint64)(x).Swap(uint64(new)))
}

// CompareAndSwap executes the compare-and-swap operation for x.
func (x *AtomicDuration) CompareAndSwap(old, new time.Duration) (swapped bool) {
	return (*atomic.Uint64)(x).CompareAndSwap(uint64(old), uint64(new))
}

// Add atomically adds delta to x and returns the new value.
func (x *AtomicDuration) Add(delta time.Duration) (new time.Duration) {
	return time.Duration((*atomic.Uint64)(x).Add(uint64(new)))
}

type DomainIOMetrics struct {
	CacheReadCount       atomic.Uint64
	CacheReadDuration    AtomicDuration
	GetCacheCount        atomic.Uint64
	GetCacheSize         atomic.Uint64
	GetCacheKeySize      atomic.Uint64
	GetCacheValueSize    atomic.Uint64
	GetCacheReadCount    atomic.Uint64
	GetCacheReadDuration AtomicDuration
	PutCacheCount        atomic.Uint64
	PutCacheSize         atomic.Uint64
	PutCacheKeySize      atomic.Uint64
	PutCacheValueSize    atomic.Uint64
	PutCacheReadCount    atomic.Uint64
	PutCacheReadDuration AtomicDuration
	DbReadCount          atomic.Uint64
	DbReadDuration       AtomicDuration
	FileReadCount        atomic.Uint64
	FileReadDuration     AtomicDuration
}

type DomainIOMetricsSnapshot struct {
	CacheReadCount       uint64
	CacheReadDuration    time.Duration
	GetCacheCount        uint64
	GetCacheSize         uint64
	GetCacheKeySize      uint64
	GetCacheValueSize    uint64
	GetCacheReadCount    uint64
	GetCacheReadDuration time.Duration
	PutCacheCount        uint64
	PutCacheSize         uint64
	PutCacheKeySize      uint64
	PutCacheValueSize    uint64
	PutCacheReadCount    uint64
	PutCacheReadDuration time.Duration
	DbReadCount          uint64
	DbReadDuration       time.Duration
	FileReadCount        uint64
	FileReadDuration     time.Duration
}

func (dm *DomainIOMetrics) Snapshot() *DomainIOMetricsSnapshot {
	return &DomainIOMetricsSnapshot{
		CacheReadCount:       dm.CacheReadCount.Load(),
		CacheReadDuration:    dm.CacheReadDuration.Load(),
		GetCacheCount:        dm.GetCacheCount.Load(),
		GetCacheSize:         dm.GetCacheSize.Load(),
		GetCacheKeySize:      dm.GetCacheKeySize.Load(),
		GetCacheValueSize:    dm.GetCacheValueSize.Load(),
		GetCacheReadCount:    dm.GetCacheReadCount.Load(),
		GetCacheReadDuration: dm.GetCacheReadDuration.Load(),
		PutCacheCount:        dm.PutCacheCount.Load(),
		PutCacheSize:         dm.PutCacheSize.Load(),
		PutCacheKeySize:      dm.PutCacheKeySize.Load(),
		PutCacheValueSize:    dm.PutCacheValueSize.Load(),
		PutCacheReadCount:    dm.PutCacheReadCount.Load(),
		PutCacheReadDuration: dm.PutCacheReadDuration.Load(),
		DbReadCount:          dm.DbReadCount.Load(),
		DbReadDuration:       dm.DbReadDuration.Load(),
		FileReadCount:        dm.FileReadCount.Load(),
		FileReadDuration:     dm.FileReadDuration.Load(),
	}
}

type DomainMetrics struct {
	sync.RWMutex
	DomainIOMetrics
	Domains map[kv.Domain]*DomainIOMetrics
}
type DomainMetricsSnapshot struct {
	*DomainIOMetricsSnapshot
	Domains map[kv.Domain]*DomainIOMetricsSnapshot
}

func (dm *DomainMetrics) Snapshot() *DomainMetricsSnapshot {
	dm.RLock()
	defer dm.RUnlock()
	snapshot := DomainMetricsSnapshot{
		DomainIOMetricsSnapshot: dm.DomainIOMetrics.Snapshot(),
		Domains:                 map[kv.Domain]*DomainIOMetricsSnapshot{},
	}
	for domain, metrics := range dm.Domains {
		snapshot.Domains[domain] = metrics.Snapshot()
	}
	return &snapshot
}

func (dm *DomainMetrics) UpdatePutCacheWrites(domain kv.Domain, putKeySize int, putValueSize int) {
	dm.PutCacheCount.Add(1)
	dm.PutCacheSize.Add(uint64(putKeySize + putValueSize))
	dm.PutCacheKeySize.Add(uint64(putKeySize))
	dm.PutCacheValueSize.Add(uint64(putValueSize))
	if m, ok := dm.Domains[domain]; ok {
		m.PutCacheCount.Add(1)
		m.PutCacheSize.Add(uint64(putKeySize + putValueSize))
		m.PutCacheKeySize.Add(uint64(putKeySize))
		m.PutCacheValueSize.Add(uint64(putValueSize))
	} else {
		dm.Lock()
		defer dm.Unlock()
		m = &DomainIOMetrics{}
		m.PutCacheCount.Store(1)
		m.PutCacheSize.Store(uint64(putKeySize + putValueSize))
		m.PutCacheKeySize.Store(uint64(putKeySize))
		m.PutCacheValueSize.Store(uint64(putValueSize))
		dm.Domains[domain] = m
	}
}

func (dm *DomainMetrics) UpdateGetCacheWrites(domain kv.Domain, getKeySize int, getValueSize int) {
	dm.GetCacheCount.Add(1)
	dm.GetCacheSize.Add(uint64(getKeySize + getValueSize))
	dm.GetCacheKeySize.Add(uint64(getKeySize))
	dm.GetCacheValueSize.Add(uint64(getValueSize))
	if m, ok := dm.Domains[domain]; ok {
		m.GetCacheCount.Add(1)
		m.GetCacheSize.Add(uint64(getKeySize + getValueSize))
		m.GetCacheKeySize.Add(uint64(getKeySize))
		m.GetCacheValueSize.Add(uint64(getValueSize))
	} else {
		dm.Lock()
		defer dm.Unlock()
		m := &DomainIOMetrics{}
		m.GetCacheCount.Store(1)
		m.GetCacheSize.Store(uint64(getKeySize + getValueSize))
		m.GetCacheKeySize.Store(uint64(getKeySize))
		m.GetCacheValueSize.Store(uint64(getValueSize))
		dm.Domains[domain] = m
	}
}

func (dm *DomainMetrics) UpdateCacheReads(domain kv.Domain, direction iodir, start time.Time) {
	dm.CacheReadCount.Add(1)
	readDuration := time.Since(start)
	dm.CacheReadDuration.Add(readDuration)
	m, ok := dm.Domains[domain]
	if ok {
		m.CacheReadCount.Add(1)
		m.CacheReadDuration.Add(readDuration)
	} else {
		dm.Lock()
		defer dm.Unlock()
		m = &DomainIOMetrics{}
		m.CacheReadCount.Store(1)
		m.CacheReadDuration.Store(readDuration)
		dm.Domains[domain] = m
	}
	switch direction {
	case Get:
		dm.GetCacheReadCount.Add(1)
		dm.GetCacheReadDuration.Add(readDuration)
		m.GetCacheReadCount.Add(1)
		m.GetCacheReadDuration.Add(readDuration)
	case Put:
		dm.PutCacheReadCount.Add(1)
		dm.PutCacheReadDuration.Add(readDuration)
		m.PutCacheReadCount.Add(1)
		m.PutCacheReadDuration.Add(readDuration)
	}
}

func (dm *DomainMetrics) UpdateDbReads(domain kv.Domain, start time.Time) {
	dm.DbReadCount.Add(1)
	readDuration := time.Since(start)
	dm.DbReadDuration.Add(readDuration)
	if d, ok := dm.Domains[domain]; ok {
		d.DbReadCount.Add(1)
		d.DbReadDuration.Add(readDuration)
	} else {
		dm.Lock()
		defer dm.Unlock()
		m := &DomainIOMetrics{}
		m.DbReadCount.Store(1)
		m.DbReadDuration.Store(readDuration)
		dm.Domains[domain] = m
	}
}

func (dm *DomainMetrics) UpdateFileReads(domain kv.Domain, start time.Time) {
	dm.FileReadCount.Add(1)
	readDuration := time.Since(start)
	dm.FileReadDuration.Add(readDuration)
	if d, ok := dm.Domains[domain]; ok {
		d.FileReadCount.Add(1)
		d.FileReadDuration.Add(readDuration)
	} else {
		dm.Lock()
		defer dm.Unlock()
		m := &DomainIOMetrics{}
		m.FileReadCount.Store(1)
		m.FileReadDuration.Store(readDuration)
		dm.Domains[domain] = m
	}
}

type valueCache[K comparable, V any] interface {
	Name() kv.Domain
	Add(key K, value V, txNum uint64) (evicted bool)
	Remove(key K) (evicted bool)
	Get(key K) (value ValueWithTxNum[V], ok bool)
}

type lruValueCache[K comparable, V any] struct {
	d     kv.Domain
	lru   *freelru.ShardedLRU[K, ValueWithTxNum[V]]
	limit uint32
}

func newLRUValueCache[K comparable, U comparable, V any](d kv.Domain, limit uint32) (*lruValueCache[K, V], error) {
	c, err := freelru.NewSharded[K, ValueWithTxNum[V]](limit, func(k K) uint32 {
		return uint32(uintptr(unsafe.Pointer((*handle[U])(unsafe.Pointer(&k)).value)))
	})

	if err != nil {
		return nil, err
	}
	return &lruValueCache[K, V]{d: d, lru: c, limit: limit}, nil
}

func (c *lruValueCache[K, V]) Name() kv.Domain {
	return c.d
}

func (c *lruValueCache[K, V]) Add(key K, value V, txNum uint64) (evicted bool) {
	return c.lru.Add(key, ValueWithTxNum[V]{value, txNum})
}

func (c *lruValueCache[K, V]) Remove(key K) (evicted bool) {
	return c.lru.Remove(key)
}

func (c *lruValueCache[K, V]) Get(key K) (value ValueWithTxNum[V], ok bool) {
	return c.lru.GetAndRefresh(key, 5*time.Minute)
}

type updates[K comparable, V any] interface {
	Get(key K) ([]ValueWithTxNum[V], bool)
	Put(key K, value ValueWithTxNum[V]) (bool, error)
	Iter() iter.Seq2[K, []ValueWithTxNum[V]]
	Clear() updates[K, V]
}

type valueUpdates[K comparable, U comparable, V any] struct {
	cmap cmap.ConcurrentMap[K, []ValueWithTxNum[V]]
}

type handle[C comparable] struct{ value *C }

func init() {
	if unsafe.Sizeof(handle[common.Address]{}) != unsafe.Sizeof(unique.Handle[common.Address]{}) {
		panic("handle type != unique.Handle - check unique.Handle implementation details for this version of go")
	}
}

func newValueUpdates[K comparable, U comparable, V any]() updates[K, V] {
	return &valueUpdates[K, U, V]{
		cmap: cmap.NewWithCustomShardingFunction[K, []ValueWithTxNum[V]](func(k K) uint32 {
			return uint32(uintptr(unsafe.Pointer((*handle[U])(unsafe.Pointer(&k)).value)))
		}),
	}
}

func (u *valueUpdates[K, U, V]) Get(k K) ([]ValueWithTxNum[V], bool) {
	v, ok := u.cmap.Get(k)
	return v, ok
}

func (u *valueUpdates[K, U, V]) Put(k K, v ValueWithTxNum[V]) (inserted bool, err error) {
	u.cmap.Upsert(k, nil, func(e bool, values, _ []ValueWithTxNum[V]) []ValueWithTxNum[V] {
		if e {
			txNum := values[len(values)-1].TxNum
			switch {
			case v.TxNum < txNum:
				err = fmt.Errorf("can't insert non sequential tx: got: %d, expected > %d", v.TxNum, values[len(values)-1].TxNum)
			case v.TxNum == txNum:
				values[len(values)-1] = v
			default:
				inserted = true
				return append(values, v)
			}
		}
		inserted = true
		return []ValueWithTxNum[V]{v}
	})

	return inserted, err
}

func (u *valueUpdates[K, U, V]) Iter() iter.Seq2[K, []ValueWithTxNum[V]] {
	return func(yield func(K, []ValueWithTxNum[V]) bool) {
		for v := range u.cmap.IterBuffered() {
			yield(v.Key, v.Val)
		}
	}
}

func (u *valueUpdates[K, U, V]) Clear() updates[K, V] {
	*u = *newValueUpdates[K, U, V]().(*valueUpdates[K, U, V])
	return u
}

type keyOps[V any] struct {
	serializer func(v V) []byte
	formatter  func(v V) string
}
type valueOps[V any] struct {
	cmp          func(v0 V, v1 V) bool
	isEmpty      func(v0 V) bool
	serializer   func(v V) []byte
	deserializer func(b []byte) (V, error)
	formatter    func(v V) string
}

type domain[K comparable, V any] struct {
	lock       sync.RWMutex
	mem        kv.TemporalMemBatch
	metrics    *DomainMetrics
	commitCtx  *commitment.CommitmentContext
	valueCache valueCache[K, V]
	updates    updates[K, V]
	keyOps     keyOps[K]
	valueOps   valueOps[V]
}

func (d *domain[K, V]) name() kv.Domain {
	return d.valueCache.Name()
}

func (d *domain[K, V]) get(ctx context.Context, k K, tx kv.TemporalTx) (v V, txNum uint64, ok bool, err error) {
	if tx == nil {
		return v, 0, false, errors.New("domain get: unexpected nil tx")
	}

	tracePrefix, trace := dbg.TraceValues(ctx)

	start := time.Now()
	val, ok := d.updates.Get(k)

	if ok {
		d.metrics.UpdateCacheReads(d.name(), Put, start)
		if trace {
			fmt.Printf("%s%s get(updates) [%s] => [%s] (%d)\n", tracePrefix, d.name(), d.keyOps.formatter(k), d.valueOps.formatter(val[len(val)-1].Value), val[len(val)-1].TxNum)
		}

		if d.valueOps.isEmpty(val[len(val)-1].Value) {
			var v V
			return v, 0, false, nil
		}
		return val[len(val)-1].Value, val[len(val)-1].TxNum, true, nil
	}

	maxStep := kv.Step(math.MaxUint64)

	if d.mem.IsUnwound() {
		av := d.keyOps.serializer(k)
		if val, step, ok := d.mem.GetLatest(d.name(), av[:]); ok {
			if v, err = d.valueOps.deserializer(val); err != nil {
				return v, uint64(step) * tx.StepSize(), false, err
			}
			txNum := step.ToTxNum(tx.StepSize())
			d.metrics.UpdateCacheReads(d.name(), Put, start)
			if trace {
				fmt.Printf("%s%s get(unwind) [%s] => [%s] (%d)\n", tracePrefix, d.name(), d.keyOps.formatter(k), d.valueOps.formatter(v), txNum)
			}
			return v, txNum, true, nil
		} else {
			if step > 0 {
				maxStep = step
			}
		}
	}

	if d.valueCache != nil {
		if val, ok := d.valueCache.Get(k); ok {
			d.metrics.UpdateCacheReads(d.name(), Get, start)
			if trace {
				fmt.Printf("%s%s get(cache) [%s] => [%s] (%d)\n", tracePrefix, d.name(), d.keyOps.formatter(k), d.valueOps.formatter(val.Value), val.TxNum)
			}
			return val.Value, val.TxNum, true, nil
		}
	}

	av := d.keyOps.serializer(k)
	latest, txNum, err := d.getLatest(ctx, d.name(), av[:], tx, maxStep, start)

	if len(latest) == 0 {
		return v, txNum, false, nil
	}

	if v, err = d.valueOps.deserializer(latest); err != nil {
		return v, txNum, false, err
	}

	if d.valueCache != nil {
		d.valueCache.Add(k, v, txNum)
	}

	if trace {
		fmt.Printf("%s%s get(db) [%s] => [%s] (%d)\n", tracePrefix, d.name(), d.keyOps.formatter(k), d.valueOps.formatter(v), txNum)
	}

	return v, txNum, true, nil
}

func (d *domain[K, V]) put(ctx context.Context, k K, v V, roTx kv.TemporalTx, txNum uint64, prev *ValueWithTxNum[V]) error {
	var ok bool
	var prevVal V
	var prevTxNum uint64

	if prev == nil {
		var err error
		prevVal, prevTxNum, ok, err = d.get(ctx, k, roTx)
		if err != nil {
			return err
		}
	} else {
		ok = true
		prevVal = prev.Value
		prevTxNum = prev.TxNum
	}

	if ok && d.valueOps.cmp(v, prevVal) {
		return nil
	}

	if tracePrefix, trace := dbg.TraceValues(ctx); trace {
		fmt.Printf("%s%s put [%s] => [%s] (%d)\n", tracePrefix, d.name(), d.keyOps.formatter(k), d.valueOps.formatter(v), txNum)
	}

	vbuf := d.valueOps.serializer(v)
	kbuf := d.keyOps.serializer(k)
	var pvbuf []byte
	if ok {
		pvbuf = d.valueOps.serializer(prevVal)
	}
	if err := d.mem.DomainPut(d.name(), kbuf, vbuf, txNum, pvbuf, kv.Step(prevTxNum/roTx.StepSize())); err != nil {
		return err
	}

	putKeySize := 0
	putValueSize := 0

	inserted, err := d.updates.Put(k, ValueWithTxNum[V]{v, txNum})

	if err != nil {
		return err
	}

	if inserted {
		putKeySize += len(kbuf)
		putValueSize += len(vbuf)
	} else {
		putValueSize = len(vbuf) - len(pvbuf)
	}

	d.metrics.UpdatePutCacheWrites(d.name(), putKeySize, putValueSize)
	return nil
}

func (d *domain[K, V]) del(ctx context.Context, k K, roTx kv.TemporalTx, txNum uint64, prev *ValueWithTxNum[V]) error {
	var ok bool
	var prevVal V
	var prevTxNum uint64

	if prev == nil {
		var err error
		prevVal, prevTxNum, ok, err = d.get(ctx, k, roTx)
		if err != nil {
			return err
		}
	} else {
		ok = true
		prevVal = prev.Value
		prevTxNum = prev.TxNum
	}

	kbuf := d.keyOps.serializer(k)
	var pvbuf []byte
	if ok {
		pvbuf = d.valueOps.serializer(prevVal)
	}

	if tracePrefix, trace := dbg.TraceValues(ctx); trace {
		fmt.Printf("%s%s del [%s] (%d)\n", tracePrefix, d.name(), d.keyOps.formatter(k), txNum)
	}

	if err := d.mem.DomainDel(d.name(), kbuf, txNum, pvbuf, kv.Step(prevTxNum/roTx.StepSize())); err != nil {
		return err
	}

	putKeySize := 0
	putValueSize := 0

	inserted, err := d.updates.Put(k, ValueWithTxNum[V]{TxNum: txNum})

	if err != nil {
		return err
	}

	if inserted {
		putKeySize += len(kbuf)
	} else {
		putValueSize = -len(pvbuf)
	}

	d.metrics.UpdatePutCacheWrites(d.name(), putKeySize, putValueSize)
	return nil
}

func (d *domain[K, V]) getLatest(ctx context.Context, domain kv.Domain, k []byte, tx kv.TemporalTx, maxStep kv.Step, start time.Time) (v []byte, txNum uint64, err error) {
	type MeteredGetter interface {
		MeteredGetLatest(domain kv.Domain, k []byte, tx kv.Tx, maxStep kv.Step, metrics state.DomainMetrics, start time.Time) (v []byte, step kv.Step, ok bool, err error)
	}

	var step kv.Step
	if aggTx, ok := tx.AggTx().(MeteredGetter); ok {
		v, step, _, err = aggTx.MeteredGetLatest(domain, k, tx, maxStep, d.metrics, start)
	} else {
		v, step, err = tx.GetLatest(domain, k)
	}

	if err != nil {
		return nil, 0, fmt.Errorf("account %s read error: %w", k, err)
	}

	return v, uint64(step) * tx.StepSize(), nil
}

func (d *domain[K, V]) getAsOf(key []byte, ts uint64) (v []byte, ok bool, err error) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	/* TODO
	keyS := toStringZeroCopy(key)
	var dataWithTxNums []dataWithTxNum
	if domain == kv.StorageDomain {
		dataWithTxNums, ok = sd.storage.Get(keyS)
		if !ok {
			return nil, false, nil
		}
		for i, dataWithTxNum := range dataWithTxNums {
			if ts > dataWithTxNum.txNum && (i == len(dataWithTxNums)-1 || ts <= dataWithTxNums[i+1].txNum) {
				return dataWithTxNum.data, true, nil
			}
		}
		return nil, false, nil
	}

	dataWithTxNums, ok = sd.domains[domain][keyS]
	if !ok {
		return nil, false, nil
	}
	for i, dataWithTxNum := range dataWithTxNums {
		if ts > dataWithTxNum.txNum && (i == len(dataWithTxNums)-1 || ts <= dataWithTxNums[i+1].txNum) {
			return dataWithTxNum.data, true, nil
		}
	}
	*/
	return nil, false, nil
}

func (sd *domain[K, V]) ClearMetrics() {
	sd.metrics.RLock()
	defer sd.metrics.RUnlock()
	if dm, ok := sd.metrics.Domains[sd.name()]; ok {
		dm.PutCacheCount.Store(0)
		dm.PutCacheSize.Store(0)
		dm.PutCacheKeySize.Store(0)
		dm.PutCacheValueSize.Store(0)
	}
}

func (d *domain[K, V]) Merge(other *domain[K, V]) {
	d.lock.Lock()
	defer d.lock.Unlock()
	for key, values := range other.updates.Iter() {
		for _, value := range values {
			d.updates.Put(key, value)
		}
	}
}

func (d *domain[K, V]) FlushUpdates() {
	d.lock.Lock()
	defer d.lock.Unlock()
	for k, vs := range d.updates.Iter() {
		if len(vs) > 0 {
			// the value cache only handles latest values for the moment
			v := vs[len(vs)-1]
			if d.valueOps.isEmpty(v.Value) {
				d.valueCache.Remove(k)
			} else {
				d.valueCache.Add(k, v.Value, v.TxNum)
			}
		}
	}
	d.updates = d.updates.Clear()
	d.ClearMetrics()
}

type AccountsDomain struct {
	domain[accounts.Address, *accounts.Account]
	storage *StorageDomain
	code    *CodeDomain
}

func NewAccountsDomain(mem kv.TemporalMemBatch, storage *StorageDomain, code *CodeDomain, commitCtx *commitment.CommitmentContext, metrics *DomainMetrics) (*AccountsDomain, error) {
	cache := mem.ValueCache(kv.AccountsDomain)

	if cache != nil {
		if _, ok := cache.(*lruValueCache[accounts.Address, *accounts.Account]); !ok {
			return nil, fmt.Errorf("unexpected cache initializaton type: got: %T, expected %T", cache, &lruValueCache[accounts.Address, *accounts.Account]{})
		}
	} else {
		var err error
		cache, err = newLRUValueCache[accounts.Address, common.Address, *accounts.Account](kv.AccountsDomain, 50_000)
		if err != nil {
			return nil, err
		}
		mem.SetValueCache(cache)
	}

	return &AccountsDomain{
		domain: domain[accounts.Address, *accounts.Account]{
			metrics:    metrics,
			mem:        mem,
			commitCtx:  commitCtx,
			valueCache: cache.(valueCache[accounts.Address, *accounts.Account]),
			updates:    newValueUpdates[accounts.Address, common.Address, *accounts.Account](),
			keyOps: keyOps[accounts.Address]{
				serializer: func(k accounts.Address) []byte {
					kv := k.Value()
					return kv[:]
				},
				formatter: func(k accounts.Address) string {
					return k.Value().Hex()
				},
			},
			valueOps: valueOps[*accounts.Account]{
				cmp: func(v0 *accounts.Account, v1 *accounts.Account) bool {
					return v0.Equals(v1)
				},
				isEmpty: func(v *accounts.Account) bool {
					return v == nil
				},
				serializer: func(v *accounts.Account) []byte {
					return accounts.SerialiseV3(v)
				},
				deserializer: func(b []byte) (v *accounts.Account, err error) {
					v = &accounts.Account{}
					err = accounts.DeserialiseV3(v, b)
					return v, err
				},
				formatter: func(a *accounts.Account) string {
					if a != nil {
						return fmt.Sprintf("nonce: %d, balance: %d, codeHash: %x", a.Nonce, &a.Balance, a.CodeHash)
					}
					return "empty"
				},
			},
		},
		storage: storage,
		code:    code,
	}, nil
}

func (ad *AccountsDomain) Get(ctx context.Context, k accounts.Address, tx kv.TemporalTx) (v *accounts.Account, txNum uint64, ok bool, err error) {
	return ad.domain.get(ctx, k, tx)
}

func (ad *AccountsDomain) Put(ctx context.Context, k accounts.Address, v *accounts.Account, roTx kv.TemporalTx, txNum uint64, prev ...ValueWithTxNum[*accounts.Account]) error {
	if v == nil {
		return errors.New("accounts domain: trying to put nil value. not allowed")
	}

	ad.commitCtx.TouchAccount(k, v)

	var pv *ValueWithTxNum[*accounts.Account]
	if len(prev) != 0 {
		pv = &prev[0]
	}

	return ad.domain.put(ctx, k, v, roTx, txNum, pv)
}

func (ad *AccountsDomain) Del(ctx context.Context, k accounts.Address, roTx kv.TemporalTx, txNum uint64, prev ...ValueWithTxNum[*accounts.Account]) error {
	ad.commitCtx.TouchAccount(k, nil)

	if err := ad.storage.Del(ctx, k, accounts.NilKey, roTx, txNum); err != nil {
		return err
	}

	if err := ad.code.Del(ctx, k, roTx, txNum); err != nil {
		return err
	}

	var pv *ValueWithTxNum[*accounts.Account]
	if len(prev) != 0 {
		pv = &prev[0]
	}

	return ad.domain.del(ctx, k, roTx, txNum, pv)
}

type storageLocation struct {
	address accounts.Address
	key     accounts.StorageKey
}

type storageCache struct {
	lru   *freelru.ShardedLRU[storageLocation, ValueWithTxNum[uint256.Int]]
	limit uint32
}

func newStorageCache(limit uint32) (*storageCache, error) {
	c, err := freelru.NewSharded[storageLocation, ValueWithTxNum[uint256.Int]](limit, func(k storageLocation) uint32 {
		return uint32(uintptr(unsafe.Pointer((*handle[common.Address])(unsafe.Pointer(&k.address)).value))) ^
			uint32(uintptr(unsafe.Pointer((*handle[common.Hash])(unsafe.Pointer(&k.key)).value)))
	})

	if err != nil {
		return nil, err
	}

	return &storageCache{lru: c, limit: limit}, nil
}

func (c *storageCache) Name() kv.Domain {
	return kv.StorageDomain
}

func (c *storageCache) Add(key storageLocation, value uint256.Int, txNum uint64) (evicted bool) {
	return c.lru.Add(key, ValueWithTxNum[uint256.Int]{value, txNum})
}

func (c storageCache) Remove(key storageLocation) (evicted bool) {
	return c.lru.Remove(key)
}

func (c *storageCache) Get(key storageLocation) (value ValueWithTxNum[uint256.Int], ok bool) {
	return c.lru.GetAndRefresh(key, 5*time.Minute)
}

type storageUpdates struct {
	cmap cmap.ConcurrentMap[accounts.Address, *cmap.ConcurrentMap[accounts.StorageKey, []ValueWithTxNum[uint256.Int]]]
}

func newStorageUpdates() updates[storageLocation, uint256.Int] {
	return &storageUpdates{
		cmap: cmap.NewWithCustomShardingFunction[accounts.Address, *cmap.ConcurrentMap[accounts.StorageKey, []ValueWithTxNum[uint256.Int]]](
			func(k accounts.Address) uint32 {
				return uint32(uintptr(unsafe.Pointer((*handle[common.Address])(unsafe.Pointer(&k)).value)))
			}),
	}
}

func (u storageUpdates) Get(k storageLocation) ([]ValueWithTxNum[uint256.Int], bool) {
	sm, ok := u.cmap.Get(k.address)
	if !ok {
		return nil, false
	}
	return sm.Get(k.key)
}

func (u storageUpdates) Put(k storageLocation, v ValueWithTxNum[uint256.Int]) (inserted bool, err error) {
	u.cmap.Upsert(k.address, nil, func(e bool, valueMap, _ *cmap.ConcurrentMap[accounts.StorageKey, []ValueWithTxNum[uint256.Int]]) *cmap.ConcurrentMap[accounts.StorageKey, []ValueWithTxNum[uint256.Int]] {
		if e {
			valueMap.Upsert(k.key, nil, func(e bool, values, _ []ValueWithTxNum[uint256.Int]) []ValueWithTxNum[uint256.Int] {
				if e {
					txNum := values[len(values)-1].TxNum
					switch {
					case v.TxNum < txNum:
						err = fmt.Errorf("can't insert non sequential tx: got: %d, expected > %d", v.TxNum, values[len(values)-1].TxNum)
						return values
					case v.TxNum == txNum:
						values[len(values)-1] = v
						return values
					default:
						inserted = true
						return append(values, v)
					}
				}
				inserted = true
				return []ValueWithTxNum[uint256.Int]{v}
			})
			return valueMap
		}
		slots := cmap.NewWithCustomShardingFunction[accounts.StorageKey, []ValueWithTxNum[uint256.Int]](
			func(k accounts.StorageKey) uint32 {
				return uint32(uintptr(unsafe.Pointer((*handle[common.Hash])(unsafe.Pointer(&k)).value)))
			})
		slots.Set(k.key, []ValueWithTxNum[uint256.Int]{v})
		inserted = true
		return &slots
	})

	return inserted, err
}

func (u *storageUpdates) Iter() iter.Seq2[storageLocation, []ValueWithTxNum[uint256.Int]] {
	return func(yield func(storageLocation, []ValueWithTxNum[uint256.Int]) bool) {
		for ta := range u.cmap.IterBuffered() {
			for ts := range ta.Val.IterBuffered() {
				yield(storageLocation{address: ta.Key, key: ts.Key}, ts.Val)
			}
		}
	}
}

func (u *storageUpdates) UpdatedSlots(addr accounts.Address) (*cmap.ConcurrentMap[accounts.StorageKey, []ValueWithTxNum[uint256.Int]], bool) {
	return u.cmap.Get(addr)
}

func (u storageUpdates) SortedIter(addr accounts.Address) iter.Seq2[storageLocation, []ValueWithTxNum[uint256.Int]] {
	if addr.IsNil() {
		iter := slices.SortedFunc(slices.Values(u.cmap.Keys()), func(a, b accounts.Address) int { return a.Value().Cmp(b.Value()) })
		return func(yield func(storageLocation, []ValueWithTxNum[uint256.Int]) bool) {
			for _, a := range iter {
				if slots, ok := u.cmap.Get(a); ok {
					for _, k := range slices.SortedFunc(slices.Values(slots.Keys()), func(a, b accounts.StorageKey) int { return a.Value().Cmp(b.Value()) }) {
						if slot, ok := slots.Get(k); ok {
							yield(storageLocation{address: a, key: k}, slot)
						}
					}
				}
			}
		}
	}

	return func(yield func(storageLocation, []ValueWithTxNum[uint256.Int]) bool) {
		if slots, ok := u.cmap.Get(addr); ok {
			for _, k := range slices.SortedFunc(slices.Values(slots.Keys()), func(a, b accounts.StorageKey) int { return a.Value().Cmp(b.Value()) }) {
				if slot, ok := slots.Get(k); ok {
					yield(storageLocation{address: addr, key: k}, slot)
				}
			}
		}
	}
}

func (u *storageUpdates) Clear() updates[storageLocation, uint256.Int] {
	*u = *newStorageUpdates().(*storageUpdates)
	return u
}

type StorageDomain struct {
	domain[storageLocation, uint256.Int]
}

func NewStorageDomain(mem kv.TemporalMemBatch, commitCtx *commitment.CommitmentContext, metrics *DomainMetrics) (*StorageDomain, error) {
	cache := mem.ValueCache(kv.StorageDomain)

	if cache != nil {
		if _, ok := cache.(*storageCache); !ok {
			return nil, fmt.Errorf("unexpected cache initializaton type: got: %T, expected %T", cache, &storageCache{})
		}
	} else {
		var err error
		cache, err = newStorageCache(100_000)
		if err != nil {
			return nil, err
		}
		mem.SetValueCache(cache)
	}

	return &StorageDomain{
		domain: domain[storageLocation, uint256.Int]{
			metrics:    metrics,
			mem:        mem,
			commitCtx:  commitCtx,
			valueCache: cache.(*storageCache),
			updates:    newStorageUpdates(),
			keyOps: keyOps[storageLocation]{
				serializer: func(k storageLocation) []byte {
					av := k.address.Value()
					kv := k.key.Value()
					return append(av[:], kv[:]...)
				},
				formatter: func(k storageLocation) string {
					return fmt.Sprintf("%x %x", k.address.Value(), k.key.Value())
				},
			},
			valueOps: valueOps[uint256.Int]{
				cmp: func(v0 uint256.Int, v1 uint256.Int) bool {
					return v0 == v1
				},
				isEmpty: func(v uint256.Int) bool {
					return v.IsZero()
				},
				serializer: func(v uint256.Int) []byte {
					return v.Bytes()
				},
				deserializer: func(b []byte) (v uint256.Int, err error) {
					v.SetBytes(b)
					return v, nil
				},
				formatter: func(a uint256.Int) string {
					if !a.IsZero() {
						return fmt.Sprintf("%x", &a)
					}
					return "empty"
				},
			},
		},
	}, nil
}

func (sd *StorageDomain) Get(ctx context.Context, addr accounts.Address, key accounts.StorageKey, tx kv.TemporalTx) (v uint256.Int, txNum uint64, ok bool, err error) {
	return sd.domain.get(ctx, storageLocation{address: addr, key: key}, tx)
}

func (sd *StorageDomain) Put(ctx context.Context, addr accounts.Address, key accounts.StorageKey, v uint256.Int, roTx kv.TemporalTx, txNum uint64, prev ...ValueWithTxNum[uint256.Int]) error {
	if sd.valueOps.isEmpty(v) {
		return errors.New("storage domain: trying to put zero value. not allowed")
	}

	sd.commitCtx.TouchStorage(addr, key, v)

	var pv *ValueWithTxNum[uint256.Int]
	if len(prev) != 0 {
		pv = &prev[0]
	}

	return sd.domain.put(ctx, storageLocation{addr, key}, v, roTx, txNum, pv)
}

func (sd *StorageDomain) Del(ctx context.Context, addr accounts.Address, key accounts.StorageKey, roTx kv.TemporalTx, txNum uint64, prev ...ValueWithTxNum[uint256.Int]) error {
	if !key.IsNil() && addr.IsNil() {
		return errors.New("address unexpectedly nil")
	}

	if addr.IsNil() || key.IsNil() {
		return sd.DelAll(ctx, addr, roTx, txNum)
	}

	sd.commitCtx.TouchStorage(addr, key, uint256.Int{})

	var pv *ValueWithTxNum[uint256.Int]

	if len(prev) != 0 {
		pv = &prev[0]
	}

	return sd.domain.del(ctx, storageLocation{addr, key}, roTx, txNum, pv)
}

func (sd *StorageDomain) slotIterator(addr accounts.Address) func(yield func(string, []kv.DataWithTxNum) bool) {
	sd.lock.RLock()
	iter := sd.updates.(*storageUpdates).SortedIter(addr)
	sd.lock.RUnlock()
	return func(yield func(string, []kv.DataWithTxNum) bool) {
		for k, vs := range iter {
			aval := k.address.Value()
			kval := k.key.Value()
			dataWithTxNum := make([]kv.DataWithTxNum, len(vs))
			for i, v := range vs {
				dataWithTxNum[i] = kv.DataWithTxNum{Data: v.Value.Bytes(), TxNum: v.TxNum}
			}
			yield(string(append(aval[:], kval[:]...)), dataWithTxNum)
		}
	}
}

func (sd *StorageDomain) DelAll(ctx context.Context, addr accounts.Address, roTx kv.TemporalTx, txNum uint64) error {
	type tuple struct {
		k, v []byte
		step kv.Step
	}
	tombs := make([]tuple, 0, 8)

	var prefix []byte
	if !addr.IsNil() {
		value := addr.Value()
		prefix = value[:]
	}

	if err := sd.mem.IteratePrefix(kv.StorageDomain, prefix, sd.slotIterator(addr), roTx, func(k, v []byte, step kv.Step) (bool, error) {
		tombs = append(tombs, tuple{k, v, step})
		return true, nil
	}); err != nil {
		return err
	}
	for _, tomb := range tombs {
		var tv uint256.Int
		tv.SetBytes(tomb.v)
		if err := sd.Del(ctx,
			accounts.BytesToAddress(tomb.k[:length.Addr]), accounts.BytesToKey(tomb.k[length.Addr:]),
			roTx, txNum, ValueWithTxNum[uint256.Int]{tv, uint64(tomb.step) * roTx.StepSize()}); err != nil {
			return err
		}
	}

	if assert.Enable {
		forgotten := 0
		if err := sd.mem.IteratePrefix(kv.StorageDomain, prefix, sd.slotIterator(addr), roTx, func(k, v []byte, step kv.Step) (bool, error) {
			forgotten++
			return true, nil
		}); err != nil {
			return err
		}
		if forgotten > 0 {
			panic(fmt.Errorf("DomainDelPrefix: %d forgotten keys after '%x' prefix removal", forgotten, prefix))
		}
	}
	return nil
}

func (sd *StorageDomain) HasStorage(ctx context.Context, addr accounts.Address, roTx kv.Tx) (bool, error) {
	slots, ok := sd.updates.(*storageUpdates).UpdatedSlots(addr)
	if ok {
		for kv := range slots.IterBuffered() {
			if kv.Val[len(kv.Val)-1].Value.ByteLen() > 0 {
				return true, nil
			}
		}
	}

	var hasPrefix bool
	addrVal := addr.Value()
	err := sd.mem.IteratePrefix(kv.StorageDomain, addrVal[:], nil, roTx, func(k []byte, v []byte, step kv.Step) (bool, error) {
		if sd.mem.IsUnwound() {
			// if we have been unwound we need to do this to ensure the value
			// has not been removed during the unwind
			if val, _, ok := sd.mem.GetLatest(kv.StorageDomain, k); ok {
				v = val
			}
		}
		hasPrefix = len(v) > 0
		if hasPrefix {
			if updateVals, ok := sd.updates.Get(storageLocation{addr, accounts.BytesToKey(k[length.Addr:])}); ok {
				if updateVals[len(updateVals)-1].Value.ByteLen() == 0 {
					hasPrefix = false
				}
			}
			return !hasPrefix, nil
		}
		return true, nil
	})
	return hasPrefix, err
}

func (sd *StorageDomain) IterateStorage(ctx context.Context, addr accounts.Address, it func(k accounts.StorageKey, v uint256.Int, step kv.Step) (cont bool, err error), roTx kv.Tx) error {
	var addrVal []byte
	if !addr.IsNil() {
		value := addr.Value()
		addrVal = value[:]
	}
	return sd.mem.IteratePrefix(kv.StorageDomain, addrVal, sd.slotIterator(addr), roTx, func(k []byte, v []byte, step kv.Step) (cont bool, err error) {
		var i uint256.Int
		i.SetBytes(v)
		return it(accounts.BytesToKey(k), i, step)
	})
}

type codeWithHash struct {
	hash accounts.CodeHash
	code []byte
}

type weakCodeWithHash struct {
	hash accounts.CodeHash
	code weak.Pointer[[]byte]
}

type CodeWithTxNum struct {
	Code  []byte
	Hash  accounts.CodeHash
	TxNum uint64
}

type codeUpdates struct {
	hashes cmap.ConcurrentMap[accounts.Address, accounts.CodeHash]
	code   cmap.ConcurrentMap[accounts.CodeHash, []ValueWithTxNum[codeWithHash]]
}

func newCodeUpdates() *codeUpdates {
	return &codeUpdates{
		hashes: cmap.NewWithCustomShardingFunction[accounts.Address, accounts.CodeHash](
			func(k accounts.Address) uint32 {
				return uint32(uintptr(unsafe.Pointer((*handle[common.Address])(unsafe.Pointer(&k)).value)))
			}),
		code: cmap.NewWithCustomShardingFunction[accounts.CodeHash, []ValueWithTxNum[codeWithHash]](
			func(k accounts.CodeHash) uint32 {
				return uint32(uintptr(unsafe.Pointer((*handle[common.Hash])(unsafe.Pointer(&k)).value)))
			}),
	}
}

func (u *codeUpdates) Get(k accounts.Address) ([]ValueWithTxNum[codeWithHash], bool) {
	h, ok := u.hashes.Get(k)

	if !ok {
		return nil, false
	}

	if h.IsEmpty() {
		return []ValueWithTxNum[codeWithHash]{{Value: codeWithHash{hash: h}}}, true
	}

	c, ok := u.code.Get(h)

	return c, ok
}

func (u *codeUpdates) Put(k accounts.Address, v ValueWithTxNum[codeWithHash]) (inserted bool, err error) {
	u.hashes.Upsert(k, v.Value.hash, func(e bool, valueInMap, newValue accounts.CodeHash) accounts.CodeHash {
		if e {
			if valueInMap != newValue {
				inserted = true
			}
		} else {
			inserted = true
		}
		return newValue
	})

	if inserted && !v.Value.hash.IsEmpty() {
		u.code.Upsert(v.Value.hash, nil, func(e bool, valueInMap, newValue []ValueWithTxNum[codeWithHash]) []ValueWithTxNum[codeWithHash] {
			if e {
				return valueInMap
			}
			return []ValueWithTxNum[codeWithHash]{v}
		})
	}

	return inserted, nil
}

func (u *codeUpdates) Iter() iter.Seq2[accounts.Address, []ValueWithTxNum[codeWithHash]] {
	return func(yield func(accounts.Address, []ValueWithTxNum[codeWithHash]) bool) {
		for kv := range u.hashes.IterBuffered() {
			if code, ok := u.code.Get(kv.Val); ok {
				yield(kv.Key, code)
			}
		}
	}
}

func (u *codeUpdates) Clear() updates[accounts.Address, codeWithHash] {
	*u = *newCodeUpdates()
	return u
}

type codeCache struct {
	hashes *freelru.ShardedLRU[accounts.Address, accounts.CodeHash]
	code   *freelru.ShardedLRU[accounts.CodeHash, ValueWithTxNum[weakCodeWithHash]]
	limit  uint32
}

func newCodeCache(limit uint32) (*codeCache, error) {
	hashes, err := freelru.NewSharded[accounts.Address, accounts.CodeHash](limit, func(k accounts.Address) uint32 {
		return uint32(uintptr(unsafe.Pointer((*handle[common.Address])(unsafe.Pointer(&k)).value)))
	})

	if err != nil {
		return nil, err
	}

	code, err := freelru.NewSharded[accounts.CodeHash, ValueWithTxNum[weakCodeWithHash]](limit, func(k accounts.CodeHash) uint32 {
		return uint32(uintptr(unsafe.Pointer((*handle[common.Hash])(unsafe.Pointer(&k)).value)))
	})

	if err != nil {
		return nil, err
	}

	return &codeCache{hashes: hashes, code: code, limit: limit}, nil
}

func (c *codeCache) Name() kv.Domain {
	return kv.CodeDomain
}

func (c *codeCache) Get(key accounts.Address) (value ValueWithTxNum[codeWithHash], ok bool) {
	h, ok := c.hashes.Get(key)

	if !ok {
		return ValueWithTxNum[codeWithHash]{}, false
	}

	if h.IsEmpty() {
		return ValueWithTxNum[codeWithHash]{Value: codeWithHash{hash: h}}, true
	}

	pc, ok := c.code.Get(h)

	if !ok || pc.Value.code.Value() == nil {
		c.code.Remove(h)
		return ValueWithTxNum[codeWithHash]{}, false
	}

	return ValueWithTxNum[codeWithHash]{Value: codeWithHash{code: *pc.Value.code.Value(), hash: h}}, true
}

func (c *codeCache) Add(k accounts.Address, v codeWithHash, txNum uint64) (evicted bool) {
	h, ok := c.hashes.Get(k)

	if !ok || h != v.hash {
		evicted = c.hashes.Add(k, v.hash)
	}

	if !v.hash.IsEmpty() {
		if pv, ok := c.code.Get(h); !ok || pv.Value.code.Value() == nil {
			return c.code.Add(v.hash, ValueWithTxNum[weakCodeWithHash]{Value: weakCodeWithHash{code: weak.Make(&v.code), hash: v.hash}, TxNum: txNum})
		}
	}

	return evicted
}

func (c *codeCache) Remove(k accounts.Address) (evicted bool) {
	// note we're making the hash inaccessable, the code will get evicted
	// if its not accessed or its gc'd
	return c.hashes.Remove(k)
}

type CodeDomain struct {
	domain[accounts.Address, codeWithHash]
}

func NewCodeDomain(mem kv.TemporalMemBatch, commitCtx *commitment.CommitmentContext, metrics *DomainMetrics) (*CodeDomain, error) {
	cache := mem.ValueCache(kv.CodeDomain)

	if cache != nil {
		if _, ok := cache.(*codeCache); !ok {
			return nil, fmt.Errorf("unexpected cache initializaton type: got: %T, expected %T", cache, &codeCache{})
		}
	} else {
		var err error
		cache, err = newCodeCache(10_000)
		if err != nil {
			return nil, err
		}
		mem.SetValueCache(cache)
	}

	return &CodeDomain{
		domain: domain[accounts.Address, codeWithHash]{
			metrics:    metrics,
			mem:        mem,
			commitCtx:  commitCtx,
			valueCache: cache.(valueCache[accounts.Address, codeWithHash]),
			updates:    newCodeUpdates(),
			keyOps: keyOps[accounts.Address]{
				serializer: func(k accounts.Address) []byte {
					av := k.Value()
					return av[:]
				},
				formatter: func(k accounts.Address) string {
					return fmt.Sprintf("%x", k.Value())
				},
			},
			valueOps: valueOps[codeWithHash]{
				cmp: func(v0 codeWithHash, v1 codeWithHash) bool {
					return bytes.Equal(v0.code, v1.code)
				},
				isEmpty: func(v codeWithHash) bool {
					return len(v.code) == 0
				},
				serializer: func(v codeWithHash) []byte {
					return v.code
				},
				deserializer: func(b []byte) (v codeWithHash, err error) {
					code := common.Copy(b)
					return codeWithHash{code: code, hash: accounts.InternCodeHash(crypto.Keccak256Hash(code))}, nil
				},
				formatter: func(c codeWithHash) string {
					if c.code != nil {
						l, c := printCode(c.code)
						return fmt.Sprintf("%d:%s)", l, c)
					}
					return "empty"
				},
			},
		},
	}, nil
}

func (cd *CodeDomain) Get(ctx context.Context, k accounts.Address, tx kv.TemporalTx) (h accounts.CodeHash, c []byte, txNum uint64, ok bool, err error) {
	v, step, ok, err := cd.domain.get(ctx, k, tx)

	if !ok {
		return accounts.EmptyCodeHash, nil, step, ok, err
	}

	if v.hash.IsEmpty() {
		return v.hash, nil, step, ok, err
	}

	return v.hash, v.code, step, ok, err
}

func (cd *CodeDomain) Put(ctx context.Context, k accounts.Address, h accounts.CodeHash, c []byte, roTx kv.TemporalTx, txNum uint64, prev ...CodeWithTxNum) error {
	if c == nil {
		return fmt.Errorf("domain: %s, trying to put nil value. not allowed", kv.CodeDomain)
	}

	cd.commitCtx.TouchCode(k, c)

	var pv *ValueWithTxNum[codeWithHash]
	if len(prev) != 0 {
		pv = &ValueWithTxNum[codeWithHash]{
			Value: codeWithHash{code: prev[0].Code, hash: prev[0].Hash},
			TxNum: prev[0].TxNum,
		}
	}

	if pv != nil && bytes.Equal(c, pv.Value.code) {
		return nil
	}

	if h.IsNil() {
		h = accounts.InternCodeHash(crypto.Keccak256Hash(c))
	}

	return cd.domain.put(ctx, k, codeWithHash{code: c, hash: h}, roTx, txNum, pv)
}

func (cd *CodeDomain) Del(ctx context.Context, k accounts.Address, roTx kv.TemporalTx, txNum uint64, prev ...CodeWithTxNum) error {
	cd.commitCtx.TouchCode(k, nil)

	var pv *ValueWithTxNum[codeWithHash]
	if len(prev) != 0 {
		pv = &ValueWithTxNum[codeWithHash]{
			Value: codeWithHash{code: prev[0].Code, hash: prev[0].Hash},
			TxNum: prev[0].TxNum,
		}
	}

	return cd.domain.del(ctx, k, roTx, txNum, pv)
}

type CommitmentDomain struct {
	domain[commitment.Path, commitment.BranchData]
}

type branchCache struct {
	state       ValueWithTxNum[commitment.BranchData]
	t0          ValueWithTxNum[commitment.BranchData]
	t1          [16]ValueWithTxNum[commitment.BranchData]
	t2          [256]ValueWithTxNum[commitment.BranchData]
	t3          [4096]ValueWithTxNum[commitment.BranchData]
	t4          [65536]ValueWithTxNum[commitment.BranchData]
	branches    *lruValueCache[commitment.Path, commitment.BranchData]
	branchLimit uint32
}

func newBranchCache(branchLimit uint32) (*branchCache, error) {
	c, err := newLRUValueCache[commitment.Path, string, commitment.BranchData](kv.CommitmentDomain, branchLimit)

	if err != nil {
		return nil, err
	}

	return &branchCache{
		branches:    c,
		branchLimit: branchLimit,
	}, nil
}

func (c *branchCache) Name() kv.Domain {
	return kv.CommitmentDomain
}

var statePath = commitment.InternPath([]byte("state"))

func (c *branchCache) Get(key commitment.Path) (ValueWithTxNum[commitment.BranchData], bool) {
	// see: HexNibblesToCompactBytes for encoding spec
	switch {
	case key == statePath:
		if len(c.state.Value) == 0 {
			return ValueWithTxNum[commitment.BranchData]{}, false
		}
		return c.state, true
	case len(key.Value()) < 4:
		keyValue := key.Value()
		switch len(keyValue) {
		case 0:
			return ValueWithTxNum[commitment.BranchData]{}, false
		case 1:
			if keyValue[0]&0x10 == 0 {
				if len(c.t0.Value) > 0 {
					return c.t0, true
				}
				return ValueWithTxNum[commitment.BranchData]{}, false
			} else {
				value := c.t1[keyValue[0]&0x0f]
				return value, len(value.Value) > 0
			}
		case 2:
			if keyValue[0]&0x10 == 0 {
				value := c.t2[keyValue[1]]
				return value, len(value.Value) > 0
			} else {
				value := c.t3[uint16(keyValue[0]&0x0f)<<8|uint16(keyValue[1])]
				return value, len(value.Value) > 0
			}
		default:
			if keyValue[0]&0x10 == 0 {
				value := c.t4[uint16(keyValue[1])<<8|uint16(keyValue[2])]
				return value, len(value.Value) > 0
			}
		}
	}

	return c.branches.Get(key)
}

func (c *branchCache) Add(k commitment.Path, v commitment.BranchData, txNum uint64) (evicted bool) {
	// see: HexNibblesToCompactBytes for encoding spec
	switch {
	case k == statePath:
		c.state = ValueWithTxNum[commitment.BranchData]{Value: v, TxNum: txNum}
		return false
	case len(k.Value()) < 4:
		keyValue := k.Value()
		switch len(keyValue) {
		case 0:
			return false
		case 1:
			if keyValue[0]&0x10 == 0 {
				c.t0 = ValueWithTxNum[commitment.BranchData]{Value: v, TxNum: txNum}
				return false
			} else {
				c.t1[keyValue[0]&0x0f] = ValueWithTxNum[commitment.BranchData]{Value: v, TxNum: txNum}
				return false
			}
		case 2:
			if keyValue[0]&0x10 == 0 {
				c.t2[keyValue[1]] = ValueWithTxNum[commitment.BranchData]{Value: v, TxNum: txNum}
			} else {
				c.t3[uint16(keyValue[0]&0x0f)<<8|uint16(keyValue[1])] = ValueWithTxNum[commitment.BranchData]{Value: v, TxNum: txNum}
			}
			return false
		default:
			if keyValue[0]&0x10 == 0 {
				c.t4[uint16(keyValue[1])<<8|uint16(keyValue[2])] = ValueWithTxNum[commitment.BranchData]{Value: v, TxNum: txNum}
				return false
			}
		}

	}

	return c.branches.Add(k, v, txNum)
}

func (c *branchCache) Remove(k commitment.Path) (evicted bool) {
	// see: HexNibblesToCompactBytes for encoding spec
	switch {
	case k == statePath:
		evicted = len(c.state.Value) > 0
		c.state = ValueWithTxNum[commitment.BranchData]{}
		return evicted
	case len(k.Value()) < 4:
		keyValue := k.Value()
		switch len(keyValue) {
		case 0:
			return false
		case 1:
			if keyValue[0]&0x10 == 0 {
				evicted = len(c.t0.Value) > 0
				c.t0 = ValueWithTxNum[commitment.BranchData]{}
				return evicted
			} else {
				evicted := len(c.t1[keyValue[0]&0x0f].Value) > 0
				c.t1[keyValue[0]&0x0f] = ValueWithTxNum[commitment.BranchData]{}
				return evicted
			}
		case 2:
			if keyValue[0]&0x10 == 0 {
				kv := keyValue[1]
				evicted := len(c.t2[kv].Value) > 0
				c.t2[kv] = ValueWithTxNum[commitment.BranchData]{}
				return evicted
			} else {
				kv := uint16(keyValue[0]&0x0f)<<8 | uint16(keyValue[1])
				evicted := len(c.t3[kv].Value) > 0
				c.t3[kv] = ValueWithTxNum[commitment.BranchData]{}
				return evicted
			}
		default:
			if keyValue[0]&0x10 == 0 {
				kv := uint16(keyValue[1])<<8 | uint16(keyValue[2])
				evicted := len(c.t4[kv].Value) > 0
				c.t4[kv] = ValueWithTxNum[commitment.BranchData]{}
				return evicted
			}
		}
	}
	return c.branches.Remove(k)
}

func NewCommitmentDomain(mem kv.TemporalMemBatch, commitCtx *commitment.CommitmentContext, metrics *DomainMetrics) (*CommitmentDomain, error) {
	cache := mem.ValueCache(kv.CommitmentDomain)

	if cache != nil {
		if _, ok := cache.(*branchCache); !ok {
			return nil, fmt.Errorf("unexpected cache initializaton type: got: %T, expected %T", cache, &branchCache{})
		}
	} else {
		var err error
		cache, err = newBranchCache(50_000)
		if err != nil {
			return nil, err
		}
		mem.SetValueCache(cache)
	}

	return &CommitmentDomain{
		domain: domain[commitment.Path, commitment.BranchData]{
			metrics:    metrics,
			mem:        mem,
			commitCtx:  commitCtx,
			valueCache: cache.(valueCache[commitment.Path, commitment.BranchData]),
			updates:    newValueUpdates[commitment.Path, string, commitment.BranchData](),
			keyOps: keyOps[commitment.Path]{
				serializer: func(k commitment.Path) []byte {
					return k.Value()
				},
				formatter: func(k commitment.Path) string {
					return fmt.Sprintf("%x", k.Value())
				},
			},
			valueOps: valueOps[commitment.BranchData]{
				cmp: func(v0 commitment.BranchData, v1 commitment.BranchData) bool {
					return bytes.Equal(v0, v1)
				},
				isEmpty: func(v commitment.BranchData) bool {
					return len(v) == 0
				},
				serializer: func(v commitment.BranchData) []byte {
					return v
				},
				deserializer: func(b []byte) (v commitment.BranchData, err error) {
					return commitment.BranchData(common.Copy(b)), nil
				},
				formatter: func(c commitment.BranchData) string {
					if len(c) > 0 {
						l, s := printCode(c)
						return fmt.Sprintf("%p:%d:%s)", &c[0], l, s)
					}
					return "empty"
				},
			},
		},
	}, nil
}

func (cd *CommitmentDomain) GetBranch(ctx context.Context, k commitment.Path, tx kv.TemporalTx) (v commitment.BranchData, txNum uint64, ok bool, err error) {
	return cd.domain.get(dbg.WithTrace(ctx, false), k, tx)
}

func (cd *CommitmentDomain) PutBranch(ctx context.Context, k commitment.Path, v commitment.BranchData, roTx kv.TemporalTx, txNum uint64, prev ...ValueWithTxNum[commitment.BranchData]) error {
	if v == nil {
		return fmt.Errorf("PutBranch: %s, trying to put nil value. not allowed", kv.CommitmentDomain)
	}

	var pv *ValueWithTxNum[commitment.BranchData]
	if len(prev) != 0 {
		pv = &prev[0]
	}

	return cd.domain.put(dbg.WithTrace(ctx, false), k, v, roTx, txNum, pv)
}

func (cd *CommitmentDomain) DelBranch(ctx context.Context, k commitment.Path, roTx kv.TemporalTx, txNum uint64, prev ...ValueWithTxNum[commitment.BranchData]) error {
	var pv *ValueWithTxNum[commitment.BranchData]
	if len(prev) != 0 {
		pv = &prev[0]
	}

	return cd.domain.del(dbg.WithTrace(ctx, false), k, roTx, txNum, pv)
}

type BlockDomain struct {
	mem *membatchwithdb.MemoryMutation
}

func NewBlockDomain(tx kv.TemporalTx, tmpDir string, logger log.Logger) *BlockDomain {
	return &BlockDomain{mem: membatchwithdb.NewMemoryBatch(tx, tmpDir, logger)}
}

func (d *BlockDomain) Flush(ctx context.Context, tx kv.RwTx) error {
	if d.mem != nil {
		return d.mem.Flush(ctx, tx)
	}
	return nil
}

func (d *BlockDomain) Close() {
	mem := d.mem
	if mem != nil {
		d.mem = nil
		mem.Close()
	}
}

func (d *BlockDomain) RwTx() kv.TemporalRwTx {
	return d.mem
}
