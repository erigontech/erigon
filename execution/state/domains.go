package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"
	"math"
	"slices"
	"sync"
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
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/holiman/uint256"
)

type DomainIOMetrics struct {
	CacheReadCount    int64
	CacheReadDuration time.Duration
	CacheGetCount     int64
	CachePutCount     int64
	CacheGetSize      int
	CacheGetKeySize   int
	CacheGetValueSize int
	CachePutSize      int
	CachePutKeySize   int
	CachePutValueSize int
	DbReadCount       int64
	DbReadDuration    time.Duration
	FileReadCount     int64
	FileReadDuration  time.Duration
}

type DomainMetrics struct {
	sync.RWMutex
	DomainIOMetrics
	Domains map[kv.Domain]*DomainIOMetrics
}

func (dm *DomainMetrics) UpdatePutCacheWrites(domain kv.Domain, putKeySize int, putValueSize int) {
	dm.Lock()
	defer dm.Unlock()
	dm.CachePutCount++
	dm.CachePutSize += putKeySize + putValueSize
	dm.CachePutKeySize += putKeySize
	dm.CachePutValueSize += putValueSize
	if m, ok := dm.Domains[domain]; ok {
		m.CachePutCount++
		m.CachePutSize += putKeySize + putValueSize
		m.CachePutKeySize += putKeySize
		m.CachePutValueSize += putValueSize
	} else {
		dm.Domains[domain] = &DomainIOMetrics{
			CachePutCount:     1,
			CachePutSize:      putKeySize + putValueSize,
			CachePutKeySize:   putKeySize,
			CachePutValueSize: putValueSize,
		}
	}
}

func (dm *DomainMetrics) UpdateCacheReads(domain kv.Domain, start time.Time) {
	dm.Lock()
	defer dm.Unlock()
	dm.CacheReadCount++
	readDuration := time.Since(start)
	dm.CacheReadDuration += readDuration
	if d, ok := dm.Domains[domain]; ok {
		d.CacheReadCount++
		d.CacheReadDuration += readDuration
	} else {
		dm.Domains[domain] = &DomainIOMetrics{
			CacheReadCount:    1,
			CacheReadDuration: readDuration,
		}
	}
}

func (dm *DomainMetrics) UpdateDbReads(domain kv.Domain, start time.Time) {
	dm.Lock()
	defer dm.Unlock()
	dm.DbReadCount++
	readDuration := time.Since(start)
	dm.DbReadDuration += readDuration
	if d, ok := dm.Domains[domain]; ok {
		d.DbReadCount++
		d.DbReadDuration += readDuration
	} else {
		dm.Domains[domain] = &DomainIOMetrics{
			DbReadCount:    1,
			DbReadDuration: readDuration,
		}
	}
}

func (dm *DomainMetrics) UpdateFileReads(domain kv.Domain, start time.Time) {
	dm.Lock()
	defer dm.Unlock()
	dm.FileReadCount++
	readDuration := time.Since(start)
	dm.FileReadDuration += readDuration
	if d, ok := dm.Domains[domain]; ok {
		d.FileReadCount++
		d.FileReadDuration += readDuration
	} else {
		dm.Domains[domain] = &DomainIOMetrics{
			FileReadCount:    1,
			FileReadDuration: readDuration,
		}
	}
}

type valueCache[K comparable, V any] interface {
	Name() kv.Domain
	Add(key K, value V, txNum uint64) (evicted bool)
	Remove(key K) (evicted bool)
	Get(key K) (value ValueWithTxNum[V], ok bool)
}

type lruValueCache[K comparable, U comparable, V any] struct {
	d     kv.Domain
	lru   *freelru.ShardedLRU[K, ValueWithTxNum[V]]
	limit uint32
}

func newLRUValueCache[K comparable, U comparable, V any](d kv.Domain, limit uint32) (*lruValueCache[K, U, V], error) {
	type handle[U comparable] struct{ value *U }

	if unsafe.Sizeof(handle[U]{}) != unsafe.Sizeof(unique.Handle[U]{}) {
		panic("handle type != unique.Handle - check unique.Handle implementation details for this version of go")
	}

	c, err := freelru.NewSharded[K, ValueWithTxNum[V]](limit, func(k K) uint32 {
		return uint32(uintptr(unsafe.Pointer((*handle[U])(unsafe.Pointer(&k)).value)))
	})

	if err != nil {
		return nil, err
	}
	return &lruValueCache[K, U, V]{d: d, lru: c, limit: limit}, nil
}

func (c *lruValueCache[K, U, V]) Name() kv.Domain {
	return c.d
}

func (c *lruValueCache[K, U, V]) Add(key K, value V, txNum uint64) (evicted bool) {
	return c.lru.Add(key, ValueWithTxNum[V]{value, txNum})
}

func (c *lruValueCache[K, U, V]) Remove(key K) (evicted bool) {
	return c.lru.Remove(key)
}

func (c *lruValueCache[K, U, V]) Get(key K) (value ValueWithTxNum[V], ok bool) {
	return c.lru.GetAndRefresh(key, 5*time.Minute)
}

type updates[K comparable, V any] interface {
	Get(key K) ([]ValueWithTxNum[V], bool)
	Put(key K, value ValueWithTxNum[V]) (bool, error)
	Iter() iter.Seq2[K, []ValueWithTxNum[V]]
	Clear() updates[K, V]
}

//keySerializer func(k K) []byte
//keyFormatter func(k K) string

type elementOps[V any] struct {
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
	keyOps     elementOps[K]
	valueOps   elementOps[V]
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
	d.lock.RLock()
	val, ok := d.updates.Get(k)
	d.lock.RUnlock()

	if ok {
		d.metrics.UpdateCacheReads(d.name(), start)
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
			d.metrics.UpdateCacheReads(d.name(), start)
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
			d.metrics.UpdateCacheReads(d.name(), start)
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

	d.lock.Lock()
	inserted, err := d.updates.Put(k, ValueWithTxNum[V]{v, txNum})
	d.lock.Unlock()

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

	d.lock.Lock()
	inserted, err := d.updates.Put(k, ValueWithTxNum[V]{TxNum: txNum})
	d.lock.Unlock()

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
		MeteredGetLatest(domain kv.Domain, k []byte, tx kv.Tx, maxStep kv.Step, metrics *DomainMetrics, start time.Time) (v []byte, step kv.Step, ok bool, err error)
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
	sd.metrics.Lock()
	defer sd.metrics.Unlock()
	if dm, ok := sd.metrics.Domains[sd.name()]; ok {
		dm.CachePutCount = 0
		dm.CachePutSize = 0
		dm.CachePutKeySize = 0
		dm.CachePutValueSize = 0
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
		for _, v := range vs {
			d.valueCache.Add(k, v.Value, v.TxNum)
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

type accountUpdates map[accounts.Address][]ValueWithTxNum[*accounts.Account]

func (u accountUpdates) Get(k accounts.Address) ([]ValueWithTxNum[*accounts.Account], bool) {
	v, ok := u[k]
	return v, ok
}

func (u accountUpdates) Put(k accounts.Address, v ValueWithTxNum[*accounts.Account]) (bool, error) {
	if values, exists := u[k]; exists {
		if v.TxNum <= values[len(values)-1].TxNum {
			return false, fmt.Errorf("can't insert non sequential tx: got: %d, expected > %d", v.TxNum, values[len(values)-1].TxNum)
		}
		u[k] = append(values, v)
		return true, nil
	} else {
		u[k] = []ValueWithTxNum[*accounts.Account]{v}
		return false, nil
	}
}

func (u accountUpdates) Iter() iter.Seq2[accounts.Address, []ValueWithTxNum[*accounts.Account]] {
	return func(yield func(accounts.Address, []ValueWithTxNum[*accounts.Account]) bool) {
		for k, v := range u {
			yield(k, v)
		}
	}
}

func (u accountUpdates) Clear() updates[accounts.Address, *accounts.Account] {
	return accountUpdates{}
}

func NewAccountsDomain(mem kv.TemporalMemBatch, storage *StorageDomain, code *CodeDomain, commitCtx *commitment.CommitmentContext, metrics *DomainMetrics) (*AccountsDomain, error) {
	cache := mem.ValueCache(kv.AccountsDomain)

	if cache != nil {
		if _, ok := cache.(*lruValueCache[accounts.Address, common.Address, *accounts.Account]); !ok {
			return nil, fmt.Errorf("unexpected cache initializaton type: got: %T, expected %T", cache, &lruValueCache[accounts.Address, common.Address, *accounts.Account]{})
		}
	} else {
		var err error
		cache, err = newLRUValueCache[accounts.Address, common.Address, *accounts.Account](kv.AccountsDomain, 250_000)
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
			updates:    accountUpdates{},
			keyOps: elementOps[accounts.Address]{
				serializer: func(k accounts.Address) []byte {
					kv := k.Value()
					return kv[:]
				},
				formatter: func(k accounts.Address) string {
					return k.Value().Hex()
				},
			},
			valueOps: elementOps[*accounts.Account]{
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
		return fmt.Errorf("accounts domain: %s, trying to put nil value. not allowed", kv.AccountsDomain)
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

func newStroageCache(limit uint32) (*storageCache, error) {
	type handle[U comparable] struct{ value *U }

	if unsafe.Sizeof(handle[common.Address]{}) != unsafe.Sizeof(unique.Handle[common.Address]{}) {
		panic("handle type != unique.Handle - check unique.Handle implementation details for this version of go")
	}

	c, err := freelru.NewSharded[storageLocation, ValueWithTxNum[uint256.Int]](limit, func(k storageLocation) uint32 {
		return uint32(uintptr(unsafe.Pointer((*handle[common.Address])(unsafe.Pointer(&k.address)).value))) ^
			uint32(uintptr(unsafe.Pointer((*handle[common.Hash])(unsafe.Pointer(&k.address)).value)))
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

type storageUpdates map[accounts.Address]map[accounts.StorageKey][]ValueWithTxNum[uint256.Int]

func (u storageUpdates) Get(k storageLocation) ([]ValueWithTxNum[uint256.Int], bool) {
	v, ok := u[k.address][k.key]
	return v, ok
}

func (u storageUpdates) Put(k storageLocation, v ValueWithTxNum[uint256.Int]) (bool, error) {
	if vm, ok := u[k.address]; ok {
		if values, exists := vm[k.key]; exists {
			if v.TxNum <= values[len(values)-1].TxNum {
				return false, fmt.Errorf("can't insert non sequential tx: got: %d, expected > %d", v.TxNum, values[len(values)-1].TxNum)
			}
			vm[k.key] = append(values, v)
			return true, nil
		} else {
			vm[k.key] = []ValueWithTxNum[uint256.Int]{v}
		}
	} else {
		u[k.address] = map[accounts.StorageKey][]ValueWithTxNum[uint256.Int]{k.key: []ValueWithTxNum[uint256.Int]{v}}
	}

	return false, nil
}

func (u storageUpdates) Iter() iter.Seq2[storageLocation, []ValueWithTxNum[uint256.Int]] {
	return func(yield func(storageLocation, []ValueWithTxNum[uint256.Int]) bool) {
		for a, m := range u {
			for k, v := range m {
				yield(storageLocation{address: a, key: k}, v)
			}
		}
	}
}

func (u storageUpdates) UpdatedSlots(addr accounts.Address) (map[accounts.StorageKey][]ValueWithTxNum[uint256.Int], bool) {
	vm, ok := u[addr]
	return vm, ok
}

func (u storageUpdates) SortedIter(addr accounts.Address) iter.Seq2[storageLocation, []ValueWithTxNum[uint256.Int]] {
	if addr.IsNil() {
		iter := slices.SortedFunc(maps.Keys(u), func(a, b accounts.Address) int { return a.Value().Cmp(b.Value()) })
		return func(yield func(storageLocation, []ValueWithTxNum[uint256.Int]) bool) {
			for _, a := range iter {
				for _, k := range slices.SortedFunc(maps.Keys(u[a]), func(a, b accounts.StorageKey) int { return a.Value().Cmp(b.Value()) }) {
					yield(storageLocation{address: a, key: k}, u[a][k])
				}
			}
		}
	}

	return func(yield func(storageLocation, []ValueWithTxNum[uint256.Int]) bool) {
		for _, k := range slices.SortedFunc(maps.Keys(u[addr]), func(a, b accounts.StorageKey) int { return a.Value().Cmp(b.Value()) }) {
			yield(storageLocation{address: addr, key: k}, u[addr][k])
		}
	}
}

func (u storageUpdates) Clear() updates[storageLocation, uint256.Int] {
	return storageUpdates{}
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
		cache, err = newStroageCache(250_000)
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
			updates:    storageUpdates{},
			keyOps: elementOps[storageLocation]{
				serializer: func(k storageLocation) []byte {
					av := k.address.Value()
					kv := k.key.Value()
					return append(av[:], kv[:]...)
				},
				formatter: func(k storageLocation) string {
					return fmt.Sprintf("%x %x", k.address.Value(), k.key.Value())
				},
			},
			valueOps: elementOps[uint256.Int]{
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
	iter := sd.updates.(storageUpdates).SortedIter(addr)
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
	sd.lock.RLock()
	slots, ok := sd.updates.(storageUpdates).UpdatedSlots(addr)
	sd.lock.RUnlock()
	if ok {
		for _, slot := range slots {
			if slot[len(slot)-1].Value.ByteLen() > 0 {
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
		return !hasPrefix, nil
	})
	return hasPrefix, err
}

func (sd *StorageDomain) IterateStorage(ctx context.Context, addr accounts.Address, it func(k accounts.StorageKey, v uint256.Int, step kv.Step) (cont bool, err error), roTx kv.Tx) error {
	addrVal := addr.Value()
	return sd.mem.IteratePrefix(kv.StorageDomain, addrVal[:], sd.slotIterator(addr), roTx, func(k []byte, v []byte, step kv.Step) (cont bool, err error) {
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
	hashes map[accounts.Address]accounts.CodeHash
	code   map[accounts.CodeHash][]ValueWithTxNum[codeWithHash]
}

func (u codeUpdates) Get(k accounts.Address) ([]ValueWithTxNum[codeWithHash], bool) {
	h, ok := u.hashes[k]

	if !ok {
		return nil, false
	}

	if h.IsEmpty() {
		return []ValueWithTxNum[codeWithHash]{{Value: codeWithHash{hash: h}}}, true
	}

	c, ok := u.code[h]

	return c, ok
}

func (u codeUpdates) Put(k accounts.Address, v ValueWithTxNum[codeWithHash]) (bool, error) {
	h, exists := u.hashes[k]

	if exists {
		if h != v.Value.hash {
			u.hashes[k] = v.Value.hash
		}
	} else {
		u.hashes[k] = v.Value.hash
	}

	if !v.Value.hash.IsEmpty() {
		if _, ok := u.code[v.Value.hash]; !ok {
			u.code[v.Value.hash] = []ValueWithTxNum[codeWithHash]{v}
		}
	}

	return !exists, nil
}

func (u codeUpdates) Iter() iter.Seq2[accounts.Address, []ValueWithTxNum[codeWithHash]] {
	return func(yield func(accounts.Address, []ValueWithTxNum[codeWithHash]) bool) {
		for a, h := range u.hashes {
			yield(a, u.code[h])
		}
	}
}

func (u codeUpdates) Clear() updates[accounts.Address, codeWithHash] {
	return codeUpdates{
		hashes: map[accounts.Address]accounts.CodeHash{},
		code:   map[accounts.CodeHash][]ValueWithTxNum[codeWithHash]{},
	}
}

type codeCache struct {
	hashes *freelru.ShardedLRU[accounts.Address, accounts.CodeHash]
	code   *freelru.ShardedLRU[accounts.CodeHash, ValueWithTxNum[weakCodeWithHash]]
	limit  uint32
}

func newCodeCache(limit uint32) (*codeCache, error) {
	type handle[U comparable] struct{ value *U }

	if unsafe.Sizeof(handle[common.Address]{}) != unsafe.Sizeof(unique.Handle[common.Address]{}) {
		panic("handle type != unique.Handle - check unique.Handle implementation details for this version of go")
	}

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
			updates: codeUpdates{
				hashes: map[accounts.Address]accounts.CodeHash{},
				code:   map[accounts.CodeHash][]ValueWithTxNum[codeWithHash]{},
			},
			keyOps: elementOps[accounts.Address]{
				serializer: func(k accounts.Address) []byte {
					av := k.Value()
					return av[:]
				},
				formatter: func(k accounts.Address) string {
					return fmt.Sprintf("%x", k.Value())
				},
			},
			valueOps: elementOps[codeWithHash]{
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
	domain[commitment.Path, commitment.Branch]
}

type branchCache struct {
	state       ValueWithTxNum[commitment.Branch]
	t0          ValueWithTxNum[commitment.Branch]
	t1          [16]ValueWithTxNum[commitment.Branch]
	t2          [256]ValueWithTxNum[commitment.Branch]
	t3          [4096]ValueWithTxNum[commitment.Branch]
	t4          [65536]ValueWithTxNum[commitment.Branch]
	branches    *lruValueCache[commitment.Path, string, commitment.Branch]
	branchLimit uint32
}

func newBranchCache(branchLimit uint32) (*branchCache, error) {
	c, err := newLRUValueCache[commitment.Path, string, commitment.Branch](kv.CommitmentDomain, branchLimit)

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

func (c *branchCache) Get(key commitment.Path) (ValueWithTxNum[commitment.Branch], bool) {
	// see: HexNibblesToCompactBytes for encoding spec
	switch {
	case key == statePath:
		if len(c.state.Value) == 0 {
			return ValueWithTxNum[commitment.Branch]{}, false
		}
		return c.state, true
	case len(key.Value()) < 4:
		keyValue := key.Value()
		switch len(keyValue) {
		case 0:
			return ValueWithTxNum[commitment.Branch]{}, false
		case 1:
			if keyValue[0]&0x10 == 0 {
				if len(c.t0.Value) > 0 {
					return c.t0, true
				}
				return ValueWithTxNum[commitment.Branch]{}, false
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

func (c *branchCache) Add(k commitment.Path, v commitment.Branch, txNum uint64) (evicted bool) {
	// see: HexNibblesToCompactBytes for encoding spec
	switch {
	case k == statePath:
		c.state = ValueWithTxNum[commitment.Branch]{Value: v, TxNum: txNum}
		return false
	case len(k.Value()) < 4:
		keyValue := k.Value()
		switch len(keyValue) {
		case 0:
			return false
		case 1:
			if keyValue[0]&0x10 == 0 {
				c.t0 = ValueWithTxNum[commitment.Branch]{Value: v, TxNum: txNum}
				return false
			} else {
				c.t1[keyValue[0]&0x0f] = ValueWithTxNum[commitment.Branch]{Value: v, TxNum: txNum}
				return false
			}
		case 2:
			if keyValue[0]&0x10 == 0 {
				c.t2[keyValue[1]] = ValueWithTxNum[commitment.Branch]{Value: v, TxNum: txNum}
			} else {
				c.t3[uint16(keyValue[0]&0x0f)<<8|uint16(keyValue[1])] = ValueWithTxNum[commitment.Branch]{Value: v, TxNum: txNum}
			}
			return false
		default:
			if keyValue[0]&0x10 == 0 {
				c.t4[uint16(keyValue[1])<<8|uint16(keyValue[2])] = ValueWithTxNum[commitment.Branch]{Value: v, TxNum: txNum}
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
		c.state = ValueWithTxNum[commitment.Branch]{}
		return evicted
	case len(k.Value()) < 4:
		keyValue := k.Value()
		switch len(keyValue) {
		case 0:
			return false
		case 1:
			if keyValue[0]&0x10 == 0 {
				evicted = len(c.t0.Value) > 0
				c.t0 = ValueWithTxNum[commitment.Branch]{}
				return evicted
			} else {
				evicted := len(c.t1[keyValue[0]&0x0f].Value) > 0
				c.t1[keyValue[0]&0x0f] = ValueWithTxNum[commitment.Branch]{}
				return evicted
			}
		case 2:
			if keyValue[0]&0x10 == 0 {
				kv := keyValue[1]
				evicted := len(c.t2[kv].Value) > 0
				c.t2[kv] = ValueWithTxNum[commitment.Branch]{}
				return evicted
			} else {
				kv := uint16(keyValue[0]&0x0f)<<8 | uint16(keyValue[1])
				evicted := len(c.t3[kv].Value) > 0
				c.t3[kv] = ValueWithTxNum[commitment.Branch]{}
				return evicted
			}
		default:
			if keyValue[0]&0x10 == 0 {
				kv := uint16(keyValue[1])<<8 | uint16(keyValue[2])
				evicted := len(c.t4[kv].Value) > 0
				c.t4[kv] = ValueWithTxNum[commitment.Branch]{}
				return evicted
			}
		}
	}
	return c.branches.Remove(k)
}

type branchUpdates map[commitment.Path][]ValueWithTxNum[commitment.Branch]

func (u branchUpdates) Get(k commitment.Path) ([]ValueWithTxNum[commitment.Branch], bool) {
	v, ok := u[k]
	return v, ok
}

func (u branchUpdates) Put(k commitment.Path, v ValueWithTxNum[commitment.Branch]) (bool, error) {
	if values, exists := u[k]; exists {
		if v.TxNum <= values[len(values)-1].TxNum {
			return false, fmt.Errorf("can't insert non sequential tx: got: %d, expected > %d", v.TxNum, values[len(values)-1].TxNum)
		}
		u[k] = append(values, v)
		return true, nil
	} else {
		u[k] = []ValueWithTxNum[commitment.Branch]{v}
		return false, nil
	}
}

func (u branchUpdates) Iter() iter.Seq2[commitment.Path, []ValueWithTxNum[commitment.Branch]] {
	return func(yield func(commitment.Path, []ValueWithTxNum[commitment.Branch]) bool) {
		for k, v := range u {
			yield(k, v)
		}
	}
}

func (u branchUpdates) Clear() updates[commitment.Path, commitment.Branch] {
	return branchUpdates{}
}

func NewCommitmentDomain(mem kv.TemporalMemBatch, commitCtx *commitment.CommitmentContext, metrics *DomainMetrics) (*CommitmentDomain, error) {
	cache := mem.ValueCache(kv.CommitmentDomain)

	if cache != nil {
		if _, ok := cache.(*branchCache); !ok {
			return nil, fmt.Errorf("unexpected cache initializaton type: got: %T, expected %T", cache, &branchCache{})
		}
	} else {
		var err error
		cache, err = newBranchCache(500_000)
		if err != nil {
			return nil, err
		}
		mem.SetValueCache(cache)
	}

	return &CommitmentDomain{
		domain: domain[commitment.Path, commitment.Branch]{
			metrics:    metrics,
			mem:        mem,
			commitCtx:  commitCtx,
			valueCache: cache.(valueCache[commitment.Path, commitment.Branch]),
			updates:    branchUpdates{},
			keyOps: elementOps[commitment.Path]{
				serializer: func(k commitment.Path) []byte {
					return k.Value()
				},
				formatter: func(k commitment.Path) string {
					return fmt.Sprintf("%x", k.Value())
				},
			},
			valueOps: elementOps[commitment.Branch]{
				cmp: func(v0 commitment.Branch, v1 commitment.Branch) bool {
					return bytes.Equal(v0, v1)
				},
				isEmpty: func(v commitment.Branch) bool {
					return len(v) == 0
				},
				serializer: func(v commitment.Branch) []byte {
					return v
				},
				deserializer: func(b []byte) (v commitment.Branch, err error) {
					return commitment.Branch(common.Copy(b)), nil
				},
				formatter: func(c commitment.Branch) string {
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

func (cd *CommitmentDomain) GetBranch(ctx context.Context, k commitment.Path, tx kv.TemporalTx) (v commitment.Branch, txNum uint64, ok bool, err error) {
	return cd.domain.get(dbg.WithTrace(ctx, true), k, tx)
}

func (cd *CommitmentDomain) PutBranch(ctx context.Context, k commitment.Path, v commitment.Branch, roTx kv.TemporalTx, txNum uint64, prev ...ValueWithTxNum[commitment.Branch]) error {
	if v == nil {
		return fmt.Errorf("PutBranch: %s, trying to put nil value. not allowed", kv.CommitmentDomain)
	}

	var pv *ValueWithTxNum[commitment.Branch]
	if len(prev) != 0 {
		pv = &prev[0]
	}

	return cd.domain.put(dbg.WithTrace(ctx, true), k, v, roTx, txNum, pv)
}

func (cd *CommitmentDomain) DelBranch(ctx context.Context, k commitment.Path, roTx kv.TemporalTx, txNum uint64, prev ...ValueWithTxNum[commitment.Branch]) error {
	var pv *ValueWithTxNum[commitment.Branch]
	if len(prev) != 0 {
		pv = &prev[0]
	}

	return cd.domain.del(dbg.WithTrace(ctx, true), k, roTx, txNum, pv)
}
