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
	Add(key K, value V, step kv.Step) (evicted bool)
	Remove(key K) (evicted bool)
	Get(key K) (value ValueWithStep[V], ok bool)
}

type lruValueCache[K comparable, U comparable, V any] struct {
	d     kv.Domain
	lru   *freelru.ShardedLRU[K, ValueWithStep[V]]
	limit uint32
}

func newLRUValueCache[K comparable, U comparable, V any](d kv.Domain, limit uint32) (*lruValueCache[K, U, V], error) {
	type handle[U comparable] struct{ value *U }

	if unsafe.Sizeof(handle[U]{}) != unsafe.Sizeof(unique.Handle[U]{}) {
		panic("handle type != unique.Handle - check unique.Handle implementation details for this version of go")
	}

	c, err := freelru.NewSharded[K, ValueWithStep[V]](limit, func(k K) uint32 {
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

func (c *lruValueCache[K, U, V]) Add(key K, value V, step kv.Step) (evicted bool) {
	return c.lru.Add(key, ValueWithStep[V]{value, step})
}

func (c *lruValueCache[K, U, V]) Remove(key K) (evicted bool) {
	return c.lru.Remove(key)
}

func (c *lruValueCache[K, U, V]) Get(key K) (value ValueWithStep[V], ok bool) {
	return c.lru.GetAndRefresh(key, 5*time.Minute)
}

type updates[K comparable, V any] interface {
	Get(key K) (ValueWithStep[V], bool)
	Put(key K, value ValueWithStep[V]) bool
	Iter() iter.Seq2[K, ValueWithStep[V]]
	Clear() updates[K, V]
}

type domain[K comparable, V any] struct {
	lock       sync.RWMutex
	mem        kv.TemporalMemBatch
	metrics    *DomainMetrics
	commitCtx  *commitment.CommitmentContext
	valueCache valueCache[K, V]
	updates    updates[K, V]
}

func (d *domain[K, V]) get(ctx context.Context, k K, tx kv.TemporalTx, serializer func(k K) []byte, deserializer func(b []byte) (V, error)) (v V, step kv.Step, ok bool, err error) {
	if tx == nil {
		return v, 0, false, errors.New("domain get: unexpected nil tx")
	}

	start := time.Now()
	d.lock.RLock()
	val, ok := d.updates.Get(k)
	d.lock.RUnlock()

	if ok {
		d.metrics.UpdateCacheReads(d.valueCache.Name(), start)
		return val.Value, val.Step, true, nil
	}

	maxStep := kv.Step(math.MaxUint64)

	if d.mem.IsUnwound() {
		av := serializer(k)
		if val, step, ok := d.mem.GetLatest(d.valueCache.Name(), av[:]); ok {
			if v, err = deserializer(val); err != nil {
				return v, step, false, err
			}
			d.metrics.UpdateCacheReads(d.valueCache.Name(), start)
			return v, step, true, nil
		} else {
			if step > 0 {
				maxStep = step
			}
		}
	}

	if d.valueCache != nil {
		if val, ok := d.valueCache.Get(k); ok {
			d.metrics.UpdateCacheReads(d.valueCache.Name(), start)
			return val.Value, val.Step, true, nil
		}
	}

	av := serializer(k)
	latest, step, err := d.getLatest(ctx, d.valueCache.Name(), av[:], tx, maxStep, start)

	if len(latest) == 0 {
		return v, step, false, nil
	}

	if v, err = deserializer(latest); err != nil {
		return v, step, false, err
	}

	if d.valueCache != nil {
		d.valueCache.Add(k, v, step)
	}

	return v, step, true, nil
}

func (d *domain[K, V]) put(ctx context.Context, k K, v V,
	keySerializer func(k K) []byte, valueCmp func(v0 V, v1 V) bool, valueSerializer func(v V) []byte, valueDeserializer func(b []byte) (V, error),
	roTx kv.TemporalTx, txNum uint64, prev *ValueWithStep[V]) error {
	var ok bool
	var prevVal V
	var prevStep kv.Step

	if prev == nil {
		var err error
		prevVal, prevStep, ok, err = d.get(ctx, k, roTx, keySerializer, valueDeserializer)
		if err != nil {
			return err
		}
	} else {
		ok = true
		prevVal = prev.Value
		prevStep = prev.Step
	}

	if ok && valueCmp(v, prevVal) {
		return nil
	}

	vbuf := valueSerializer(v)
	kbuf := keySerializer(k)
	var pvbuf []byte
	if ok {
		pvbuf = valueSerializer(prevVal)
	}
	if err := d.mem.DomainPut(d.valueCache.Name(), kbuf, vbuf, txNum, pvbuf, prevStep); err != nil {
		return err
	}

	putKeySize := 0
	putValueSize := 0

	d.lock.Lock()
	inserted := d.updates.Put(k, ValueWithStep[V]{v, kv.Step(txNum / roTx.StepSize())})
	d.lock.Unlock()

	if inserted {
		putKeySize += len(kbuf)
		putValueSize += len(vbuf)
	} else {
		putValueSize = len(vbuf) - len(pvbuf)
	}

	d.metrics.UpdatePutCacheWrites(d.valueCache.Name(), putKeySize, putValueSize)
	return nil
}

func (d *domain[K, V]) del(ctx context.Context, k K,
	keySerializer func(k K) []byte, valueSerializer func(v V) []byte, valueDeserializer func(b []byte) (V, error),
	roTx kv.TemporalTx, txNum uint64, prev *ValueWithStep[V]) error {
	var ok bool
	var prevVal V
	var prevStep kv.Step

	if prev == nil {
		var err error
		prevVal, prevStep, ok, err = d.get(ctx, k, roTx, keySerializer, valueDeserializer)
		if err != nil {
			return err
		}
	} else {
		ok = true
		prevVal = prev.Value
		prevStep = prev.Step
	}

	kbuf := keySerializer(k)
	var pvbuf []byte
	if ok {
		pvbuf = valueSerializer(prevVal)
	}

	if err := d.mem.DomainDel(kv.AccountsDomain, kbuf, txNum, pvbuf, prevStep); err != nil {
		return err
	}

	putKeySize := 0
	putValueSize := 0

	d.lock.Lock()
	inserted := d.updates.Put(k, ValueWithStep[V]{Step: kv.Step(txNum / roTx.StepSize())})
	d.lock.Unlock()

	if inserted {
		putKeySize += len(kbuf)
	} else {
		putValueSize = -len(pvbuf)
	}

	d.metrics.UpdatePutCacheWrites(d.valueCache.Name(), putKeySize, putValueSize)
	return nil
}

func (d *domain[K, V]) getLatest(ctx context.Context, domain kv.Domain, k []byte, tx kv.TemporalTx, maxStep kv.Step, start time.Time) (v []byte, step kv.Step, err error) {
	type MeteredGetter interface {
		MeteredGetLatest(domain kv.Domain, k []byte, tx kv.Tx, maxStep kv.Step, metrics *DomainMetrics, start time.Time) (v []byte, step kv.Step, ok bool, err error)
	}

	if aggTx, ok := tx.AggTx().(MeteredGetter); ok {
		v, step, _, err = aggTx.MeteredGetLatest(domain, k, tx, maxStep, d.metrics, start)
	} else {
		v, step, err = tx.GetLatest(domain, k)
	}

	if err != nil {
		return nil, 0, fmt.Errorf("account %s read error: %w", k, err)
	}

	return v, step, nil
}

func (sd *domain[K, V]) ClearMetrics() {
	sd.metrics.Lock()
	defer sd.metrics.Unlock()
	if dm, ok := sd.metrics.Domains[kv.AccountsDomain]; ok {
		dm.CachePutCount = 0
		dm.CachePutSize = 0
		dm.CachePutKeySize = 0
		dm.CachePutValueSize = 0
	}
}

func (d *domain[K, V]) Merge(other *domain[K, V]) {
	d.lock.Lock()
	defer d.lock.Unlock()
	for key, value := range other.updates.Iter() {
		d.updates.Put(key, value)
	}
}

func (d *domain[K, V]) FlushUpdates() {
	d.lock.Lock()
	defer d.lock.Unlock()
	for k, v := range d.updates.Iter() {
		d.valueCache.Add(k, v.Value, v.Step)
	}
	d.updates = d.updates.Clear()
	d.ClearMetrics()
}

type AccountsDomain struct {
	domain[accounts.Address, *accounts.Account]
	storage *StorageDomain
	code    *CodeDomain
}

type accountUpdates map[accounts.Address]ValueWithStep[*accounts.Account]

func (u accountUpdates) Get(k accounts.Address) (ValueWithStep[*accounts.Account], bool) {
	v, ok := u[k]
	return v, ok
}

func (u accountUpdates) Put(k accounts.Address, v ValueWithStep[*accounts.Account]) bool {
	_, exists := u[k]
	u[k] = v
	return !exists
}

func (u accountUpdates) Iter() iter.Seq2[accounts.Address, ValueWithStep[*accounts.Account]] {
	return func(yield func(accounts.Address, ValueWithStep[*accounts.Account]) bool) {
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
		},
		storage: storage,
		code:    code,
	}, nil
}

func (ad *AccountsDomain) Get(ctx context.Context, k accounts.Address, tx kv.TemporalTx) (v *accounts.Account, step kv.Step, ok bool, err error) {
	return ad.domain.get(ctx, k, tx,
		func(k accounts.Address) []byte {
			kv := k.Value()
			return kv[:]
		},
		func(b []byte) (v *accounts.Account, err error) {
			v = &accounts.Account{}
			err = accounts.DeserialiseV3(v, b)
			return v, err
		})
}

func (ad *AccountsDomain) Put(ctx context.Context, k accounts.Address, v *accounts.Account, roTx kv.TemporalTx, txNum uint64, prev ...ValueWithStep[*accounts.Account]) error {
	if v == nil {
		return fmt.Errorf("accounts domain: %s, trying to put nil value. not allowed", kv.AccountsDomain)
	}

	ad.commitCtx.TouchAccount(k, v)

	var pv *ValueWithStep[*accounts.Account]
	if len(prev) != 0 {
		pv = &prev[0]
	}

	return ad.domain.put(ctx, k, v,
		func(k accounts.Address) []byte {
			kv := k.Value()
			return kv[:]
		},
		func(v0 *accounts.Account, v1 *accounts.Account) bool {
			return v0.Equals(v1)
		},
		func(v *accounts.Account) []byte {
			return accounts.SerialiseV3(v)
		},
		func(b []byte) (v *accounts.Account, err error) {
			v = &accounts.Account{}
			err = accounts.DeserialiseV3(v, b)
			return v, err
		},
		roTx, txNum, pv)
}

func (ad *AccountsDomain) Del(ctx context.Context, k accounts.Address, roTx kv.TemporalTx, txNum uint64, prev ...ValueWithStep[*accounts.Account]) error {
	ad.commitCtx.TouchAccount(k, nil)

	if err := ad.storage.Del(ctx, k, accounts.NilKey, roTx, txNum); err != nil {
		return err
	}

	if err := ad.code.Del(ctx, k, roTx, txNum); err != nil {
		return err
	}

	var pv *ValueWithStep[*accounts.Account]
	if len(prev) != 0 {
		pv = &prev[0]
	}

	return ad.domain.del(ctx, k,
		func(k accounts.Address) []byte {
			kv := k.Value()
			return kv[:]
		},
		func(v *accounts.Account) []byte {
			return accounts.SerialiseV3(v)
		},
		func(b []byte) (v *accounts.Account, err error) {
			v = &accounts.Account{}
			err = accounts.DeserialiseV3(v, b)
			return v, err
		},
		roTx, txNum, pv)
}

type storageLocation struct {
	address accounts.Address
	key     accounts.StorageKey
}

type storageCache struct {
	lru   *freelru.ShardedLRU[storageLocation, ValueWithStep[uint256.Int]]
	limit uint32
}

func newStroageCache(limit uint32) (*storageCache, error) {
	type handle[U comparable] struct{ value *U }

	if unsafe.Sizeof(handle[common.Address]{}) != unsafe.Sizeof(unique.Handle[common.Address]{}) {
		panic("handle type != unique.Handle - check unique.Handle implementation details for this version of go")
	}

	c, err := freelru.NewSharded[storageLocation, ValueWithStep[uint256.Int]](limit, func(k storageLocation) uint32 {
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

func (c *storageCache) Add(key storageLocation, value uint256.Int, step kv.Step) (evicted bool) {
	return c.lru.Add(key, ValueWithStep[uint256.Int]{value, step})
}

func (c storageCache) Remove(key storageLocation) (evicted bool) {
	return c.lru.Remove(key)
}

func (c *storageCache) Get(key storageLocation) (value ValueWithStep[uint256.Int], ok bool) {
	return c.lru.GetAndRefresh(key, 5*time.Minute)
}

type storageUpdates map[accounts.Address]map[accounts.StorageKey]ValueWithStep[uint256.Int]

func (u storageUpdates) Get(k storageLocation) (ValueWithStep[uint256.Int], bool) {
	v, ok := u[k.address][k.key]
	return v, ok
}

func (u storageUpdates) Put(k storageLocation, v ValueWithStep[uint256.Int]) bool {
	exists := false
	if vm, ok := u[k.address]; ok {
		_, exists = vm[k.key]
		vm[k.key] = v
	} else {
		u[k.address] = map[accounts.StorageKey]ValueWithStep[uint256.Int]{k.key: v}
	}

	return !exists
}

func (u storageUpdates) Iter() iter.Seq2[storageLocation, ValueWithStep[uint256.Int]] {
	return func(yield func(storageLocation, ValueWithStep[uint256.Int]) bool) {
		for a, m := range u {
			for k, v := range m {
				yield(storageLocation{address: a, key: k}, v)
			}
		}
	}
}

func (u storageUpdates) UpdatedSlots(addr accounts.Address) (map[accounts.StorageKey]ValueWithStep[uint256.Int], bool) {
	vm, ok := u[addr]
	return vm, ok
}

func (u storageUpdates) SortedIter(addr accounts.Address) iter.Seq2[storageLocation, ValueWithStep[uint256.Int]] {
	if addr.IsNil() {
		iter := slices.SortedFunc(maps.Keys(u), func(a, b accounts.Address) int { return a.Value().Cmp(b.Value()) })
		return func(yield func(storageLocation, ValueWithStep[uint256.Int]) bool) {
			for _, a := range iter {
				for _, k := range slices.SortedFunc(maps.Keys(u[a]), func(a, b accounts.StorageKey) int { return a.Value().Cmp(b.Value()) }) {
					yield(storageLocation{address: a, key: k}, u[a][k])
				}
			}
		}
	}

	return func(yield func(storageLocation, ValueWithStep[uint256.Int]) bool) {
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
		},
	}, nil
}

func (sd *StorageDomain) Get(ctx context.Context, addr accounts.Address, key accounts.StorageKey, tx kv.TemporalTx) (v uint256.Int, step kv.Step, ok bool, err error) {
	return sd.domain.get(ctx, storageLocation{address: addr, key: key}, tx,
		func(k storageLocation) []byte {
			av := k.address.Value()
			kv := k.key.Value()
			return append(av[:], kv[:]...)
		},
		func(b []byte) (v uint256.Int, err error) {
			v.SetBytes(b)
			return v, nil
		})
}

func (sd *StorageDomain) Put(ctx context.Context, addr accounts.Address, key accounts.StorageKey, v uint256.Int, roTx kv.TemporalTx, txNum uint64, prev ...ValueWithStep[uint256.Int]) error {
	sd.commitCtx.TouchStorage(addr, key, v)

	var pv *ValueWithStep[uint256.Int]
	if len(prev) != 0 {
		pv = &prev[0]
	}

	return sd.domain.put(ctx, storageLocation{addr, key}, v,
		func(k storageLocation) []byte {
			av := k.address.Value()
			kv := k.key.Value()
			return append(av[:], kv[:]...)
		},
		func(v0 uint256.Int, v1 uint256.Int) bool {
			return v0 == v1
		},
		func(v uint256.Int) []byte {
			return v.Bytes()
		},
		func(b []byte) (v uint256.Int, err error) {
			v.SetBytes(b)
			return v, err
		},
		roTx, txNum, pv)
}

func (sd *StorageDomain) Del(ctx context.Context, addr accounts.Address, key accounts.StorageKey, roTx kv.TemporalTx, txNum uint64, prev ...ValueWithStep[uint256.Int]) error {
	if !key.IsNil() && addr.IsNil() {
		return errors.New("address unexpectedly nil")
	}

	if addr.IsNil() || key.IsNil() {
		return sd.DelAll(ctx, addr, roTx, txNum)
	}

	sd.commitCtx.TouchStorage(addr, key, uint256.Int{})

	var pv *ValueWithStep[uint256.Int]

	if len(prev) != 0 {
		pv = &prev[0]
	}

	return sd.domain.del(ctx, storageLocation{addr, key},
		func(k storageLocation) []byte {
			av := k.address.Value()
			kv := k.key.Value()
			return append(av[:], kv[:]...)
		},
		func(v uint256.Int) []byte {
			return v.Bytes()
		},
		func(b []byte) (v uint256.Int, err error) {
			v.SetBytes(b)
			return v, err
		},
		roTx, txNum, pv)
}

func (sd *StorageDomain) slotIterator(addr accounts.Address) func(yield func(string, kv.DataWithStep) bool) {
	sd.lock.RLock()
	iter := sd.updates.(storageUpdates).SortedIter(addr)
	sd.lock.RUnlock()
	return func(yield func(string, kv.DataWithStep) bool) {
		for k, v := range iter {
			aval := k.address.Value()
			kval := k.key.Value()
			yield(string(append(aval[:], kval[:]...)), kv.DataWithStep{Data: v.Value.Bytes(), Step: v.Step})
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
			roTx, txNum, ValueWithStep[uint256.Int]{tv, tomb.step}); err != nil {
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
			if slot.Value.ByteLen() > 0 {
				return true, nil
			}
		}
	}

	var hasPrefix bool
	addrVal := addr.Value()
	err := sd.mem.IteratePrefix(kv.StorageDomain, addrVal[:], nil, roTx, func(k []byte, v []byte, step kv.Step) (bool, error) {
		hasPrefix = true
		return false, nil // do not continue, end on first occurrence
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

type CodeWithStep struct {
	Code []byte
	Hash accounts.CodeHash
	Step kv.Step
}

type codeUpdates struct {
	hashes map[accounts.Address]accounts.CodeHash
	code   map[accounts.CodeHash]ValueWithStep[codeWithHash]
}

func (u codeUpdates) Get(k accounts.Address) (ValueWithStep[codeWithHash], bool) {
	h, ok := u.hashes[k]

	if !ok {
		return ValueWithStep[codeWithHash]{}, false
	}

	if h.IsEmpty() {
		return ValueWithStep[codeWithHash]{Value: codeWithHash{hash: h}}, true
	}

	c, ok := u.code[h]

	return c, ok
}

func (u codeUpdates) Put(k accounts.Address, v ValueWithStep[codeWithHash]) bool {
	h, exists := u.hashes[k]

	if exists {
		if h != v.Value.hash {
			u.hashes[k] = v.Value.hash
		}
	} else {
		u.hashes[k] = v.Value.hash
	}

	if !v.Value.hash.IsEmpty() {
		u.code[v.Value.hash] = v
	}

	return !exists
}

func (u codeUpdates) Iter() iter.Seq2[accounts.Address, ValueWithStep[codeWithHash]] {
	return func(yield func(accounts.Address, ValueWithStep[codeWithHash]) bool) {
		for a, h := range u.hashes {
			yield(a, u.code[h])
		}
	}
}

func (u codeUpdates) Clear() updates[accounts.Address, codeWithHash] {
	return codeUpdates{
		hashes: map[accounts.Address]accounts.CodeHash{},
		code:   map[accounts.CodeHash]ValueWithStep[codeWithHash]{},
	}
}

type codeCache struct {
	hashes *freelru.ShardedLRU[accounts.Address, accounts.CodeHash]
	code   *freelru.ShardedLRU[accounts.CodeHash, ValueWithStep[weakCodeWithHash]]
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

	code, err := freelru.NewSharded[accounts.CodeHash, ValueWithStep[weakCodeWithHash]](limit, func(k accounts.CodeHash) uint32 {
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

func (c *codeCache) Get(key accounts.Address) (value ValueWithStep[codeWithHash], ok bool) {
	h, ok := c.hashes.Get(key)

	if !ok {
		return ValueWithStep[codeWithHash]{}, false
	}

	if h.IsEmpty() {
		return ValueWithStep[codeWithHash]{Value: codeWithHash{hash: h}}, true
	}

	pc, ok := c.code.Get(h)

	if !ok || pc.Value.code.Value() == nil {
		c.code.Remove(h)
		return ValueWithStep[codeWithHash]{}, false
	}

	return ValueWithStep[codeWithHash]{Value: codeWithHash{code: *pc.Value.code.Value(), hash: h}}, true
}

func (c *codeCache) Add(k accounts.Address, v codeWithHash, s kv.Step) (evicted bool) {
	h, ok := c.hashes.Get(k)

	if !ok || h != v.hash {
		evicted = c.hashes.Add(k, v.hash)
	}

	if !v.hash.IsEmpty() {
		if pv, ok := c.code.Get(h); !ok || pv.Value.code.Value() == nil {
			return c.code.Add(v.hash, ValueWithStep[weakCodeWithHash]{Value: weakCodeWithHash{code: weak.Make(&v.code), hash: v.hash}, Step: s})
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
				code:   map[accounts.CodeHash]ValueWithStep[codeWithHash]{},
			},
		},
	}, nil
}

func (cd *CodeDomain) Get(ctx context.Context, k accounts.Address, tx kv.TemporalTx) (h accounts.CodeHash, c []byte, step kv.Step, ok bool, err error) {
	v, step, ok, err := cd.domain.get(ctx, k, tx,
		func(k accounts.Address) []byte {
			av := k.Value()
			return av[:]
		},
		func(b []byte) (v codeWithHash, err error) {
			return codeWithHash{code: b, hash: accounts.InternCodeHash(crypto.Keccak256Hash(b))}, nil
		})

	if !ok {
		return accounts.EmptyCodeHash, nil, step, ok, err
	}

	if v.hash.IsEmpty() {
		return v.hash, nil, step, ok, err
	}

	return v.hash, v.code, step, ok, err
}

func (cd *CodeDomain) Put(ctx context.Context, k accounts.Address, h accounts.CodeHash, c []byte, roTx kv.TemporalTx, txNum uint64, prev ...CodeWithStep) error {
	if c == nil {
		return fmt.Errorf("domain: %s, trying to put nil value. not allowed", kv.CodeDomain)
	}

	cd.commitCtx.TouchCode(k, c)

	var pv *ValueWithStep[codeWithHash]
	if len(prev) != 0 {
		pv = &ValueWithStep[codeWithHash]{
			Value: codeWithHash{code: prev[0].Code, hash: prev[0].Hash},
			Step:  prev[0].Step,
		}
	}

	if pv != nil && bytes.Equal(c, pv.Value.code) {
		return nil
	}

	if h.IsNil() {
		h = accounts.InternCodeHash(crypto.Keccak256Hash(c))
	}

	return cd.domain.put(ctx, k, codeWithHash{code: c, hash: h},
		func(k accounts.Address) []byte {
			kv := k.Value()
			return kv[:]
		},
		func(v0 codeWithHash, v1 codeWithHash) bool {
			return bytes.Equal(v0.code, v1.code)
		},
		func(v codeWithHash) []byte {
			return v.code
		},
		func(b []byte) (v codeWithHash, err error) {
			return codeWithHash{code: b, hash: accounts.InternCodeHash(crypto.Keccak256Hash(b))}, nil
		},
		roTx, txNum, pv)
}

func (cd *CodeDomain) Del(ctx context.Context, k accounts.Address, roTx kv.TemporalTx, txNum uint64, prev ...CodeWithStep) error {
	cd.commitCtx.TouchCode(k, nil)

	var pv *ValueWithStep[codeWithHash]
	if len(prev) != 0 {
		pv = &ValueWithStep[codeWithHash]{
			Value: codeWithHash{code: prev[0].Code, hash: prev[0].Hash},
			Step:  prev[0].Step,
		}
	}

	return cd.domain.del(ctx, k,
		func(k accounts.Address) []byte {
			kv := k.Value()
			return kv[:]
		},
		func(v codeWithHash) []byte {
			return v.code
		},
		func(b []byte) (v codeWithHash, err error) {
			return codeWithHash{code: b, hash: accounts.InternCodeHash(crypto.Keccak256Hash(b))}, nil
		},
		roTx, txNum, pv)
}

type CommitmentDomain struct {
	domain[commitment.Path, commitment.Branch]
}

type branchCache struct {
	state       ValueWithStep[commitment.Branch]
	t0          ValueWithStep[commitment.Branch]
	t1          [16]ValueWithStep[commitment.Branch]
	t2          [256]ValueWithStep[commitment.Branch]
	t3          [4096]ValueWithStep[commitment.Branch]
	t4          [65536]ValueWithStep[commitment.Branch]
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

func (c *branchCache) Get(key commitment.Path) (ValueWithStep[commitment.Branch], bool) {
	// see: HexNibblesToCompactBytes for encoding spec
	switch {
	case key == statePath:
		if len(c.state.Value) == 0 {
			return ValueWithStep[commitment.Branch]{}, false
		}
		return c.state, true
	case len(key.Value()) < 4:
		keyValue := key.Value()
		switch len(keyValue) {
		case 0:
			if len(c.t0.Value) > 0 {
				return c.t0, true
			}
			return ValueWithStep[commitment.Branch]{}, false
		case 1:
			value := c.t1[keyValue[0]&0x0f]
			return value, len(value.Value) > 0
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

func (c *branchCache) Add(k commitment.Path, v commitment.Branch, s kv.Step) (evicted bool) {
	// see: HexNibblesToCompactBytes for encoding spec
	switch {
	case k == statePath:
		c.state = ValueWithStep[commitment.Branch]{Value: v, Step: s}
		return false
	case len(k.Value()) < 4:
		keyValue := k.Value()
		switch len(keyValue) {
		case 0:
			c.t0 = ValueWithStep[commitment.Branch]{Value: v, Step: s}
			return false
		case 1:
			c.t1[keyValue[0]&0x0f] = ValueWithStep[commitment.Branch]{Value: v, Step: s}
			return false
		case 2:
			if keyValue[0]&0x10 == 0 {
				c.t2[keyValue[1]] = ValueWithStep[commitment.Branch]{Value: v, Step: s}
			} else {
				c.t3[uint16(keyValue[0]&0x0f)<<8|uint16(keyValue[1])] = ValueWithStep[commitment.Branch]{Value: v, Step: s}
			}
			return false
		default:
			if keyValue[0]&0x10 == 0 {
				c.t4[uint16(keyValue[1])<<8|uint16(keyValue[2])] = ValueWithStep[commitment.Branch]{Value: v, Step: s}
				return false
			}
		}

	}

	return c.branches.Add(k, v, s)
}

func (c *branchCache) Remove(k commitment.Path) (evicted bool) {
	// see: HexNibblesToCompactBytes for encoding spec
	switch {
	case k == statePath:
		evicted = len(c.state.Value) > 0
		c.state = ValueWithStep[commitment.Branch]{}
		return evicted
	case len(k.Value()) < 4:
		keyValue := k.Value()
		switch len(keyValue) {
		case 0:
			evicted = len(c.t0.Value) > 0
			c.t0 = ValueWithStep[commitment.Branch]{}
			return evicted
		case 1:
			evicted := len(c.t1[keyValue[0]&0x0f].Value) > 0
			c.t1[keyValue[0]&0x0f] = ValueWithStep[commitment.Branch]{}
			return evicted
		case 2:
			if keyValue[0]&0x10 == 0 {
				kv := keyValue[1]
				evicted := len(c.t2[kv].Value) > 0
				c.t2[kv] = ValueWithStep[commitment.Branch]{}
				return evicted
			} else {
				kv := uint16(keyValue[0]&0x0f)<<8 | uint16(keyValue[1])
				evicted := len(c.t3[kv].Value) > 0
				c.t3[kv] = ValueWithStep[commitment.Branch]{}
				return evicted
			}
		default:
			if keyValue[0]&0x10 == 0 {
				kv := uint16(keyValue[1])<<8 | uint16(keyValue[2])
				evicted := len(c.t4[kv].Value) > 0
				c.t4[kv] = ValueWithStep[commitment.Branch]{}
				return evicted
			}
		}
	}
	return c.branches.Remove(k)
}

type branchUpdates map[commitment.Path]ValueWithStep[commitment.Branch]

func (u branchUpdates) Get(k commitment.Path) (ValueWithStep[commitment.Branch], bool) {
	v, ok := u[k]
	return v, ok
}

func (u branchUpdates) Put(k commitment.Path, v ValueWithStep[commitment.Branch]) bool {
	_, exists := u[k]
	u[k] = v
	return !exists
}

func (u branchUpdates) Iter() iter.Seq2[commitment.Path, ValueWithStep[commitment.Branch]] {
	return func(yield func(commitment.Path, ValueWithStep[commitment.Branch]) bool) {
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
		},
	}, nil
}

func (cd *CommitmentDomain) GetBranch(ctx context.Context, k commitment.Path, tx kv.TemporalTx) (v commitment.Branch, step kv.Step, ok bool, err error) {
	return cd.domain.get(ctx, k, tx,
		func(k commitment.Path) []byte {
			return k.Value()
		},
		func(b []byte) (v commitment.Branch, err error) {
			return commitment.Branch(b), nil
		})
}

func (cd *CommitmentDomain) PutBranch(ctx context.Context, k commitment.Path, v commitment.Branch, roTx kv.TemporalTx, txNum uint64, prev ...ValueWithStep[commitment.Branch]) error {
	if v == nil {
		return fmt.Errorf("PutBranch: %s, trying to put nil value. not allowed", kv.CommitmentDomain)
	}

	var pv *ValueWithStep[commitment.Branch]
	if len(prev) != 0 {
		pv = &prev[0]
	}

	return cd.domain.put(ctx, k, v,
		func(k commitment.Path) []byte {
			return k.Value()
		},
		func(v0 commitment.Branch, v1 commitment.Branch) bool {
			return bytes.Equal(v0, v1)
		},
		func(v commitment.Branch) []byte {
			return v
		},
		func(b []byte) (v commitment.Branch, err error) {
			return commitment.Branch(b), nil
		},
		roTx, txNum, pv)
}

func (cd *CommitmentDomain) DelBranch(ctx context.Context, k commitment.Path, roTx kv.TemporalTx, txNum uint64, prev ...ValueWithStep[commitment.Branch]) error {
	var pv *ValueWithStep[commitment.Branch]
	if len(prev) != 0 {
		pv = &prev[0]
	}

	return cd.domain.del(ctx, k,
		func(k commitment.Path) []byte {
			return k.Value()
		},
		func(v commitment.Branch) []byte {
			return v
		},
		func(b []byte) (v commitment.Branch, err error) {
			return commitment.Branch(b), nil
		},
		roTx, txNum, pv)
}
