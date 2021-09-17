/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package kvcache

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	goatomic "sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/google/btree"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"
	"go.uber.org/atomic"
)

type Cache interface {
	// View - returns CacheView consistent with givent kv.Tx
	View(ctx context.Context, tx kv.Tx) (CacheView, error)
	OnNewBlock(sc *remote.StateChangeBatch)
	Evict() int
}
type CacheView interface {
	Get(k []byte, tx kv.Tx) ([]byte, error)
}

// Coherent works on top of Database Transaction and pair Coherent+ReadTransaction must
// provide "Serializable Isolation Level" semantic: all data form consistent db view at moment
// when read transaction started, read data are immutable until end of read transaction, reader can't see newer updates
//
// Every time a new state change comes, we do the following:
// - Check that prevBlockHeight and prevBlockHash match what is the top values we have, and if they don't we
// invalidate the cache, because we missed some messages and cannot consider the cache coherent anymore.
// - Clone the cache pointer (such that the previous pointer is still accessible, but new one shared the content with it),
// apply state updates to the cloned cache pointer and save under the new identified made from blockHeight and blockHash.
// - If there is a conditional variable corresponding to the identifier, remove it from the map and notify conditional
// variable, waking up the read-only transaction waiting on it.
//
// On the other hand, whenever we have a cache miss (by looking at the top cache), we do the following:
// - Once read the current block height and block hash (canonical) from underlying db transaction
// - Construct the identifier from the current block height and block hash
// - Look for the constructed identifier in the cache. If the identifier is found, use the corresponding
// cache in conjunction with this read-only transaction (it will be consistent with it). If the identifier is
// not found, it means that the transaction has been committed in Erigon, but the state update has not
// arrived yet (as shown in the picture on the right). Insert conditional variable for this identifier and wait on
// it until either cache with the given identifier appears, or timeout (indicating that the cache update
// mechanism is broken and cache is likely invalidated).
//
// Tech details:
// - If found in cache - return value without copy (reader can rely on fact that data are immutable until end of db transaction)
// - Otherwise just read from db (no requests deduplication for now - preliminary optimization).
//
// Pair.Value == nil - is a marker of absense key in db

type Coherent struct {
	hits, miss, timeout, keys *metrics.Counter
	evict                     *metrics.Summary
	roots                     map[uint64]*CoherentView
	rootsLock                 sync.RWMutex
	cfg                       CoherentCacheConfig
}
type CoherentView struct {
	hits, miss      *metrics.Counter
	cache           *btree.BTree
	lock            sync.RWMutex
	id              atomic.Uint64
	ready           chan struct{} // close when ready
	readyChanClosed atomic.Bool   // protecting `ready` field from double-close (on unwind). Consumers don't need check this field.
}

var _ Cache = (*Coherent)(nil)         // compile-time interface check
var _ CacheView = (*CoherentView)(nil) // compile-time interface check
type Pair struct {
	K, V []byte
	t    uint64 //TODO: can be uint32 if remember first txID and use it as base zero. because it's monotonic.
}

func (p *Pair) Less(than btree.Item) bool { return bytes.Compare(p.K, than.(*Pair).K) < 0 }

type CoherentCacheConfig struct {
	KeepViews    uint64        // keep in memory up to this amount of views, evict older
	NewBlockWait time.Duration // how long wait
	MetricsLabel string
	WithStorage  bool
}

var DefaultCoherentCacheConfig = CoherentCacheConfig{
	KeepViews:    100,
	NewBlockWait: 50 * time.Millisecond,
	MetricsLabel: "default",
	WithStorage:  false,
}

func New(cfg CoherentCacheConfig) *Coherent {
	return &Coherent{roots: map[uint64]*CoherentView{}, cfg: cfg,
		miss:    metrics.GetOrCreateCounter(fmt.Sprintf(`cache_total{result="miss",name="%s"}`, cfg.MetricsLabel)),
		hits:    metrics.GetOrCreateCounter(fmt.Sprintf(`cache_total{result="hit",name="%s"}`, cfg.MetricsLabel)),
		timeout: metrics.GetOrCreateCounter(fmt.Sprintf(`cache_timeout_total{name="%s"}`, cfg.MetricsLabel)),
		keys:    metrics.GetOrCreateCounter(fmt.Sprintf(`cache_keys_total{name="%s"}`, cfg.MetricsLabel)),
		evict:   metrics.GetOrCreateSummary(fmt.Sprintf(`cache_evict{name="%s"}`, cfg.MetricsLabel)),
	}
}

// selectOrCreateRoot - used for usual getting root
func (c *Coherent) selectOrCreateRoot(txID uint64) *CoherentView {
	c.rootsLock.Lock()
	defer c.rootsLock.Unlock()
	r, ok := c.roots[txID]
	if ok {
		return r
	}
	r = &CoherentView{ready: make(chan struct{}), hits: c.hits, miss: c.miss}
	r.id.Store(txID)
	c.roots[txID] = r
	return r
}

// advanceRoot - used for advancing root onNewBlock
func (c *Coherent) advanceRoot(viewID uint64) (r *CoherentView) {
	c.rootsLock.Lock()
	defer c.rootsLock.Unlock()
	r, rootExists := c.roots[viewID]
	if !rootExists {
		r = &CoherentView{ready: make(chan struct{}), hits: c.hits, miss: c.miss}
		r.id.Store(viewID)
		c.roots[viewID] = r
	}

	//TODO: need check if c.latest hash is still canonical. If not - can't clone from it
	if prevView, ok := c.roots[viewID-1]; ok {
		//log.Info("advance: clone", "from", viewID-1, "to", viewID)
		r.cache = prevView.Clone()
	} else {
		if r.cache == nil {
			//log.Info("advance: new", "to", viewID)
			r.cache = btree.New(32)
		}
	}
	return r
}

func (c *Coherent) OnNewBlock(stateChanges *remote.StateChangeBatch) {
	r := c.advanceRoot(stateChanges.DatabaseViewID)
	r.lock.Lock()
	defer r.lock.Unlock()
	for _, sc := range stateChanges.ChangeBatch {
		for i := range sc.Changes {
			switch sc.Changes[i].Action {
			case remote.Action_UPSERT, remote.Action_UPSERT_CODE:
				addr := gointerfaces.ConvertH160toAddress(sc.Changes[i].Address)
				v := sc.Changes[i].Data
				//fmt.Printf("set: %x,%x\n", addr, v)
				r.cache.ReplaceOrInsert(&Pair{K: addr[:], V: v, t: sc.BlockHeight})
			case remote.Action_DELETE:
				addr := gointerfaces.ConvertH160toAddress(sc.Changes[i].Address)
				r.cache.ReplaceOrInsert(&Pair{K: addr[:], V: nil, t: sc.BlockHeight})
			case remote.Action_CODE, remote.Action_STORAGE:
				//skip
			default:
				panic("not implemented yet")
			}
			if c.cfg.WithStorage && len(sc.Changes[i].StorageChanges) > 0 {
				addr := gointerfaces.ConvertH160toAddress(sc.Changes[i].Address)
				for _, change := range sc.Changes[i].StorageChanges {
					loc := gointerfaces.ConvertH256ToHash(change.Location)
					k := make([]byte, 20+8+32)
					copy(k, addr[:])
					binary.BigEndian.PutUint64(k[20:], sc.Changes[i].Incarnation)
					copy(k[20+8:], loc[:])
					r.cache.ReplaceOrInsert(&Pair{K: k, V: change.Data, t: sc.BlockHeight})
				}
			}
		}
	}
	switched := r.readyChanClosed.CAS(false, true)
	if switched {
		close(r.ready) //broadcast
	}
	//log.Info("on new block handled", "viewID", stateChanges.DatabaseViewID)
}

func (c *Coherent) View(ctx context.Context, tx kv.Tx) (CacheView, error) {
	r := c.selectOrCreateRoot(tx.ViewID())
	select { // fast non-blocking path
	case <-r.ready:
		//fmt.Printf("recv broadcast: %d,%d\n", r.id.Load(), tx.ViewID())
		return r, nil
	default:
	}

	select { // slow blocking path
	case <-r.ready:
		//fmt.Printf("recv broadcast2: %d,%d\n", r.id.Load(), tx.ViewID())
	case <-ctx.Done():
		return nil, fmt.Errorf("kvcache rootNum=%x, %w", tx.ViewID(), ctx.Err())
	case <-time.After(c.cfg.NewBlockWait): //TODO: switch to timer to save resources
		c.timeout.Inc()
		r.lock.Lock()
		//log.Info("timeout", "mem_id", r.id.Load(), "db_id", tx.ViewID(), "has_btree", r.cache != nil)
		if r.cache == nil {
			//parent := c.selectOrCreateRoot(tx.ViewID() - 1)
			//if parent.cache != nil {
			//	r.cache = parent.Clone()
			//} else {
			//	r.cache = btree.New(32)
			//}
			r.cache = btree.New(32)
		}
		r.lock.Unlock()
	}
	return r, nil
}

func (c *CoherentView) Get(k []byte, tx kv.Tx) ([]byte, error) {
	c.lock.RLock()
	it := c.cache.Get(&Pair{K: k})
	c.lock.RUnlock()

	if it != nil {
		c.hits.Inc()
		goatomic.StoreUint64(&it.(*Pair).t, c.id.Load())
		//fmt.Printf("from cache %x: %#x,%x\n", c.id.Load(), k, it.(*Pair).V)
		return it.(*Pair).V, nil
	}
	c.miss.Inc()

	v, err := tx.GetOne(kv.PlainState, k)
	if err != nil {
		return nil, err
	}
	//fmt.Printf("from db %x: %#x,%x\n", c.id.Load(), k, v)

	it = &Pair{K: k, V: common.Copy(v), t: c.id.Load()}
	c.lock.Lock()
	c.cache.ReplaceOrInsert(it)
	c.lock.Unlock()
	return it.(*Pair).V, nil
}
func (c *CoherentView) Len() int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.cache.Len()
}
func (c *CoherentView) Clone() *btree.BTree {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.cache == nil {
		c.cache = btree.New(32)
		return btree.New(32) // return independent tree because nothing to share
	}
	return c.cache.Clone()
}

type Stat struct {
	BlockNum  uint64
	BlockHash [32]byte
	Lenght    int
}

func DebugStats(cache Cache) []Stat {
	res := []Stat{}
	casted, ok := cache.(*Coherent)
	if !ok {
		return res
	}
	casted.rootsLock.RLock()
	defer casted.rootsLock.RUnlock()
	for root, r := range casted.roots {
		res = append(res, Stat{
			BlockNum: root,
			Lenght:   r.Len(),
		})
	}
	sort.Slice(res, func(i, j int) bool { return res[i].BlockNum < res[j].BlockNum })
	return res
}
func DebugAges(cache Cache) []Stat {
	res := []Stat{}
	casted, ok := cache.(*Coherent)
	if !ok {
		return res
	}
	casted.rootsLock.RLock()
	defer casted.rootsLock.RUnlock()
	_, latestView := cache.(*Coherent).lastRoot()
	latestView.lock.RLock()
	defer latestView.lock.RUnlock()
	counters := map[uint64]int{}
	latestView.cache.Ascend(func(it btree.Item) bool {
		age := goatomic.LoadUint64(&it.(*Pair).t)
		_, ok := counters[age]
		if !ok {
			counters[age] = 0
		}
		counters[age]++
		return true
	})
	for i, j := range counters {
		res = append(res, Stat{BlockNum: i, Lenght: j})
	}
	sort.Slice(res, func(i, j int) bool { return res[i].BlockNum < res[j].BlockNum })
	return res
}
func AssertCheckValues(ctx context.Context, tx kv.Tx, cache Cache) (int, error) {
	defer func(t time.Time) { fmt.Printf("AssertCheckValues:327: %s\n", time.Since(t)) }(time.Now())
	c, err := cache.View(ctx, tx)
	if err != nil {
		return 0, err
	}
	casted, ok := c.(*CoherentView)
	if !ok {
		return 0, nil
	}
	checked := 0
	casted.lock.RLock()
	defer casted.lock.RUnlock()
	//log.Info("AssertCheckValues start", "db_id", tx.ViewID(), "mem_id", casted.id.Load(), "len", casted.cache.Len())
	casted.cache.Ascend(func(i btree.Item) bool {
		k, v := i.(*Pair).K, i.(*Pair).V
		var dbV []byte
		dbV, err = tx.GetOne(kv.PlainState, k)
		if err != nil {
			return false
		}
		if !bytes.Equal(dbV, v) {
			err = fmt.Errorf("key: %x, has different values: %x != %x, viewID: %d", k, v, dbV, casted.id.Load())
			return false
		}
		checked++
		return true
	})
	return checked, err
}

func (c *Coherent) lastRoot() (latestTxId uint64, view *CoherentView) {
	c.rootsLock.RLock()
	defer c.rootsLock.RUnlock()
	for txID := range c.roots { // max
		if txID > latestTxId {
			latestTxId = txID
		}
	}
	return latestTxId, c.roots[latestTxId]
}
func (c *Coherent) evictRoots(to uint64) {
	c.rootsLock.Lock()
	defer c.rootsLock.Unlock()
	var toDel []uint64
	for txId := range c.roots {
		if txId > to {
			continue
		}
		toDel = append(toDel, txId)
	}
	//log.Info("forget old roots", "list", fmt.Sprintf("%d", toDel))
	for _, txId := range toDel {
		delete(c.roots, txId)
	}
}
func (c *Coherent) Evict() int {
	defer c.evict.UpdateDuration(time.Now())
	latestBlockNum, lastView := c.lastRoot()
	c.evictRoots(latestBlockNum - 10)
	keysAmount := lastView.Len()
	c.keys.Set(uint64(keysAmount))
	//if lastView != nil {
	//lastView.evictOld(100, 150_000)
	//lastView.evictNew2Random(200_000)
	//}
	return lastView.Len()
}

//nolint
func (c *CoherentView) evictOld(dropOlder uint64, keysLimit int) {
	if c.Len() < keysLimit {
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()

	var toDel []btree.Item
	c.cache.Ascend(func(it btree.Item) bool {
		age := goatomic.LoadUint64(&it.(*Pair).t)
		if age < dropOlder {
			toDel = append(toDel, it)
		}
		return true
	})
	for _, it := range toDel {
		c.cache.Delete(it)
	}
	log.Info("evicted", "too_old_amount", len(toDel))
}

//nolint
func (c *CoherentView) evictNew2Random(keysLimit int) {
	if c.Len() < keysLimit {
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	i := 0
	var toDel []btree.Item
	var fst, snd btree.Item
	firstPrime, secondPrime := 11, 13 // to choose 2-pseudo-random elements and evict worse one
	c.cache.Ascend(func(it btree.Item) bool {
		if i%firstPrime == 0 {
			fst = it
		}
		if i%secondPrime == 0 {
			snd = it
		}
		if fst != nil && snd != nil {
			if goatomic.LoadUint64(&fst.(*Pair).t) < goatomic.LoadUint64(&snd.(*Pair).t) {
				toDel = append(toDel, fst)
			} else {
				toDel = append(toDel, snd)
			}
			fst = nil
			snd = nil
		}
		return true
	})

	for _, it := range toDel {
		c.cache.Delete(it)
	}
	log.Info("evicted", "2_random__amount", len(toDel))
}
