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
	"go.uber.org/atomic"
)

type Cache interface {
	// View - returns CacheView consistent with givent kv.Tx
	View(ctx context.Context, tx kv.Tx) (CacheView, error)
	OnNewBlock(sc *remote.StateChange)
	Evict()
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
	hits, miss *metrics.Counter
	evict      *metrics.Summary
	roots      map[string]*CoherentView
	rootsLock  sync.RWMutex
	cfg        CoherentCacheConfig
}
type CoherentView struct {
	hits, miss      *metrics.Counter
	cache           *btree.BTree
	lock            sync.RWMutex
	id              string
	ready           chan struct{} // close when ready
	readyChanClosed atomic.Bool   // protecting `ready` field from double-close (on unwind). Consumers don't need check this field.
}

var _ Cache = (*Coherent)(nil)         // compile-time interface check
var _ CacheView = (*CoherentView)(nil) // compile-time interface check
type Pair struct {
	K, V []byte
	t    uint64
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
	return &Coherent{roots: map[string]*CoherentView{}, cfg: cfg,
		miss:  metrics.GetOrCreateCounter(fmt.Sprintf(`cache_total{result="miss",name="%s"}`, cfg.MetricsLabel)),
		hits:  metrics.GetOrCreateCounter(fmt.Sprintf(`cache_total{result="hit",name="%s"}`, cfg.MetricsLabel)),
		evict: metrics.GetOrCreateSummary(fmt.Sprintf(`cache_evict{name="%s"}`, cfg.MetricsLabel)),
	}
}

// selectOrCreateRoot - used for usual getting root
func (c *Coherent) selectOrCreateRoot(root string) *CoherentView {
	c.rootsLock.Lock()
	defer c.rootsLock.Unlock()
	r, ok := c.roots[root]
	if ok {
		return r
	}
	r = &CoherentView{id: root, ready: make(chan struct{}), hits: c.hits, miss: c.miss}
	c.roots[root] = r
	return r
}

// advanceRoot - used for advancing root onNewBlock
func (c *Coherent) advanceRoot(root, prevRoot string, direction remote.Direction) (r *CoherentView, fastUnwind bool) {
	c.rootsLock.Lock()
	defer c.rootsLock.Unlock()
	r, rootExists := c.roots[root]
	if !rootExists {
		r = &CoherentView{id: root, ready: make(chan struct{}), hits: c.hits, miss: c.miss}
		c.roots[root] = r
	}

	//TODO: need check if c.latest hash is still canonical. If not - can't clone from it
	switch direction {
	case remote.Direction_FORWARD:
		prevCacheRoot, prevRootExists := c.roots[prevRoot]
		//log.Warn("[kvcache] forward", "to", fmt.Sprintf("%x", root), "prevRootExists", prevRootExists)
		if prevRootExists {
			//fmt.Printf("advance: clone %x to %x \n", prevRoot, root)
			r.cache = prevCacheRoot.Clone()
		} else {
			//fmt.Printf("advance: new %x \n", root)
			r.cache = btree.New(32)
		}
	case remote.Direction_UNWIND:
		//log.Warn("[kvcache] unwind", "to", fmt.Sprintf("%x", root), "rootExists", rootExists)
		if rootExists {
			fastUnwind = true
		} else {
			r.cache = btree.New(32)
		}
	default:
		panic("not implemented yet")
	}
	return r, fastUnwind
}

func (c *Coherent) OnNewBlock(sc *remote.StateChange) {
	prevRoot := make([]byte, 40)
	binary.BigEndian.PutUint64(prevRoot, sc.PrevBlockHeight)
	prevH := gointerfaces.ConvertH256ToHash(sc.PrevBlockHash)
	copy(prevRoot[8:], prevH[:])

	root := make([]byte, 40)
	binary.BigEndian.PutUint64(root, sc.BlockHeight)
	h := gointerfaces.ConvertH256ToHash(sc.BlockHash)
	copy(root[8:], h[:])
	r, _ := c.advanceRoot(string(root), string(prevRoot), sc.Direction)
	r.lock.Lock()
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
	r.lock.Unlock()
	switched := r.readyChanClosed.CAS(false, true)
	if switched {
		close(r.ready) //broadcast
	}
}

func (c *Coherent) View(ctx context.Context, tx kv.Tx) (CacheView, error) {
	//TODO: handle case when db has no records
	encBlockNum, err := tx.GetOne(kv.SyncStageProgress, []byte("Finish"))
	if err != nil {
		return nil, err
	}
	blockHash, err := tx.GetOne(kv.HeaderCanonical, encBlockNum)
	if err != nil {
		return nil, err
	}
	root := make([]byte, 8+32)
	copy(root, encBlockNum)
	copy(root[8:], blockHash)

	r := c.selectOrCreateRoot(string(root))
	select { // fast non-blocking path
	case <-r.ready:
		//fmt.Printf("recv broadcast: %x\n", root)
		return r, nil
	default:
	}

	select { // slow blocking path
	case <-r.ready:
	case <-ctx.Done():
		return nil, fmt.Errorf("kvcache rootNum=%x, %w", root, ctx.Err())
	case <-time.After(c.cfg.NewBlockWait): //TODO: switch to timer to save resources
		//fmt.Printf("timeout! %x\n", root)
		r.lock.Lock()
		if r.cache == nil {
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
		goatomic.StoreUint64(&it.(*Pair).t, binary.BigEndian.Uint64([]byte(c.id)))
		//fmt.Printf("from cache %x: %#x,%#v\n", c.id, k, it.(*Pair).V)
		return it.(*Pair).V, nil
	}
	c.miss.Inc()

	v, err := tx.GetOne(kv.PlainState, k)
	if err != nil {
		return nil, err
	}
	//fmt.Printf("from db %x: %#x,%#v\n", c.id, k, v)

	it = &Pair{K: k, V: common.Copy(v), t: binary.BigEndian.Uint64([]byte(c.id))}
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
		h := [32]byte{}
		copy(h[:], root[8:])
		res = append(res, Stat{
			BlockNum:  binary.BigEndian.Uint64([]byte(root[:8])),
			BlockHash: h,
			Lenght:    r.Len(),
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
	_, latestView := cache.(*Coherent).evictionInfo()
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
	casted.cache.Ascend(func(i btree.Item) bool {
		k, v := i.(*Pair).K, i.(*Pair).V
		var dbV []byte
		dbV, err = tx.GetOne(kv.PlainState, k)
		if err != nil {
			return false
		}
		if !bytes.Equal(dbV, v) {
			err = fmt.Errorf("key: %x, has different values: %x != %x", k, v, dbV)
			return false
		}
		checked++
		return true
	})
	return checked, err
}

func (c *Coherent) evictionInfo() (latestBlockNum uint64, view *CoherentView) {
	c.rootsLock.RLock()
	defer c.rootsLock.RUnlock()
	var latestRoot string
	for root := range c.roots { // max
		blockNum := binary.BigEndian.Uint64([]byte(root))
		if blockNum > latestBlockNum {
			latestBlockNum = blockNum
			latestRoot = root
		}
	}
	return latestBlockNum, c.roots[latestRoot]
}
func (c *Coherent) evictRoots(to uint64) {
	c.rootsLock.Lock()
	defer c.rootsLock.Unlock()
	var toDel []string
	for root := range c.roots {
		blockNum := binary.BigEndian.Uint64([]byte(root))
		if blockNum > to {
			continue
		}
		toDel = append(toDel, root)
	}

	for _, root := range toDel {
		delete(c.roots, root)
	}
}
func (c *Coherent) Evict() {
	defer c.evict.UpdateDuration(time.Now())
	latestBlockNum, preLatestRoot := c.evictionInfo()
	c.evictRoots(latestBlockNum - 10)
	if preLatestRoot != nil {
		preLatestRoot.evict(100, 200_000)
	}
}

func (c *CoherentView) evict(dropOlder uint64, keysLimit int) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var toDel []btree.Item
	var fst, snd btree.Item
	i := 0
	if c.cache.Len() < keysLimit {
		return
	}

	counters := map[uint64]int{}

	c.cache.Ascend(func(it btree.Item) bool {
		age := goatomic.LoadUint64(&it.(*Pair).t)
		if age < dropOlder {
			toDel = append(toDel, fst)
		}
		return true
	})
	for _, it := range toDel {
		c.cache.Delete(it)
	}
	fmt.Printf("drop too old: %d\n", len(toDel))

	firstPrime, secondPrime := 11, 13 // to choose 2-pseudo-random elements and evict worse one
	c.cache.Ascend(func(it btree.Item) bool {
		age := goatomic.LoadUint64(&it.(*Pair).t)
		if age < dropOlder {
			toDel = append(toDel, fst)
			return true
		}
		_, ok := counters[age]
		if !ok {
			counters[age] = 0
		}
		counters[age]++
		i++
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
	fmt.Printf("drop 2-random: %d\n", len(toDel))
}
