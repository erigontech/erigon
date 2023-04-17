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
	"hash"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/c2h5oh/datasize"
	btree2 "github.com/tidwall/btree"
	"golang.org/x/crypto/sha3"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
)

type CacheValidationResult struct {
	RequestCancelled   bool
	Enabled            bool
	LatestStateBehind  bool
	CacheCleared       bool
	LatestStateID      uint64
	StateKeysOutOfSync [][]byte
	CodeKeysOutOfSync  [][]byte
}

type Cache interface {
	// View - returns CacheView consistent with givent kv.Tx
	View(ctx context.Context, tx kv.Tx) (CacheView, error)
	OnNewBlock(sc *remote.StateChangeBatch)
	Len() int
	ValidateCurrentRoot(ctx context.Context, tx kv.Tx) (*CacheValidationResult, error)
}
type CacheView interface {
	Get(k []byte) ([]byte, error)
	GetCode(k []byte) ([]byte, error)
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

// Pair.Value == nil - is a marker of absense key in db

// Coherent
// High-level guaranties:
// - Keys/Values returned by cache are valid/immutable until end of db transaction
// - CacheView is always coherent with given db transaction -
//
// Rules of set view.isCanonical value:
//   - method View can't parent.Clone() - because parent view is not coherent with current kv.Tx
//   - only OnNewBlock method may do parent.Clone() and apply StateChanges to create coherent view of kv.Tx
//   - parent.Clone() can't be caled if parent.isCanonical=false
//   - only OnNewBlock method can set view.isCanonical=true
//
// Rules of filling cache.stateEvict:
//   - changes in Canonical View SHOULD reflect in stateEvict
//   - changes in Non-Canonical View SHOULD NOT reflect in stateEvict
type Coherent struct {
	hasher               hash.Hash
	codeEvictLen         *metrics.Counter
	codeKeys             *metrics.Counter
	keys                 *metrics.Counter
	evict                *metrics.Counter
	latestStateView      *CoherentRoot
	codeMiss             *metrics.Counter
	timeout              *metrics.Counter
	hits                 *metrics.Counter
	codeHits             *metrics.Counter
	roots                map[uint64]*CoherentRoot
	stateEvict           *ThreadSafeEvictionList
	codeEvict            *ThreadSafeEvictionList
	miss                 *metrics.Counter
	cfg                  CoherentConfig
	latestStateVersionID uint64
	lock                 sync.Mutex
	waitExceededCount    atomic.Int32 // used as a circuit breaker to stop the cache waiting for new blocks
}

type CoherentRoot struct {
	cache           *btree2.BTreeG[*Element]
	codeCache       *btree2.BTreeG[*Element]
	ready           chan struct{} // close when ready
	readyChanClosed atomic.Bool   // protecting `ready` field from double-close (on unwind). Consumers don't need check this field.

	// Views marked as `Canonical` if it received onNewBlock message
	// we may drop `Non-Canonical` views even if they had fresh keys
	// keys added to `Non-Canonical` views SHOULD NOT be added to stateEvict
	// cache.latestStateView is always `Canonical`
	isCanonical bool
}

// CoherentView - dumb object, which proxy all requests to Coherent object.
// It's thread-safe, because immutable
type CoherentView struct {
	tx             kv.Tx
	cache          *Coherent
	stateVersionID uint64
}

func (c *CoherentView) Get(k []byte) ([]byte, error) { return c.cache.Get(k, c.tx, c.stateVersionID) }
func (c *CoherentView) GetCode(k []byte) ([]byte, error) {
	return c.cache.GetCode(k, c.tx, c.stateVersionID)
}

var _ Cache = (*Coherent)(nil)         // compile-time interface check
var _ CacheView = (*CoherentView)(nil) // compile-time interface check

const (
	DEGREE    = 32
	MAX_WAITS = 100
)

type CoherentConfig struct {
	CacheSize       datasize.ByteSize
	CodeCacheSize   datasize.ByteSize
	WaitForNewBlock bool // should we wait 10ms for a new block message to arrive when calling View?
	WithStorage     bool
	MetricsLabel    string
	NewBlockWait    time.Duration // how long wait
	KeepViews       uint64        // keep in memory up to this amount of views, evict older
}

var DefaultCoherentConfig = CoherentConfig{
	KeepViews:       5,
	NewBlockWait:    5 * time.Millisecond,
	CacheSize:       2 * datasize.GB,
	CodeCacheSize:   2 * datasize.GB,
	MetricsLabel:    "default",
	WithStorage:     true,
	WaitForNewBlock: true,
}

func New(cfg CoherentConfig) *Coherent {
	if cfg.KeepViews == 0 {
		panic("empty config passed")
	}

	return &Coherent{
		roots:        map[uint64]*CoherentRoot{},
		stateEvict:   &ThreadSafeEvictionList{l: NewList()},
		codeEvict:    &ThreadSafeEvictionList{l: NewList()},
		hasher:       sha3.NewLegacyKeccak256(),
		cfg:          cfg,
		miss:         metrics.GetOrCreateCounter(fmt.Sprintf(`cache_total{result="miss",name="%s"}`, cfg.MetricsLabel)),
		hits:         metrics.GetOrCreateCounter(fmt.Sprintf(`cache_total{result="hit",name="%s"}`, cfg.MetricsLabel)),
		timeout:      metrics.GetOrCreateCounter(fmt.Sprintf(`cache_timeout_total{name="%s"}`, cfg.MetricsLabel)),
		keys:         metrics.GetOrCreateCounter(fmt.Sprintf(`cache_keys_total{name="%s"}`, cfg.MetricsLabel)),
		evict:        metrics.GetOrCreateCounter(fmt.Sprintf(`cache_list_total{name="%s"}`, cfg.MetricsLabel)),
		codeMiss:     metrics.GetOrCreateCounter(fmt.Sprintf(`cache_code_total{result="miss",name="%s"}`, cfg.MetricsLabel)),
		codeHits:     metrics.GetOrCreateCounter(fmt.Sprintf(`cache_code_total{result="hit",name="%s"}`, cfg.MetricsLabel)),
		codeKeys:     metrics.GetOrCreateCounter(fmt.Sprintf(`cache_code_keys_total{name="%s"}`, cfg.MetricsLabel)),
		codeEvictLen: metrics.GetOrCreateCounter(fmt.Sprintf(`cache_code_list_total{name="%s"}`, cfg.MetricsLabel)),
	}
}

// selectOrCreateRoot - used for usual getting root
func (c *Coherent) selectOrCreateRoot(versionID uint64) *CoherentRoot {
	c.lock.Lock()
	defer c.lock.Unlock()
	r, ok := c.roots[versionID]
	if ok {
		return r
	}

	r = &CoherentRoot{
		ready:     make(chan struct{}),
		cache:     btree2.NewBTreeG[*Element](Less),
		codeCache: btree2.NewBTreeG[*Element](Less),
	}
	c.roots[versionID] = r
	return r
}

// advanceRoot - used for advancing root onNewBlock
func (c *Coherent) advanceRoot(stateVersionID uint64) (r *CoherentRoot) {
	r, rootExists := c.roots[stateVersionID]

	// if nothing has progressed just return the existing root
	if c.latestStateVersionID == stateVersionID && rootExists {
		return r
	}

	if !rootExists {
		r = &CoherentRoot{ready: make(chan struct{})}
		c.roots[stateVersionID] = r
	}

	if prevView, ok := c.roots[stateVersionID-1]; ok && prevView.isCanonical {
		//log.Info("advance: clone", "from", viewID-1, "to", viewID)
		r.cache = prevView.cache.Copy()
		r.codeCache = prevView.codeCache.Copy()
	} else {
		c.stateEvict.Init()
		c.codeEvict.Init()
		if r.cache == nil {
			//log.Info("advance: new", "to", viewID)
			r.cache = btree2.NewBTreeG[*Element](Less)
			r.codeCache = btree2.NewBTreeG[*Element](Less)
		} else {
			r.cache.Walk(func(items []*Element) bool {
				for _, i := range items {
					c.stateEvict.PushFront(i)
				}
				return true
			})
			r.codeCache.Walk(func(items []*Element) bool {
				for _, i := range items {
					c.codeEvict.PushFront(i)
				}
				return true
			})
		}
	}
	r.isCanonical = true

	c.evictRoots()
	c.latestStateVersionID = stateVersionID
	c.latestStateView = r

	c.keys.Set(uint64(c.latestStateView.cache.Len()))
	c.codeKeys.Set(uint64(c.latestStateView.codeCache.Len()))
	c.evict.Set(uint64(c.stateEvict.Len()))
	c.codeEvictLen.Set(uint64(c.codeEvict.Len()))
	return r
}

func (c *Coherent) OnNewBlock(stateChanges *remote.StateChangeBatch) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.waitExceededCount.Store(0) // reset the circuit breaker
	id := stateChanges.StateVersionId
	r := c.advanceRoot(id)
	for _, sc := range stateChanges.ChangeBatch {
		for i := range sc.Changes {
			switch sc.Changes[i].Action {
			case remote.Action_UPSERT:
				addr := gointerfaces.ConvertH160toAddress(sc.Changes[i].Address)
				v := sc.Changes[i].Data
				//fmt.Printf("set: %x,%x\n", addr, v)
				c.add(addr[:], v, r, id)
			case remote.Action_UPSERT_CODE:
				addr := gointerfaces.ConvertH160toAddress(sc.Changes[i].Address)
				v := sc.Changes[i].Data
				c.add(addr[:], v, r, id)
				c.hasher.Reset()
				c.hasher.Write(sc.Changes[i].Code)
				k := make([]byte, 32)
				c.hasher.Sum(k)
				c.addCode(k, sc.Changes[i].Code, r, id)
			case remote.Action_REMOVE:
				addr := gointerfaces.ConvertH160toAddress(sc.Changes[i].Address)
				c.add(addr[:], nil, r, id)
			case remote.Action_STORAGE:
				//skip, will check later
			case remote.Action_CODE:
				c.hasher.Reset()
				c.hasher.Write(sc.Changes[i].Code)
				k := make([]byte, 32)
				c.hasher.Sum(k)
				c.addCode(k, sc.Changes[i].Code, r, id)
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
					c.add(k, change.Data, r, id)
				}
			}
		}
	}

	switched := r.readyChanClosed.CompareAndSwap(false, true)
	if switched {
		close(r.ready) //broadcast
	}
	//log.Info("on new block handled", "viewID", stateChanges.StateVersionID)
}

func (c *Coherent) View(ctx context.Context, tx kv.Tx) (CacheView, error) {
	idBytes, err := tx.GetOne(kv.Sequence, kv.PlainStateVersion)
	if err != nil {
		return nil, err
	}
	var id uint64
	if len(idBytes) == 0 {
		id = 0
	} else {
		id = binary.BigEndian.Uint64(idBytes)
	}
	r := c.selectOrCreateRoot(id)

	if !c.cfg.WaitForNewBlock || c.waitExceededCount.Load() >= MAX_WAITS {
		return &CoherentView{stateVersionID: id, tx: tx, cache: c}, nil
	}

	select { // fast non-blocking path
	case <-r.ready:
		//fmt.Printf("recv broadcast: %d\n", id)
		return &CoherentView{stateVersionID: id, tx: tx, cache: c}, nil
	default:
	}

	select { // slow blocking path
	case <-r.ready:
		//fmt.Printf("recv broadcast2: %d\n", tx.ViewID())
	case <-ctx.Done():
		return nil, fmt.Errorf("kvcache rootNum=%x, %w", tx.ViewID(), ctx.Err())
	case <-time.After(c.cfg.NewBlockWait): //TODO: switch to timer to save resources
		c.timeout.Inc()
		c.waitExceededCount.Add(1)
		//log.Info("timeout", "db_id", id, "has_btree", r.cache != nil)
	}
	return &CoherentView{stateVersionID: id, tx: tx, cache: c}, nil
}

func (c *Coherent) getFromCache(k []byte, id uint64, code bool) (*Element, *CoherentRoot, error) {
	// using the full lock here rather than RLock as RLock causes a lot of calls to runtime.usleep degrading
	// performance under load
	c.lock.Lock()
	defer c.lock.Unlock()
	r, ok := c.roots[id]
	if !ok {
		return nil, r, fmt.Errorf("too old ViewID: %d, latestStateVersionID=%d", id, c.latestStateVersionID)
	}
	isLatest := c.latestStateVersionID == id

	var it *Element
	if code {
		it, _ = r.codeCache.Get(&Element{K: k})
	} else {
		it, _ = r.cache.Get(&Element{K: k})
	}
	if it != nil && isLatest {
		c.stateEvict.MoveToFront(it)
	}

	return it, r, nil
}
func (c *Coherent) Get(k []byte, tx kv.Tx, id uint64) ([]byte, error) {
	it, r, err := c.getFromCache(k, id, false)
	if err != nil {
		return nil, err
	}

	if it != nil {
		//fmt.Printf("from cache:  %#x,%x\n", k, it.(*Element).V)
		c.hits.Inc()
		return it.V, nil
	}
	c.miss.Inc()

	v, err := tx.GetOne(kv.PlainState, k)
	if err != nil {
		return nil, err
	}
	//fmt.Printf("from db: %#x,%x\n", k, v)

	c.lock.Lock()
	defer c.lock.Unlock()
	v = c.add(common.Copy(k), common.Copy(v), r, id).V
	return v, nil
}

func (c *Coherent) GetCode(k []byte, tx kv.Tx, id uint64) ([]byte, error) {
	it, r, err := c.getFromCache(k, id, true)
	if err != nil {
		return nil, err
	}

	if it != nil {
		//fmt.Printf("from cache:  %#x,%x\n", k, it.(*Element).V)
		c.codeHits.Inc()
		return it.V, nil
	}
	c.codeMiss.Inc()

	v, err := tx.GetOne(kv.Code, k)
	if err != nil {
		return nil, err
	}
	//fmt.Printf("from db: %#x,%x\n", k, v)

	c.lock.Lock()
	defer c.lock.Unlock()
	v = c.addCode(common.Copy(k), common.Copy(v), r, id).V
	return v, nil
}
func (c *Coherent) removeOldest(r *CoherentRoot) {
	e := c.stateEvict.Oldest()
	if e != nil {
		c.stateEvict.Remove(e)
		r.cache.Delete(e)
	}
}
func (c *Coherent) removeOldestCode(r *CoherentRoot) {
	e := c.codeEvict.Oldest()
	if e != nil {
		c.codeEvict.Remove(e)
		r.codeCache.Delete(e)
	}
}
func (c *Coherent) add(k, v []byte, r *CoherentRoot, id uint64) *Element {
	it := &Element{K: k, V: v}
	replaced, _ := r.cache.Set(it)
	if c.latestStateVersionID != id {
		//fmt.Printf("add to non-last viewID: %d<%d\n", c.latestViewID, id)
		return it
	}
	if replaced != nil {
		c.stateEvict.Remove(replaced)
	}
	c.stateEvict.PushFront(it)

	// clear down cache until size below the configured limit
	for c.stateEvict.Size() > int(c.cfg.CacheSize.Bytes()) {
		c.removeOldest(r)
	}

	return it
}
func (c *Coherent) addCode(k, v []byte, r *CoherentRoot, id uint64) *Element {
	it := &Element{K: k, V: v}
	replaced, _ := r.codeCache.Set(it)
	if c.latestStateVersionID != id {
		//fmt.Printf("add to non-last viewID: %d<%d\n", c.latestViewID, id)
		return it
	}
	if replaced != nil {
		c.codeEvict.Remove(replaced)
	}
	c.codeEvict.PushFront(it)

	for c.codeEvict.Size() > int(c.cfg.CodeCacheSize.Bytes()) {
		c.removeOldestCode(r)
	}

	return it
}

func (c *Coherent) ValidateCurrentRoot(ctx context.Context, tx kv.Tx) (*CacheValidationResult, error) {

	result := &CacheValidationResult{
		Enabled:          true,
		RequestCancelled: false,
	}

	select {
	case <-ctx.Done():
		result.RequestCancelled = true
		return result, nil
	default:
	}

	idBytes, err := tx.GetOne(kv.Sequence, kv.PlainStateVersion)
	if err != nil {
		return nil, err
	}
	stateID := binary.BigEndian.Uint64(idBytes)
	result.LatestStateID = stateID

	// if the latest view id in the cache is not the same as the tx or one below it
	// then the cache will be a new one for the next call so return early
	if stateID > c.latestStateVersionID {
		result.LatestStateBehind = true
		return result, nil
	}

	root := c.selectOrCreateRoot(c.latestStateVersionID)

	// ensure the root is ready or wait and press on
	select {
	case <-root.ready:
	case <-time.After(c.cfg.NewBlockWait):
	}

	// check context again after potentially waiting for root to be ready
	select {
	case <-ctx.Done():
		result.RequestCancelled = true
		return result, nil
	default:
	}

	clearCache := false

	compare := func(cache *btree2.BTreeG[*Element], bucket string) (bool, [][]byte, error) {
		keys := make([][]byte, 0)

		for {
			val, ok := cache.PopMax()
			if !ok {
				break
			}

			// check the db
			inDb, err := tx.GetOne(bucket, val.K)
			if err != nil {
				return false, keys, err
			}

			if !bytes.Equal(inDb, val.V) {
				keys = append(keys, val.K)
				clearCache = true
			}

			select {
			case <-ctx.Done():
				return true, keys, nil
			default:
			}
		}

		return false, keys, nil
	}

	cache, codeCache := c.cloneCaches(root)

	cancelled, keys, err := compare(cache, kv.PlainState)
	if err != nil {
		return nil, err
	}
	result.StateKeysOutOfSync = keys
	if cancelled {
		result.RequestCancelled = true
		return result, nil
	}

	cancelled, keys, err = compare(codeCache, kv.Code)
	if err != nil {
		return nil, err
	}
	result.CodeKeysOutOfSync = keys
	if cancelled {
		result.RequestCancelled = true
		return result, nil
	}

	if clearCache {
		c.clearCaches(root)
	}
	result.CacheCleared = clearCache

	return result, nil
}

func (c *Coherent) cloneCaches(r *CoherentRoot) (cache *btree2.BTreeG[*Element], codeCache *btree2.BTreeG[*Element]) {
	c.lock.Lock()
	defer c.lock.Unlock()
	cache = r.cache.Copy()
	codeCache = r.codeCache.Copy()
	return cache, codeCache
}

func (c *Coherent) clearCaches(r *CoherentRoot) {
	c.lock.Lock()
	defer c.lock.Unlock()
	r.cache.Clear()
	r.codeCache.Clear()
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
	casted.lock.Lock()
	for root, r := range casted.roots {
		res = append(res, Stat{
			BlockNum: root,
			Lenght:   r.cache.Len(),
		})
	}
	casted.lock.Unlock()
	sort.Slice(res, func(i, j int) bool { return res[i].BlockNum < res[j].BlockNum })
	return res
}
func AssertCheckValues(ctx context.Context, tx kv.Tx, cache Cache) (int, error) {
	defer func(t time.Time) { fmt.Printf("AssertCheckValues:327: %s\n", time.Since(t)) }(time.Now())
	view, err := cache.View(ctx, tx)
	if err != nil {
		return 0, err
	}
	castedView, ok := view.(*CoherentView)
	if !ok {
		return 0, nil
	}
	casted, ok := cache.(*Coherent)
	if !ok {
		return 0, nil
	}
	checked := 0
	casted.lock.Lock()
	defer casted.lock.Unlock()
	//log.Info("AssertCheckValues start", "db_id", tx.ViewID(), "mem_id", casted.id.Load(), "len", casted.cache.Len())
	root, ok := casted.roots[castedView.stateVersionID]
	if !ok {
		return 0, nil
	}
	root.cache.Walk(func(items []*Element) bool {
		for _, i := range items {
			k, v := i.K, i.V
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
		}
		return true
	})
	return checked, err
}
func (c *Coherent) evictRoots() {
	if c.latestStateVersionID <= c.cfg.KeepViews {
		return
	}
	if len(c.roots) < int(c.cfg.KeepViews) {
		return
	}
	to := c.latestStateVersionID - c.cfg.KeepViews
	toDel := make([]uint64, 0, len(c.roots))
	for txID := range c.roots {
		if txID > to {
			continue
		}
		toDel = append(toDel, txID)
	}
	//log.Info("forget old roots", "list", fmt.Sprintf("%d", toDel))
	for _, txID := range toDel {
		delete(c.roots, txID)
	}
}
func (c *Coherent) Len() int {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.latestStateView == nil {
		return 0
	}
	return c.latestStateView.cache.Len() //todo: is it same with cache.len()?
}

// Element is an element of a linked list.
type Element struct {
	// Next and previous pointers in the doubly-linked list of elements.
	// To simplify the implementation, internally a list l is implemented
	// as a ring, such that &l.root is both the next element of the last
	// list element (l.Back()) and the previous element of the first list
	// element (l.Front()).
	next, prev *Element

	// The list to which this element belongs.
	list *List

	// The value stored with this element.
	K, V []byte
}

func (e *Element) Size() int { return len(e.K) + len(e.V) }

func Less(a, b *Element) bool { return bytes.Compare(a.K, b.K) < 0 }

type ThreadSafeEvictionList struct {
	l    *List
	lock sync.Mutex
}

func (l *ThreadSafeEvictionList) Init() {
	l.lock.Lock()
	l.l.Init()
	l.lock.Unlock()
}
func (l *ThreadSafeEvictionList) PushFront(e *Element) {
	l.lock.Lock()
	l.l.PushFront(e)
	l.lock.Unlock()
}

func (l *ThreadSafeEvictionList) MoveToFront(e *Element) {
	l.lock.Lock()
	l.l.MoveToFront(e)
	l.lock.Unlock()
}

func (l *ThreadSafeEvictionList) Remove(e *Element) {
	l.lock.Lock()
	l.l.Remove(e)
	l.lock.Unlock()
}

func (l *ThreadSafeEvictionList) Oldest() *Element {
	l.lock.Lock()
	e := l.l.Back()
	l.lock.Unlock()
	return e
}

func (l *ThreadSafeEvictionList) Len() int {
	l.lock.Lock()
	length := l.l.Len()
	l.lock.Unlock()
	return length
}

func (l *ThreadSafeEvictionList) Size() int {
	l.lock.Lock()
	size := l.l.Size()
	l.lock.Unlock()
	return size
}

// ========= copypaste of List implementation from stdlib ========

// Next returns the next list element or nil.
func (e *Element) Next() *Element {
	if p := e.next; e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}

// Prev returns the previous list element or nil.
func (e *Element) Prev() *Element {
	if p := e.prev; e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}

// List represents a doubly linked list.
// The zero value for List is an empty list ready to use.
type List struct {
	root Element // sentinel list element, only &root, root.prev, and root.next are used
	len  int     // current list length excluding (this) sentinel element
	size int     // size of items in list in bytes
}

// Init initializes or clears list l.
func (l *List) Init() *List {
	l.root.next = &l.root
	l.root.prev = &l.root
	l.len = 0
	l.size = 0
	return l
}

// New returns an initialized list.
func NewList() *List { return new(List).Init() }

// Len returns the number of elements of list l.
// The complexity is O(1).
func (l *List) Len() int { return l.len }

// Size returns the size of the elements in the list by bytes
func (l *List) Size() int { return l.size }

// Front returns the first element of list l or nil if the list is empty.
func (l *List) Front() *Element {
	if l.len == 0 {
		return nil
	}
	return l.root.next
}

// Back returns the last element of list l or nil if the list is empty.
func (l *List) Back() *Element {
	if l.len == 0 {
		return nil
	}
	return l.root.prev
}

// lazyInit lazily initializes a zero List value.
func (l *List) lazyInit() {
	if l.root.next == nil {
		l.Init()
	}
}

// insert inserts e after at, increments l.len, and returns e.
func (l *List) insert(e, at *Element) *Element {
	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e
	e.list = l
	l.len++
	l.size += e.Size()
	return e
}

// insertValue is a convenience wrapper for insert(&Element{Value: v}, at).
func (l *List) insertValue(e, at *Element) *Element {
	return l.insert(e, at)
}

// remove removes e from its list, decrements l.len, and returns e.
func (l *List) remove(e *Element) *Element {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.next = nil // avoid memory leaks
	e.prev = nil // avoid memory leaks
	e.list = nil
	l.len--
	l.size -= e.Size()
	return e
}

// move moves e to next to at and returns e.
func (l *List) move(e, at *Element) *Element {
	if e == at {
		return e
	}
	e.prev.next = e.next
	e.next.prev = e.prev

	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e

	return e
}

// Remove removes e from l if e is an element of list l.
// It returns the element value e.Value.
// The element must not be nil.
func (l *List) Remove(e *Element) ([]byte, []byte) {
	if e.list == l {
		// if e.list == l, l must have been initialized when e was inserted
		// in l or l == nil (e is a zero Element) and l.remove will crash
		l.remove(e)
	}
	return e.K, e.V
}

// PushFront inserts a new element e with value v at the front of list l and returns e.
func (l *List) PushFront(e *Element) *Element {
	l.lazyInit()
	return l.insertValue(e, &l.root)
}

// PushBack inserts a new element e with value v at the back of list l and returns e.
func (l *List) PushBack(e *Element) *Element {
	l.lazyInit()
	return l.insertValue(e, l.root.prev)
}

// InsertBefore inserts a new element e with value v immediately before mark and returns e.
// If mark is not an element of l, the list is not modified.
// The mark must not be nil.
func (l *List) InsertBefore(e *Element, mark *Element) *Element {
	if mark.list != l {
		return nil
	}
	// see comment in List.Remove about initialization of l
	return l.insertValue(e, mark.prev)
}

// InsertAfter inserts a new element e with value v immediately after mark and returns e.
// If mark is not an element of l, the list is not modified.
// The mark must not be nil.
func (l *List) InsertAfter(e *Element, mark *Element) *Element {
	if mark.list != l {
		return nil
	}
	// see comment in List.Remove about initialization of l
	return l.insertValue(e, mark)
}

// MoveToFront moves element e to the front of list l.
// If e is not an element of l, the list is not modified.
// The element must not be nil.
func (l *List) MoveToFront(e *Element) {
	if e.list != l || l.root.next == e {
		return
	}
	// see comment in List.Remove about initialization of l
	l.move(e, &l.root)
}

// MoveToBack moves element e to the back of list l.
// If e is not an element of l, the list is not modified.
// The element must not be nil.
func (l *List) MoveToBack(e *Element) {
	if e.list != l || l.root.prev == e {
		return
	}
	// see comment in List.Remove about initialization of l
	l.move(e, l.root.prev)
}

// MoveBefore moves element e to its new position before mark.
// If e or mark is not an element of l, or e == mark, the list is not modified.
// The element and mark must not be nil.
func (l *List) MoveBefore(e, mark *Element) {
	if e.list != l || e == mark || mark.list != l {
		return
	}
	l.move(e, mark.prev)
}

// MoveAfter moves element e to its new position after mark.
// If e or mark is not an element of l, or e == mark, the list is not modified.
// The element and mark must not be nil.
func (l *List) MoveAfter(e, mark *Element) {
	if e.list != l || e == mark || mark.list != l {
		return
	}
	l.move(e, mark)
}
