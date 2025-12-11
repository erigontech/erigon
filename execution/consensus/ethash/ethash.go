// Copyright 2017 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

// Package ethash implements the ethash proof-of-work consensus engine.
package ethash

import (
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/erigontech/erigon-lib/common/dbg"
	dir2 "github.com/erigontech/erigon-lib/common/dir"

	"github.com/edsrzf/mmap-go"
	"github.com/hashicorp/golang-lru/v2/simplelru"

	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/consensus/ethash/ethashcfg"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

const doNotStoreCachesOnDisk = ""

var ErrInvalidDumpMagic = errors.New("invalid dump magic")

var (
	// two256 is a big integer representing 2^256
	two256 = new(big.Int).Exp(big.NewInt(2), big.NewInt(256), big.NewInt(0))

	// sharedEthash is a full instance that can be shared between multiple users.
	sharedEthashOnce sync.Once
	sharedEthash     *Ethash

	// algorithmRevision is the data structure version used for file naming.
	algorithmRevision = 23

	// dumpMagic is a dataset dump header to sanity check a data dump.
	dumpMagic = []uint32{0xbaddcafe, 0xfee1dead}
)

// sharedEthash is a full instance that can be shared between multiple users.
func GetSharedEthash() *Ethash {
	sharedEthashOnce.Do(func() {
		sharedConfig := ethashcfg.Config{
			PowMode:       ethashcfg.ModeNormal,
			CachesInMem:   3,
			DatasetsInMem: 1,
		}
		sharedEthash = New(sharedConfig, nil, false)
	})
	return sharedEthash
}

// isLittleEndian returns whether the local system is running in little or big
// endian byte order.
func isLittleEndian() bool {
	n := uint32(0x01020304)
	return *(*byte)(unsafe.Pointer(&n)) == 0x04
}

// memoryMap tries to memory map a file of uint32s for read only access.
func memoryMap(path string, lock bool) (*os.File, mmap.MMap, []uint32, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, nil, nil, err
	}
	mem, buffer, err := memoryMapFile(file, false)
	if err != nil {
		file.Close()
		return nil, nil, nil, err
	}
	for i, magic := range dumpMagic {
		if buffer[i] != magic {
			mem.Unmap()
			file.Close()
			return nil, nil, nil, ErrInvalidDumpMagic
		}
	}
	if lock {
		if err2 := mem.Lock(); err2 != nil {
			mem.Unmap() //nolint:errcheck
			file.Close()
			return nil, nil, nil, err2
		}
	}
	return file, mem, buffer[len(dumpMagic):], err
}

// memoryMapFile tries to memory map an already opened file descriptor.
func memoryMapFile(file *os.File, write bool) (mmap.MMap, []uint32, error) {
	// Try to memory map the file
	flag := mmap.RDONLY
	if write {
		flag = mmap.RDWR
	}
	mem, err := mmap.Map(file, flag, 0)
	if err != nil {
		return nil, nil, err
	}
	// The file is now memory-mapped. Create a []uint32 view of the file.
	var view []uint32
	header := (*reflect.SliceHeader)(unsafe.Pointer(&view))
	header.Data = (*reflect.SliceHeader)(unsafe.Pointer(&mem)).Data
	header.Len /= 4
	header.Cap /= 4

	return mem, view, nil
}

// memoryMapAndGenerate tries to memory map a temporary file of uint32s for write
// access, fill it with the data from a generator and then move it into the final
// path requested.
func memoryMapAndGenerate(path string, size uint64, lock bool, generator func(buffer []uint32)) (*os.File, mmap.MMap, []uint32, error) {
	// Ensure the data folder exists
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, nil, nil, err
	}
	suffix, err := math.RandInt64()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get random integer: %v", err)
	}
	// Create a huge temporary empty file to fill with data
	temp := path + "." + strconv.Itoa(int(suffix))

	dump, err := os.Create(temp)
	if err != nil {
		return nil, nil, nil, err
	}
	if err = dump.Truncate(int64(len(dumpMagic))*4 + int64(size)); err != nil {
		return nil, nil, nil, err
	}
	// Memory map the file for writing and fill it with the generator
	mem, buffer, err := memoryMapFile(dump, true)
	if err != nil {
		dump.Close()
		return nil, nil, nil, err
	}
	copy(buffer, dumpMagic)

	data := buffer[len(dumpMagic):]
	generator(data)

	if err := mem.Unmap(); err != nil {
		return nil, nil, nil, err
	}
	if err := dump.Close(); err != nil {
		return nil, nil, nil, err
	}
	if err := os.Rename(temp, path); err != nil {
		return nil, nil, nil, err
	}
	return memoryMap(path, lock)
}

// lru tracks caches or datasets by their last use time, keeping at most N of them.
type lru struct {
	what string
	new  func(epoch uint64) interface{}
	mu   sync.Mutex
	// Items are kept in a LRU cache, but there is a special case:
	// We always keep an item for (highest seen epoch) + 1 as the 'future item'.
	cache      *simplelru.LRU[uint64, any]
	future     uint64
	futureItem interface{}
}

// newlru create a new least-recently-used cache for either the verification caches
// or the mining datasets.
func newlru(what string, maxItems int, new func(epoch uint64) interface{}) *lru {
	if maxItems <= 0 {
		maxItems = 1
	}
	cache, _ := simplelru.NewLRU[uint64, any](maxItems, func(key uint64, value interface{}) {
		log.Trace("Evicted ethash "+what, "epoch", key)
	})
	return &lru{what: what, new: new, cache: cache}
}

// get retrieves or creates an item for the given epoch. The first return value is always
// non-nil. The second return value is non-nil if lru thinks that an item will be useful in
// the near future.
func (lru *lru) get(epoch uint64) (item, future interface{}) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	// Get or create the item for the requested epoch.
	item, ok := lru.cache.Get(epoch)
	if !ok {
		if lru.future > 0 && lru.future == epoch {
			item = lru.futureItem
		} else {
			log.Trace("Requiring new ethash "+lru.what, "epoch", epoch)
			item = lru.new(epoch)
		}
		lru.cache.Add(epoch, item)
	}
	// Update the 'future item' if epoch is larger than previously seen.
	if epoch < maxEpoch-1 && lru.future < epoch+1 {
		log.Trace("Requiring new future ethash "+lru.what, "epoch", epoch+1)
		future = lru.new(epoch + 1)
		lru.future = epoch + 1
		lru.futureItem = future
	}
	return item, future
}

// cache wraps an ethash cache with some metadata to allow easier concurrent use.
type cache struct {
	epoch uint64    // Epoch for which this cache is relevant
	dump  *os.File  // File descriptor of the memory mapped cache
	mmap  mmap.MMap // Memory map itself to unmap before releasing
	cache []uint32  // The actual cache data content (may be memory mapped)
	once  sync.Once // Ensures the cache is generated only once
}

// newCache creates a new ethash verification cache and returns it as a plain Go
// interface to be usable in an LRU cache.
func newCache(epoch uint64) interface{} {
	return &cache{epoch: epoch}
}

// generate ensures that the cache content is generated before use.
func (c *cache) generate(dir string, limit int, lock bool, test bool) {
	c.once.Do(func() {
		defer dbg.LogPanic()
		size := cacheSize(c.epoch*epochLength + 1)
		seed := seedHash(c.epoch*epochLength + 1)
		if test {
			size = 1024
		}
		// If we don't store anything on disk, generate and return.
		if dir == "" {
			c.cache = make([]uint32, size/4)
			generateCache(c.cache, c.epoch, seed)
			return
		}
		// Disk storage is needed, this will get fancy
		var endian string
		if !isLittleEndian() {
			endian = ".be"
		}
		path := filepath.Join(dir, fmt.Sprintf("cache-R%d-%x%s", algorithmRevision, seed[:8], endian))
		logger := log.New("epoch", c.epoch)

		// We're about to mmap the file, ensure that the mapping is cleaned up when the
		// cache becomes unused.
		runtime.SetFinalizer(c, (*cache).finalizer)

		// Try to load the file from disk and memory map it
		var err error
		c.dump, c.mmap, c.cache, err = memoryMap(path, lock)
		if err == nil {
			logger.Debug("Loaded old ethash cache from disk")
			return
		}
		logger.Debug("Failed to load old ethash cache", "err", err)

		// No previous cache available, create a new cache file to fill
		c.dump, c.mmap, c.cache, err = memoryMapAndGenerate(path, size, lock, func(buffer []uint32) { generateCache(buffer, c.epoch, seed) })
		if err != nil {
			logger.Error("Failed to generate mapped ethash cache", "err", err)

			c.cache = make([]uint32, size/4)
			generateCache(c.cache, c.epoch, seed)
		}
		// Iterate over all previous instances and delete old ones
		for ep := int(c.epoch) - limit; ep >= 0; ep-- {
			seed := seedHash(uint64(ep)*epochLength + 1)
			path := filepath.Join(dir, fmt.Sprintf("cache-R%d-%x%s", algorithmRevision, seed[:8], endian))
			dir2.RemoveFile(path)
		}
	})
}

// finalizer unmaps the memory and closes the file.
func (c *cache) finalizer() {
	if c.mmap != nil {
		c.mmap.Unmap()
		c.dump.Close()
		c.mmap, c.dump = nil, nil
	}
}

// dataset wraps an ethash dataset with some metadata to allow easier concurrent use.
type dataset struct {
	epoch   uint64    // Epoch for which this cache is relevant
	dump    *os.File  // File descriptor of the memory mapped cache
	mmap    mmap.MMap // Memory map itself to unmap before releasing
	dataset []uint32  // The actual cache data content
	once    sync.Once // Ensures the cache is generated only once
	done    uint32    // Atomic flag to determine generation status
}

// newDataset creates a new ethash mining dataset and returns it as a plain Go
// interface to be usable in an LRU cache.
func newDataset(epoch uint64) interface{} {
	return &dataset{epoch: epoch}
}

// generate ensures that the dataset content is generated before use.
func (d *dataset) generate(dir string, limit int, lock bool, test bool) {
	d.once.Do(func() {
		// Mark the dataset generated after we're done. This is needed for remote
		defer atomic.StoreUint32(&d.done, 1)

		csize := cacheSize(d.epoch*epochLength + 1)
		dsize := datasetSize(d.epoch*epochLength + 1)
		seed := seedHash(d.epoch*epochLength + 1)
		if test {
			csize = 1024
			dsize = 32 * 1024
		}
		// If we don't store anything on disk, generate and return
		if dir == "" {
			cache := make([]uint32, csize/4)
			generateCache(cache, d.epoch, seed)

			d.dataset = make([]uint32, dsize/4)
			generateDataset(d.dataset, d.epoch, cache)

			return
		}
		// Disk storage is needed, this will get fancy
		var endian string
		if !isLittleEndian() {
			endian = ".be"
		}
		path := filepath.Join(dir, fmt.Sprintf("full-R%d-%x%s", algorithmRevision, seed[:8], endian))
		logger := log.New("epoch", d.epoch)

		// We're about to mmap the file, ensure that the mapping is cleaned up when the
		// cache becomes unused.
		runtime.SetFinalizer(d, (*dataset).finalizer)

		// Try to load the file from disk and memory map it
		var err error
		d.dump, d.mmap, d.dataset, err = memoryMap(path, lock)
		if err == nil {
			logger.Debug("Loaded old ethash dataset from disk")
			return
		}
		logger.Debug("Failed to load old ethash dataset", "err", err)

		// No previous dataset available, create a new dataset file to fill
		cache := make([]uint32, csize/4)
		generateCache(cache, d.epoch, seed)

		d.dump, d.mmap, d.dataset, err = memoryMapAndGenerate(path, dsize, lock, func(buffer []uint32) { generateDataset(buffer, d.epoch, cache) })
		if err != nil {
			logger.Error("Failed to generate mapped ethash dataset", "err", err)

			d.dataset = make([]uint32, dsize/2)
			generateDataset(d.dataset, d.epoch, cache)
		}
		// Iterate over all previous instances and delete old ones
		for ep := int(d.epoch) - limit; ep >= 0; ep-- {
			seed := seedHash(uint64(ep)*epochLength + 1)
			path := filepath.Join(dir, fmt.Sprintf("full-R%d-%x%s", algorithmRevision, seed[:8], endian))
			dir2.RemoveFile(path)
		}
	})
}

// generated returns whether this particular dataset finished generating already
// or not (it may not have been started at all). This is useful for remote miners
// to default to verification caches instead of blocking on DAG generations.
func (d *dataset) generated() bool {
	return atomic.LoadUint32(&d.done) == 1
}

// finalizer closes any file handlers and memory maps open.
func (d *dataset) finalizer() {
	if d.mmap != nil {
		d.mmap.Unmap()
		d.dump.Close()
		d.mmap, d.dump = nil, nil
	}
}

// Ethash is a consensus engine based on proof-of-work implementing the ethash
// algorithm.
type Ethash struct {
	config ethashcfg.Config

	caches   *lru // In memory caches to avoid regenerating too often
	datasets *lru // In memory datasets to avoid regenerating too often

	// Mining related fields
	rand     *rand.Rand     // Properly seeded random source for nonces
	hashrate *hashRateMeter // Meter tracking the average hashrate
	remote   *remoteSealer

	// The fields below are hooks for testing
	shared *Ethash // Shared PoW verifier to avoid cache regeneration

	lock      sync.Mutex // Ensures thread safety for the in-memory caches and mining fields
	closeOnce sync.Once  // Ensures exit channel will not be closed twice.
}

// New creates a full sized ethash PoW scheme and starts a background thread for
// remote mining, also optionally notifying a batch of remote services of new work
// packages.
func New(config ethashcfg.Config, notify []string, noverify bool) *Ethash {
	if config.Log == nil {
		config.Log = log.Root()
	}
	if config.CachesInMem <= 0 {
		config.Log.Warn("One ethash cache must always be in memory", "requested", config.CachesInMem)
		config.CachesInMem = 1
	}
	if config.DatasetDir != "" && config.DatasetsOnDisk > 0 {
		config.Log.Info("Disk storage enabled for ethash DAGs", "dir", config.DatasetDir, "count", config.DatasetsOnDisk)
	}
	ethash := &Ethash{
		config:   config,
		caches:   newlru("cache", config.CachesInMem, newCache),
		datasets: newlru("dataset", config.DatasetsInMem, newDataset),
		hashrate: newHashRateMeter(),
	}
	if config.PowMode == ethashcfg.ModeShared {
		ethash.shared = GetSharedEthash()
	}
	ethash.remote = startRemoteSealer(ethash, notify, noverify)
	return ethash
}

// NewTester creates a small sized ethash PoW scheme useful only for testing
// purposes.
func NewTester(notify []string, noverify bool) *Ethash {
	return New(ethashcfg.Config{PowMode: ethashcfg.ModeTest}, notify, noverify)
}

// NewShared creates a full sized ethash PoW shared between all requesters running
// in the same process.
func NewShared() *Ethash {
	return &Ethash{shared: GetSharedEthash()}
}

// Close closes the exit channel to notify all backend threads exiting.
func (ethash *Ethash) Close() error {
	ethash.closeOnce.Do(func() {
		// Short circuit if the exit channel is not allocated.
		if ethash.remote == nil {
			return
		}
		close(ethash.remote.requestExit)
		<-ethash.remote.exitCh
	})
	return nil
}

// cache tries to retrieve a verification cache for the specified block number
// by first checking against a list of in-memory caches, then against caches
// stored on disk, and finally generating one if none can be found.
func (ethash *Ethash) cache(block uint64) *cache {
	epoch := block / epochLength
	currentI, futureI := ethash.caches.get(epoch)
	current := currentI.(*cache)

	// Wait for generation finish.
	current.generate(doNotStoreCachesOnDisk, 0, ethash.config.CachesLockMmap, ethash.config.PowMode == ethashcfg.ModeTest)

	// If we need a new future cache, now's a good time to regenerate it.
	if futureI != nil {
		future := futureI.(*cache)
		go future.generate(doNotStoreCachesOnDisk, 0, ethash.config.CachesLockMmap, ethash.config.PowMode == ethashcfg.ModeTest)
	}
	return current
}

// dataset tries to retrieve a mining dataset for the specified block number
// by first checking against a list of in-memory datasets, then against DAGs
// stored on disk, and finally generating one if none can be found.
//
// If async is specified, not only the future but the current DAG is also
// generates on a background thread.
func (ethash *Ethash) dataset(block uint64, async bool) *dataset {
	// Retrieve the requested ethash dataset
	epoch := block / epochLength
	currentI, futureI := ethash.datasets.get(epoch)
	current := currentI.(*dataset)

	// If async is specified, generate everything in a background thread
	if async && !current.generated() {
		go func() {
			defer dbg.LogPanic()
			current.generate(ethash.config.DatasetDir, ethash.config.DatasetsOnDisk, ethash.config.DatasetsLockMmap, ethash.config.PowMode == ethashcfg.ModeTest)

			if futureI != nil {
				future := futureI.(*dataset)
				future.generate(ethash.config.DatasetDir, ethash.config.DatasetsOnDisk, ethash.config.DatasetsLockMmap, ethash.config.PowMode == ethashcfg.ModeTest)
			}
		}()
	} else {
		// Either blocking generation was requested, or already done
		current.generate(ethash.config.DatasetDir, ethash.config.DatasetsOnDisk, ethash.config.DatasetsLockMmap, ethash.config.PowMode == ethashcfg.ModeTest)

		if futureI != nil {
			future := futureI.(*dataset)
			go future.generate(ethash.config.DatasetDir, ethash.config.DatasetsOnDisk, ethash.config.DatasetsLockMmap, ethash.config.PowMode == ethashcfg.ModeTest)
		}
	}
	return current
}

// Hashrate implements PoW, returning the measured rate of the search invocations
// per second over the last minute.
// Note the returned hashrate includes local hashrate, but also includes the total
// hashrate of all remote miner.
func (ethash *Ethash) Hashrate() float64 {
	// Short circuit if we are run the ethash in normal/test mode.
	if (ethash.config.PowMode != ethashcfg.ModeNormal && ethash.config.PowMode != ethashcfg.ModeTest) || ethash.remote == nil {
		return ethash.hashrate.Rate()
	}
	var res = make(chan uint64, 1)

	select {
	case ethash.remote.fetchRateCh <- res:
	case <-ethash.remote.exitCh:
		// Return local hashrate only if ethash is stopped.
		return ethash.hashrate.Rate()
	}

	// Gather total submitted hash rate of remote sealers.
	return ethash.hashrate.Rate() + float64(<-res)
}

// APIs implements consensus.Engine, returning the user facing RPC APIs.
func (ethash *Ethash) APIs(chain consensus.ChainHeaderReader) []rpc.API {
	// In order to ensure backward compatibility, we exposes ethash RPC APIs
	// to both eth and ethash namespaces.
	return []rpc.API{
		{
			Namespace: "eth",
			Version:   "1.0",
			Service:   &API{ethash},
			Public:    true,
		},
		{
			Namespace: "ethash",
			Version:   "1.0",
			Service:   &API{ethash},
			Public:    true,
		},
	}
}

// SeedHash is the seed to use for generating a verification cache and the mining
// dataset.
func SeedHash(block uint64) []byte {
	return seedHash(block)
}

func (ethash *Ethash) GetTransferFunc() evmtypes.TransferFunc {
	return consensus.Transfer
}

func (ethash *Ethash) GetPostApplyMessageFunc() evmtypes.PostApplyMessageFunc {
	return nil
}

func (c *Ethash) TxDependencies(h *types.Header) [][]int {
	return nil
}
