// Copyright 2026 The Erigon Authors
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

package testutil

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	dirutil "github.com/erigontech/erigon/common/dir"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

// allocHasSystemContracts checks if the genesis alloc already includes
// code for Prague system contracts (EIP-7002 withdrawal requests,
// EIP-7251 consolidation requests). If so, InitPraguePreDeploys should
// be skipped to avoid overwriting test-fixture-provided bytecode.
func allocHasSystemContracts(g *types.Genesis) bool {
	for _, addr := range []common.Address{
		params.WithdrawalRequestAddress.Value(),
		params.ConsolidationRequestAddress.Value(),
	} {
		if acct, ok := g.Alloc[addr]; ok && len(acct.Code) > 0 {
			return true
		}
	}
	return false
}

// originalTmpDir captures the system temp dir before RunTestMain overrides
// it with a ramdisk. Genesis cache DBs use this to avoid consuming ramdisk.
var originalTmpDir = os.TempDir()

// genesisCacheEntry holds a cached genesis DB and its genesis block.
type genesisCacheEntry struct {
	db      kv.TemporalRwDB
	genesis *types.Block
	dir     string // temp directory to remove on cleanup
	refs    int    // number of active users; protected by genesisDBMu
	lastUse int64  // UnixNano timestamp of last acquire; protected by genesisDBMu
	ready   chan struct{}
	err     error // set once, before closing ready
}

// genesisDBCache caches one MDBX database per unique (fork, genesisSpecHash) pair.
// The DB contains genesis KV state (headers, TDs, config) plus domain state
// (account balances, code, storage) written via SharedDomains.
// The cache is bounded to maxGenesisCacheSize entries to avoid exhausting disk.
// When full, the least-recently-used entry with no active users is evicted.
const maxGenesisCacheSize = 128

var (
	genesisDBMu    sync.Mutex // protects all fields below
	genesisDBCond  = sync.NewCond(&genesisDBMu)
	genesisDBCache = make(map[string]*genesisCacheEntry)
	evictionWg     sync.WaitGroup // tracks background eviction goroutines
)

// genesisSpecHash produces a deterministic hash of the genesis spec for cache keying.
// It includes alloc addresses+nonces and header fields that affect the genesis hash.
func genesisSpecHash(g *types.Genesis) string {
	h := sha256.New()

	// Hash the alloc (sorted by address for determinism).
	addrs := make([]common.Address, 0, len(g.Alloc))
	for addr := range g.Alloc {
		addrs = append(addrs, addr)
	}
	sort.Slice(addrs, func(i, j int) bool { return addrs[i].Cmp(addrs[j]) < 0 })
	for _, addr := range addrs {
		h.Write(addr[:])
		acct := g.Alloc[addr]
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], acct.Nonce)
		h.Write(buf[:])
		if acct.Balance != nil {
			h.Write(acct.Balance.Bytes())
		}
		h.Write(acct.Code)
		// Hash storage keys and values (sorted by key for determinism).
		if len(acct.Storage) > 0 {
			storageKeys := make([]common.Hash, 0, len(acct.Storage))
			for k := range acct.Storage {
				storageKeys = append(storageKeys, k)
			}
			sort.Slice(storageKeys, func(i, j int) bool {
				return bytes.Compare(storageKeys[i][:], storageKeys[j][:]) < 0
			})
			for _, k := range storageKeys {
				h.Write(k[:])
				h.Write(acct.Storage[k].Bytes())
			}
		}
	}

	// Hash header fields that affect the genesis block hash.
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], g.GasLimit)
	h.Write(buf[:])
	binary.LittleEndian.PutUint64(buf[:], g.Timestamp)
	h.Write(buf[:])
	binary.LittleEndian.PutUint64(buf[:], g.Nonce)
	h.Write(buf[:])
	h.Write(g.ExtraData)
	h.Write(g.Coinbase[:])
	h.Write(g.Mixhash[:])
	h.Write(g.ParentHash[:])
	if g.Difficulty != nil {
		h.Write(g.Difficulty.Bytes())
	}
	if g.BaseFee != nil {
		h.Write(g.BaseFee.Bytes())
	}
	if g.ExcessBlobGas != nil {
		binary.LittleEndian.PutUint64(buf[:], *g.ExcessBlobGas)
		h.Write(buf[:])
	}
	if g.BlobGasUsed != nil {
		binary.LittleEndian.PutUint64(buf[:], *g.BlobGasUsed)
		h.Write(buf[:])
	}

	return fmt.Sprintf("%x", h.Sum(nil)[:16])
}

// evictLRU finds the least-recently-used entry with refs==0, removes it from
// the cache, and performs expensive cleanup (DB close, dir removal) after
// releasing the lock. Returns true if an entry was evicted.
// Caller must hold genesisDBMu.
func evictLRU() bool {
	var victimKey string
	var victimEntry *genesisCacheEntry
	for k, e := range genesisDBCache {
		if e.refs > 0 {
			continue
		}
		if victimEntry == nil || e.lastUse < victimEntry.lastUse {
			victimKey = k
			victimEntry = e
		}
	}
	if victimEntry == nil {
		return false // all entries in use
	}
	// Remove from map under lock, then do expensive IO outside the lock.
	delete(genesisDBCache, victimKey)
	evictionWg.Add(1)
	go func() {
		defer evictionWg.Done()
		if victimEntry.db != nil {
			victimEntry.db.Close()
		}
		dirutil.RemoveAll(victimEntry.dir)
	}()
	return true
}

// releaseGenesisDB decrements the reference count for a cache entry
// and wakes any goroutines waiting for a free slot.
func releaseGenesisDB(key string) {
	genesisDBMu.Lock()
	defer genesisDBMu.Unlock()
	if e, ok := genesisDBCache[key]; ok {
		e.refs--
		genesisDBCond.Signal()
	}
}

// createGenesisDB creates a new genesis DB on disk. Called without holding genesisDBMu.
func createGenesisDB(gspec *types.Genesis) (kv.TemporalRwDB, *types.Block, string, error) {
	logger := log.New()
	logger.SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	dir, err := os.MkdirTemp(originalTmpDir, "erigon-genesis-")
	if err != nil {
		return nil, nil, "", fmt.Errorf("genesis cache: temp dir: %w", err)
	}
	success := false
	defer func() {
		if !success {
			dirutil.RemoveAll(dir)
		}
	}()
	dirs := datadir.New(dir)
	db := temporaltest.NewTestDB(nil, dirs)

	// Step 1: CommitGenesisBlock writes headers, TDs, config to KV tables.
	genesisDirs := datadir.New(dir + "-genesis-tmp")
	if err := os.MkdirAll(genesisDirs.Tmp, 0755); err != nil {
		db.Close()
		return nil, nil, "", fmt.Errorf("genesis cache: mkdir %s: %w", genesisDirs.Tmp, err)
	}
	if err := os.MkdirAll(genesisDirs.SnapDomain, 0755); err != nil {
		db.Close()
		return nil, nil, "", fmt.Errorf("genesis cache: mkdir %s: %w", genesisDirs.SnapDomain, err)
	}
	var genesis *types.Block
	err = func() (gerr error) {
		defer func() {
			if r := recover(); r != nil {
				gerr = fmt.Errorf("panic: %v", r)
			}
		}()
		_, genesis, gerr = genesiswrite.CommitGenesisBlock(db, gspec, "", genesisDirs, logger)
		return
	}()
	dirutil.RemoveAll(genesisDirs.DataDir)
	if err != nil {
		db.Close()
		return nil, nil, "", fmt.Errorf("genesis cache: CommitGenesisBlock: %w", err)
	}

	ctx := context.Background()

	// Step 2: Write domain state via SharedDomains + ComputeGenesisCommitment.
	rwTx, err := db.BeginTemporalRw(ctx)
	if err != nil {
		db.Close()
		return nil, nil, "", fmt.Errorf("genesis cache: BeginTemporalRw: %w", err)
	}
	defer rwTx.Rollback() //nolint:gocritic

	sd, err := execctx.NewSharedDomains(ctx, rwTx, logger)
	if err != nil {
		db.Close()
		return nil, nil, "", fmt.Errorf("genesis cache: NewSharedDomains: %w", err)
	}

	_, _, err = genesiswrite.ComputeGenesisCommitment(ctx, gspec, rwTx, sd, genesis.Header())
	if err != nil {
		sd.Close()
		db.Close()
		return nil, nil, "", fmt.Errorf("genesis cache: ComputeGenesisCommitment: %w", err)
	}

	err = sd.Flush(ctx, rwTx)
	sd.Close()
	if err != nil {
		db.Close()
		return nil, nil, "", fmt.Errorf("genesis cache: sd.Flush: %w", err)
	}

	err = rwTx.Commit()
	if err != nil {
		db.Close()
		return nil, nil, "", fmt.Errorf("genesis cache: tx.Commit: %w", err)
	}

	// Step 3: Deploy Prague system contracts only if the genesis alloc
	// doesn't already include them (test fixtures provide their own bytecode).
	if gspec.Config.IsPrague(0) && !allocHasSystemContracts(gspec) {
		if err := blockgen.InitPraguePreDeploys(db, logger); err != nil {
			db.Close()
			return nil, nil, "", fmt.Errorf("genesis cache: InitPraguePreDeploys: %w", err)
		}
	}

	success = true
	return db, genesis, dir, nil
}

// getOrCreateGenesisDB returns a cached genesis DB for the given fork+alloc,
// creating it if necessary. The returned release function must be called when
// the caller is done with the DB (typically via defer). The DB has both KV
// state (headers, TDs) and domain state (accounts, code, storage) fully committed.
func getOrCreateGenesisDB(fork string, gspec *types.Genesis) (kv.TemporalRwDB, *types.Block, func(), error) {
	key := fork + "/" + genesisSpecHash(gspec)
	now := time.Now().UnixNano()

	genesisDBMu.Lock()
	for {
		// Already cached — reuse.
		if e, ok := genesisDBCache[key]; ok {
			e.refs++
			e.lastUse = now
			ready := e.ready
			genesisDBMu.Unlock()
			<-ready // wait for creation to finish (no-op if already closed)
			if e.err != nil {
				releaseGenesisDB(key)
				return nil, nil, nil, e.err
			}
			return e.db, e.genesis, func() { releaseGenesisDB(key) }, nil
		}

		// Cache has room or we can evict — proceed to create.
		if len(genesisDBCache) < maxGenesisCacheSize || evictLRU() {
			break
		}

		// Cache full, all in use — wait for a release.
		genesisDBCond.Wait()
	}

	// Reserve a placeholder so concurrent requests for the same key wait.
	entry := &genesisCacheEntry{refs: 1, lastUse: now, ready: make(chan struct{})}
	genesisDBCache[key] = entry
	genesisDBMu.Unlock()

	// Create the DB without holding the lock — allows other keys to proceed.
	db, genesis, dir, err := createGenesisDB(gspec)

	genesisDBMu.Lock()
	if err != nil {
		entry.err = err
		delete(genesisDBCache, key)
		close(entry.ready)
		genesisDBCond.Broadcast() // wake goroutines waiting for a free slot
		genesisDBMu.Unlock()
		return nil, nil, nil, err
	}
	entry.db = db
	entry.genesis = genesis
	entry.dir = dir
	close(entry.ready)
	genesisDBMu.Unlock()
	return db, genesis, func() { releaseGenesisDB(key) }, nil
}

// CleanupGenesisCache closes all cached genesis databases and removes their
// temp directories. Should be called from RunTestMain after m.Run() returns.
func CleanupGenesisCache() {
	// Wait for any in-flight eviction goroutines to finish first.
	evictionWg.Wait()

	genesisDBMu.Lock()
	entries := make(map[string]*genesisCacheEntry, len(genesisDBCache))
	for k, e := range genesisDBCache {
		entries[k] = e
		delete(genesisDBCache, k)
	}
	genesisDBMu.Unlock()

	for _, e := range entries {
		<-e.ready // wait for creation to finish
		if e.db != nil {
			e.db.Close()
		}
		dirutil.RemoveAll(e.dir)
	}
}
