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

package exec

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/execution/types"
)

func balCommitmentWarmupKeys(bal types.BlockAccessList) [][]byte {
	keyCount := 0
	for i := range bal {
		account := &bal[i]
		if len(account.BalanceChanges)+len(account.NonceChanges)+len(account.CodeChanges) > 0 {
			keyCount++
		}
		keyCount += len(account.StorageChanges)
	}
	keys := make([][]byte, 0, keyCount)
	for i := range bal {
		account := &bal[i]
		address := account.Address
		if len(account.BalanceChanges)+len(account.NonceChanges)+len(account.CodeChanges) > 0 {
			keys = append(keys, commitment.KeyToHexNibbleHash(address[:]))
		}
		for _, storage := range account.StorageChanges {
			slot := storage.Slot.Value()
			plainKey := make([]byte, len(address)+len(slot))
			copy(plainKey, address[:])
			copy(plainKey[len(address):], slot[:])
			keys = append(keys, commitment.KeyToHexNibbleHash(plainKey))
		}
	}
	slices.SortFunc(keys, bytes.Compare)
	return keys
}

type balCommitmentContext struct {
	tx         kv.TemporalTx
	cache      *commitment.BranchCache
	cacheStats *balCommitmentCacheStats
}

func (c *balCommitmentContext) Branch(prefix []byte) ([]byte, kv.Step, error) {
	if c.cache == nil {
		return c.tx.GetLatest(kv.CommitmentDomain, prefix, kv.GetLatestOptions{})
	}

	data, step, ok := c.cache.Get(prefix)
	if ok {
		c.cacheStats.hits.Add(1)
		return data, kv.Step(step), nil
	}
	c.cacheStats.misses.Add(1)
	c.cacheStats.fillRequests.Add(1)
	return c.tx.GetLatest(kv.CommitmentDomain, prefix, kv.GetLatestOptions{}.WithBranchCache())
}

type balCommitmentCacheStats struct {
	hits         atomic.Uint64
	misses       atomic.Uint64
	fillRequests atomic.Uint64
}

func (*balCommitmentContext) PutBranch([]byte, []byte, []byte) error {
	return errors.New("BAL commitment warmup is read-only")
}

func (*balCommitmentContext) Account([]byte) (*commitment.Update, error) {
	return nil, errors.New("BAL commitment warmup does not read accounts")
}

func (*balCommitmentContext) Storage([]byte) (*commitment.Update, error) {
	return nil, errors.New("BAL commitment warmup does not read storage")
}

func warmBALCommitment(ctx context.Context, db kv.RoDB, bal types.BlockAccessList, workers int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	keys := balCommitmentWarmupKeys(bal)
	if len(keys) == 0 || workers <= 0 {
		return nil
	}
	workers = min(workers, len(keys))
	cacheStats := new(balCommitmentCacheStats)
	log.Info("[warmBAL] commitment warmup started", "keys", len(keys), "workers", workers)
	started := time.Now()

	factoryErrs := make(chan error, workers)
	warmuper := commitment.NewWarmuper(ctx, commitment.WarmupConfig{
		Enabled:    true,
		CtxFactory: balCommitmentContextFactory(db, cacheStats, func(err error) { factoryErrs <- err }),
		NumWorkers: workers,
		MaxDepth:   commitment.WarmupMaxDepth,
		LogPrefix:  "BAL",
	})
	warmuper.Start()
	var previous []byte
	for _, key := range keys {
		startDepth := 0
		for startDepth < min(len(previous), len(key)) && previous[startDepth] == key[startDepth] {
			startDepth++
		}
		warmuper.WarmKey(key, startDepth, 0)
		previous = key
	}
	err := warmuper.WaitBufferFree(0)
	warmuper.Close()
	warmuper.DrainPending()
	warmuper.CloseAndWait()
	close(factoryErrs)
	if ctxErr := ctx.Err(); ctxErr != nil {
		// Every worker fails with the same cancellation; report it once.
		log.Debug(
			"[warmBAL] commitment warmup canceled",
			"keys", len(keys),
			"workers", workers,
			"cacheHits", cacheStats.hits.Load(),
			"cacheMisses", cacheStats.misses.Load(),
			"cacheFillRequests", cacheStats.fillRequests.Load(),
			"elapsed", time.Since(started),
		)
		return ctxErr
	}
	// The first worker error cancels the other workers; their context errors are not causes.
	if errors.Is(err, context.Canceled) {
		err = nil
	}
	// Workers may fail with separately allocated errors that read the same.
	reported := make(map[string]struct{}, workers)
	if err != nil {
		reported[err.Error()] = struct{}{}
	}
	for factoryErr := range factoryErrs {
		if _, ok := reported[factoryErr.Error()]; ok || errors.Is(factoryErr, context.Canceled) {
			continue
		}
		reported[factoryErr.Error()] = struct{}{}
		if err == nil {
			err = factoryErr
		} else {
			err = errors.Join(err, factoryErr)
		}
	}
	log.Info(
		"[warmBAL] commitment warmup finished",
		"keys", len(keys),
		"workers", workers,
		"cacheHits", cacheStats.hits.Load(),
		"cacheMisses", cacheStats.misses.Load(),
		"cacheFillRequests", cacheStats.fillRequests.Load(),
		"elapsed", time.Since(started),
		"err", err,
	)
	return err
}

func balCommitmentContextFactory(db kv.RoDB, cacheStats *balCommitmentCacheStats, onErr func(error)) commitment.TrieContextFactory {
	return func(workerCtx context.Context) (commitment.PatriciaContext, func()) {
		tx, err := db.BeginRo(kv.WithNonBlockingAcquire(workerCtx)) //nolint:gocritic // The returned cleanup owns Rollback.
		if err != nil {
			onErr(err)
			return nil, nil
		}
		txTemporal, ok := tx.(kv.TemporalTx)
		if !ok {
			tx.Rollback()
			onErr(errors.New("BAL commitment warmup requires a temporal read transaction"))
			return nil, nil
		}
		var cache *commitment.BranchCache
		if provider, ok := txTemporal.AggTx().(commitment.BranchCacheProvider); ok {
			cache = provider.BranchCache()
		}
		return &balCommitmentContext{
			tx:         txTemporal,
			cache:      cache,
			cacheStats: cacheStats,
		}, tx.Rollback
	}
}

func StartBranchPrefetch(ctx context.Context, db kv.RoDB, workers int) *commitment.Warmuper {
	prefetch := commitment.NewWarmuper(ctx, commitment.WarmupConfig{
		Enabled:    true,
		CtxFactory: balCommitmentContextFactory(db, new(balCommitmentCacheStats), func(error) {}),
		NumWorkers: workers,
		MaxDepth:   commitment.WarmupMaxDepth,
		LogPrefix:  "prefetch",
	})
	prefetch.Start()
	return prefetch
}

type latestPrefetcher interface {
	PrefetchLatest(ctx context.Context, domain kv.Domain, keys [][]byte)
}

const (
	prefetchFlatDepth = 8 // trie levels read as one flat batch before the walk checks branches
	prefetchPar       = 4 // read txs one prefetch step spreads its keys over
)

type rawBranchReader interface {
	GetLatestBranchRaw(prefix []byte, tx kv.Tx) ([]byte, bool, error)
}

// inChunks runs fn over up to par chunks of n items at once: the first chunk on ttx, the others each on a read tx taken
// without blocking. A chunk that gets no tx runs on ttx after the others, since the caller holds a permit.
func inChunks(ctx context.Context, db kv.RoDB, ttx kv.TemporalTx, n, par int, fn func(tx kv.TemporalTx, from, to int)) {
	chunk := (n + par - 1) / max(par, 1)
	if chunk == 0 {
		return
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var skipped []int
	for from := chunk; from < n; from += chunk {
		wg.Go(func() {
			tx, err := db.BeginRo(kv.WithNonBlockingAcquire(ctx))
			if err != nil {
				mu.Lock()
				skipped = append(skipped, from)
				mu.Unlock()
				return
			}
			defer tx.Rollback()
			if ttx, ok := tx.(kv.TemporalTx); ok {
				fn(ttx, from, min(from+chunk, n))
			}
		})
	}
	fn(ttx, 0, min(chunk, n))
	wg.Wait()
	for _, from := range skipped {
		fn(ttx, from, min(from+chunk, n))
	}
}

// withTemporalTx runs fn on a new temporal read tx.
func withTemporalTx(ctx context.Context, db kv.RoDB, fn func(ttx kv.TemporalTx)) error {
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if ttx, ok := tx.(kv.TemporalTx); ok {
		fn(ttx)
	}
	return nil
}

// keysWithChild keeps the keys whose branch at depth has their next nibble.
// A key whose path ends above depth has no branch there, so checks at shallower depths can be skipped.
func keysWithChild(ctx context.Context, db kv.RoDB, ttx kv.TemporalTx, keys [][]byte, depth int) [][]byte {
	keep := make([]bool, len(keys))
	inChunks(ctx, db, ttx, len(keys), max(1, min(prefetchPar, len(keys)/256)), func(tx kv.TemporalTx, from, to int) {
		// Only the child bitmap is needed: a raw branch skips the reads that resolve shortened keys.
		raw, rawOK := tx.AggTx().(rawBranchReader)
		compact := make([]byte, 0, 128)
		// Keys are sorted, so keys sharing a branch are adjacent: read it once.
		var lastPrefix []byte
		var bits uint16
		for i := from; i < to && ctx.Err() == nil; i++ {
			k := keys[i]
			if depth >= len(k) {
				continue
			}
			if lastPrefix == nil || !bytes.Equal(lastPrefix, k[:depth]) {
				prefix := nibbles.HexToCompactInto(compact[:cap(compact)], k[:depth])
				var branch []byte
				var err error
				if rawOK {
					branch, _, err = raw.GetLatestBranchRaw(prefix, tx)
				} else {
					branch, _, err = tx.GetLatest(kv.CommitmentDomain, prefix, kv.GetLatestOptions{})
				}
				bits = 0
				if err == nil && len(branch) >= 4 {
					bits = binary.BigEndian.Uint16(branch[2:4])
				}
				lastPrefix = k[:depth]
			}
			keep[i] = bits&(uint16(1)<<k[depth]) != 0
		}
	})
	next := keys[:0:0]
	for i, k := range keys {
		if keep[i] {
			next = append(next, k)
		}
	}
	return next
}

// prefetchKeys spreads keys over prefetchPar read txs.
func prefetchKeys(ctx context.Context, db kv.RoDB, ttx kv.TemporalTx, domain kv.Domain, keys [][]byte) {
	par := prefetchPar
	if len(keys) < 2*prefetchPar {
		par = 1
	}
	inChunks(ctx, db, ttx, len(keys), par, func(tx kv.TemporalTx, from, to int) {
		if p, ok := tx.AggTx().(latestPrefetcher); ok {
			p.PrefetchLatest(ctx, domain, keys[from:to])
		}
	})
}

// prefetchBAL reads into the page cache, in io_uring batches, the file pages the block's BAL reads touch: accounts,
// storage, and the commitment trie level by level, since a branch decides whether the next level exists.
func prefetchBAL(ctx context.Context, db kv.RoDB, bal types.BlockAccessList) {
	started := time.Now()
	accs := make([][]byte, 0, len(bal))
	var slots [][]byte
	for i := range bal {
		addr := bal[i].Address
		accs = append(accs, bytes.Clone(addr[:]))
		for _, sc := range bal[i].StorageChanges {
			slot := sc.Slot.Value()
			slots = append(slots, append(bytes.Clone(addr[:]), slot[:]...))
		}
		for _, sr := range bal[i].StorageReads {
			slots = append(slots, append(bytes.Clone(addr[:]), sr[:]...))
		}
	}
	var g errgroup.Group
	g.Go(func() error { return prefetchDomain(ctx, db, kv.AccountsDomain, accs) })
	g.Go(func() error { return prefetchDomain(ctx, db, kv.StorageDomain, slots) })
	g.Go(func() error { return prefetchCommitment(ctx, db, balCommitmentWarmupKeys(bal)) })
	err := g.Wait()
	log.Debug("[warmBAL] batch prefetch", "accounts", len(accs), "slots", len(slots), "elapsed", time.Since(started), "err", err)
}

func prefetchDomain(ctx context.Context, db kv.RoDB, domain kv.Domain, keys [][]byte) error {
	return withTemporalTx(ctx, db, func(ttx kv.TemporalTx) {
		prefetchKeys(ctx, db, ttx, domain, keys)
	})
}

// compactPrefixes returns the distinct compact-encoded prefixes of the given nibble depth of keys that reach it.
func compactPrefixes(keys [][]byte, depth int) [][]byte {
	compact := make([]byte, 0, 128)
	seen := map[string]struct{}{}
	var prefixes [][]byte
	for _, k := range keys {
		if depth > len(k) {
			continue
		}
		pfx := string(nibbles.HexToCompactInto(compact[:cap(compact)], k[:depth]))
		if _, ok := seen[pfx]; !ok {
			seen[pfx] = struct{}{}
			prefixes = append(prefixes, []byte(pfx))
		}
	}
	return prefixes
}

func prefetchCommitment(ctx context.Context, db kv.RoDB, hashedKeys [][]byte) error {
	return withTemporalTx(ctx, db, func(ttx kv.TemporalTx) {
		var flat [][]byte
		for depth := 0; depth <= prefetchFlatDepth; depth++ {
			flat = append(flat, compactPrefixes(hashedKeys, depth)...)
		}
		prefetchKeys(ctx, db, ttx, kv.CommitmentDomain, flat)
		var storageKeys [][]byte
		for _, k := range hashedKeys {
			if len(k) > storageTrieDepth {
				storageKeys = append(storageKeys, k)
			}
		}
		// The account trie walk loses a storage key at the extension above its account, so storage tries start again at their root.
		walkTriePrefixes(ctx, db, ttx, hashedKeys, 0)
		walkTriePrefixes(ctx, db, ttx, storageKeys, storageTrieDepth)
	})
}

const storageTrieDepth = 64

func walkTriePrefixes(ctx context.Context, db kv.RoDB, ttx kv.TemporalTx, keys [][]byte, start int) {
	active := keys
	for depth := start; len(active) > 0 && depth <= commitment.WarmupMaxDepth; depth++ {
		if depth > prefetchFlatDepth {
			prefetchKeys(ctx, db, ttx, kv.CommitmentDomain, compactPrefixes(active, depth))
		}
		if depth >= prefetchFlatDepth {
			active = keysWithChild(ctx, db, ttx, active, depth)
		}
	}
}
