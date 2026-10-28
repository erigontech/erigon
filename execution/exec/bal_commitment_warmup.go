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
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/dbg"
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
	PrefetchLatest(domain kv.Domain, keys [][]byte) (waves, pages int, plan, io time.Duration)
}

type prefetchStats struct {
	waves, pages        int
	plan, io, branchChk time.Duration
}

func (s *prefetchStats) add(waves, pages int, plan, io time.Duration) {
	s.waves, s.pages, s.plan, s.io = s.waves+waves, s.pages+pages, s.plan+plan, s.io+io
}

var (
	prefetchFlatDepth = dbg.EnvInt("BAL_PREFETCH_FLAT_DEPTH", -1)
	prefetchPar       = dbg.EnvInt("BAL_PREFETCH_PAR", 1)
	prefetchCode      = dbg.EnvBool("BAL_PREFETCH_CODE", false)
)

// keysWithChild keeps the keys whose branch at depth has their next nibble, checked by prefetchPar goroutines.
// A key whose path ends above depth has no branch there, so checks at shallower depths can be skipped.
func keysWithChild(ctx context.Context, db kv.RoDB, ttx kv.TemporalTx, keys [][]byte, depth int) ([][]byte, error) {
	keep := make([]bool, len(keys))
	check := func(tx kv.TemporalTx, from, to int) {
		compact := make([]byte, 0, 128)
		for i := from; i < to && ctx.Err() == nil; i++ {
			k := keys[i]
			if depth >= len(k) {
				continue
			}
			branch, _, err := tx.GetLatest(kv.CommitmentDomain, nibbles.HexToCompactInto(compact[:cap(compact)], k[:depth]), kv.GetLatestOptions{})
			keep[i] = err == nil && len(branch) >= 4 && binary.BigEndian.Uint16(branch[2:4])&(uint16(1)<<k[depth]) != 0
		}
	}
	par := max(1, min(prefetchPar, len(keys)/256))
	chunk := (len(keys) + par - 1) / par
	var g errgroup.Group
	for from := chunk; from < len(keys); from += chunk {
		g.Go(func() error {
			tx, err := db.BeginRo(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			if ttx, ok := tx.(kv.TemporalTx); ok {
				check(ttx, from, min(from+chunk, len(keys)))
			}
			return nil
		})
	}
	check(ttx, 0, min(chunk, len(keys)))
	if err := g.Wait(); err != nil {
		return nil, err
	}
	next := keys[:0:0]
	for i, k := range keys {
		if keep[i] {
			next = append(next, k)
		}
	}
	return next, nil
}

// prefetchKeys splits keys over prefetchPar planners, each with its own tx; stats keep the slowest planner's times.
func prefetchKeys(ctx context.Context, db kv.RoDB, p latestPrefetcher, domain kv.Domain, keys [][]byte, stats *prefetchStats) error {
	if prefetchPar <= 1 || len(keys) < 2*prefetchPar {
		stats.add(p.PrefetchLatest(domain, keys))
		return nil
	}
	var mu sync.Mutex
	var part prefetchStats
	var g errgroup.Group
	chunk := (len(keys) + prefetchPar - 1) / prefetchPar
	for i := 0; i < len(keys); i += chunk {
		keys := keys[i:min(i+chunk, len(keys))]
		first := i == 0
		g.Go(func() error {
			pp := p
			if !first {
				tx, err := db.BeginRo(ctx)
				if err != nil {
					return err
				}
				defer tx.Rollback()
				ttx, ok := tx.(kv.TemporalTx)
				if !ok {
					return nil
				}
				if pp, ok = ttx.AggTx().(latestPrefetcher); !ok {
					return nil
				}
			}
			waves, pages, plan, io := pp.PrefetchLatest(domain, keys)
			mu.Lock()
			defer mu.Unlock()
			part.waves, part.pages = max(part.waves, waves), part.pages+pages
			part.plan, part.io = max(part.plan, plan), max(part.io, io)
			return nil
		})
	}
	err := g.Wait()
	stats.add(part.waves, part.pages, part.plan, part.io)
	return err
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
	var stats [4]prefetchStats
	var g errgroup.Group
	g.Go(func() error { return prefetchDomain(ctx, db, kv.AccountsDomain, accs, &stats[0]) })
	g.Go(func() error { return prefetchDomain(ctx, db, kv.StorageDomain, slots, &stats[1]) })
	g.Go(func() error { return prefetchCommitment(ctx, db, balCommitmentWarmupKeys(bal), &stats[2]) })
	if prefetchCode {
		g.Go(func() error { return prefetchDomain(ctx, db, kv.CodeDomain, accs, &stats[3]) })
	}
	err := g.Wait()
	log.Info("[warmBAL] batch prefetch", "accounts", len(accs), "slots", len(slots),
		"acc", fmt.Sprintf("%d/%d plan=%s io=%s", stats[0].waves, stats[0].pages, stats[0].plan, stats[0].io),
		"slot", fmt.Sprintf("%d/%d plan=%s io=%s", stats[1].waves, stats[1].pages, stats[1].plan, stats[1].io),
		"code", fmt.Sprintf("%d/%d plan=%s io=%s", stats[3].waves, stats[3].pages, stats[3].plan, stats[3].io),
		"trie", fmt.Sprintf("%d/%d plan=%s io=%s branchChk=%s", stats[2].waves, stats[2].pages, stats[2].plan, stats[2].io, stats[2].branchChk),
		"flatDepth", prefetchFlatDepth, "par", prefetchPar, "elapsed", time.Since(started), "err", err)
}

func prefetchDomain(ctx context.Context, db kv.RoDB, domain kv.Domain, keys [][]byte, stats *prefetchStats) error {
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	ttx, ok := tx.(kv.TemporalTx)
	if !ok {
		return nil
	}
	p, ok := ttx.AggTx().(latestPrefetcher)
	if !ok {
		return nil
	}
	return prefetchKeys(ctx, db, p, domain, keys, stats)
}

func prefetchCommitment(ctx context.Context, db kv.RoDB, hashedKeys [][]byte, stats *prefetchStats) error {
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	ttx, ok := tx.(kv.TemporalTx)
	if !ok {
		return nil
	}
	p, ok := ttx.AggTx().(latestPrefetcher)
	if !ok {
		return nil
	}
	compact := make([]byte, 0, 128)
	if prefetchFlatDepth >= 0 {
		seen := map[string]struct{}{}
		var prefixes [][]byte
		for _, k := range hashedKeys {
			for depth := 0; depth <= min(prefetchFlatDepth, len(k)); depth++ {
				pfx := string(nibbles.HexToCompactInto(compact[:cap(compact)], k[:depth]))
				if _, ok := seen[pfx]; !ok {
					seen[pfx] = struct{}{}
					prefixes = append(prefixes, []byte(pfx))
				}
			}
		}
		if err := prefetchKeys(ctx, db, p, kv.CommitmentDomain, prefixes, stats); err != nil {
			return err
		}
	}
	active := hashedKeys
	for depth := 0; len(active) > 0 && depth <= commitment.WarmupMaxDepth; depth++ {
		seen := map[string]struct{}{}
		prefixes := make([][]byte, 0, len(active))
		for _, k := range active {
			if depth > len(k) {
				continue
			}
			pfx := string(nibbles.HexToCompactInto(compact[:cap(compact)], k[:depth]))
			if _, ok := seen[pfx]; !ok {
				seen[pfx] = struct{}{}
				prefixes = append(prefixes, []byte(pfx))
			}
		}
		if depth > prefetchFlatDepth {
			if err := prefetchKeys(ctx, db, p, kv.CommitmentDomain, prefixes, stats); err != nil {
				return err
			}
		}
		t0 := time.Now()
		next := active
		if depth >= prefetchFlatDepth {
			var err error
			if next, err = keysWithChild(ctx, db, ttx, active, depth); err != nil {
				return err
			}
		}
		stats.branchChk += time.Since(t0)
		active = next
	}
	return nil
}
