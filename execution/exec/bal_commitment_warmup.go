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
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common"
	commonerrors "github.com/erigontech/erigon/common/errors"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
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

func (c *balCommitmentContext) BranchNoCopy(prefix []byte) ([]byte, kv.Step, error) {
	return c.Branch(prefix)
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

// warmBALCommitment warms the trie branches of keys, the BAL's balCommitmentWarmupKeys.
func warmBALCommitment(ctx context.Context, db kv.RoDB, keys [][]byte, workers int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
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
	PrefetchLatest(ctx context.Context, domain kv.Domain, keys [][]byte) (coldPages int, err error)
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

// prefetchKeys spreads keys over prefetchPar read txs and returns the first read failure (see firstFailure).
func prefetchKeys(ctx context.Context, db kv.RoDB, ttx kv.TemporalTx, domain kv.Domain, keys [][]byte) error {
	par := prefetchPar
	if len(keys) < 2*prefetchPar {
		par = 1
	}
	var mu sync.Mutex
	var first error
	inChunks(ctx, db, ttx, len(keys), par, func(tx kv.TemporalTx, from, to int) {
		if p, ok := tx.AggTx().(latestPrefetcher); ok {
			_, err := p.PrefetchLatest(ctx, domain, keys[from:to])
			mu.Lock()
			first = firstFailure(first, err)
			mu.Unlock()
		}
	})
	return first
}

// firstFailure returns the first error that is a real read failure (not an incomplete batch or a cancellation),
// else the first non-nil one.
func firstFailure(errs ...error) error {
	for _, err := range errs {
		if err != nil && !errors.Is(err, kv.ErrPrefetchIncomplete) && !commonerrors.IsOnlyCanceled(err) {
			return err
		}
	}
	return cmp.Or(errs...)
}

// logBatchPrefetch logs a finished batch prefetch; a cancelled one is not logged.
func logBatchPrefetch(ctx context.Context, what string, keys int, started time.Time, err error) {
	if commonerrors.IsOnlyCanceled(err) || err == nil && ctx.Err() != nil {
		return
	}
	if err != nil && !errors.Is(err, kv.ErrPrefetchIncomplete) {
		logPrefetchFailure("[warmBAL] batch prefetch failed", "what", what, "keys", keys, "elapsed", time.Since(started), "err", err)
		return
	}
	log.Debug("[warmBAL] batch prefetch", "what", what, "keys", keys, "elapsed", time.Since(started))
}

var prefetchFailureWarned atomic.Bool

// logPrefetchFailure warns on the first batch-prefetch or residency-sample failure of the process and logs the rest at
// debug level: a broken ring fails every block the same way.
func logPrefetchFailure(msg string, ctx ...any) {
	if prefetchFailureWarned.CompareAndSwap(false, true) {
		log.Warn(msg, ctx...)
		return
	}
	log.Debug(msg, ctx...)
}

func logSampleFailure(domain kv.Domain, err error) {
	if err != nil && !commonerrors.IsOnlyCanceled(err) {
		logPrefetchFailure("[warmBAL] residency sample failed, the batch prefetch is off", "domain", domain, "err", err)
	}
}

// prefetchBALState reads into the page cache, in io_uring batches, the file pages of the BAL's accounts and storage
// in the domains that cold marks, and reports whether the storage batch read without error.
func prefetchBALState(ctx context.Context, db kv.RoDB, bal types.BlockAccessList, cold coldState) (storageRead bool) {
	started := time.Now()
	var accs, slots [][]byte
	if cold.storage {
		accs, slots = balPrefetchKeys(bal)
	} else {
		accs = make([][]byte, len(bal))
		for i := range bal {
			accs[i] = bal[i].Address[:]
		}
	}
	var wg sync.WaitGroup
	var accErr, slotErr error
	keys := 0
	if cold.accounts {
		keys += len(accs)
		wg.Go(func() { accErr = prefetchDomain(ctx, db, kv.AccountsDomain, accs) })
	}
	if cold.storage {
		keys += len(slots)
		wg.Go(func() { slotErr = prefetchDomain(ctx, db, kv.StorageDomain, slots) })
	}
	wg.Wait()
	logBatchPrefetch(ctx, "state", keys, started, firstFailure(accErr, slotErr))
	return cold.storage && slotErr == nil
}

// prefetchBALCommitment reads into the page cache, in io_uring batches, the file pages of the BAL's commitment trie,
// level by level since a branch decides whether the next level exists.
func prefetchBALCommitment(ctx context.Context, db kv.RoDB, keys [][]byte) {
	started := time.Now()
	logBatchPrefetch(ctx, "trie", len(keys), started, prefetchCommitment(ctx, db, keys))
}

func prefetchDomain(ctx context.Context, db kv.RoDB, domain kv.Domain, keys [][]byte) error {
	var err error
	if txErr := withTemporalTx(ctx, db, func(ttx kv.TemporalTx) { err = prefetchKeys(ctx, db, ttx, domain, keys) }); txErr != nil {
		return txErr
	}
	return err
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

// flatPrefixes returns the distinct compact prefixes of keys at each of the prefetchFlatDepth levels below start.
func flatPrefixes(keys [][]byte, start int) [][]byte {
	var prefixes [][]byte
	for depth := start; depth <= start+prefetchFlatDepth; depth++ {
		prefixes = append(prefixes, compactPrefixes(keys, depth)...)
	}
	return prefixes
}

func prefetchCommitment(ctx context.Context, db kv.RoDB, hashedKeys [][]byte) error {
	var err error
	txErr := withTemporalTx(ctx, db, func(ttx kv.TemporalTx) {
		p, ok := ttx.AggTx().(latestPrefetcher)
		if !ok || trieMostlyResident(ctx, p, hashedKeys) {
			return
		}
		if err = prefetchKeys(ctx, db, ttx, kv.CommitmentDomain, flatPrefixes(hashedKeys, 0)); err != nil {
			return
		}
		// The account trie walk loses a storage key at the extension above its account, so storage tries start again at their root.
		if err = walkTriePrefixes(ctx, db, ttx, hashedKeys, 0); err == nil {
			err = walkTriePrefixes(ctx, db, ttx, storageKeysOf(hashedKeys), storageTrieDepth)
		}
	})
	return cmp.Or(txErr, err)
}

const storageTrieDepth = 64

func storageKeysOf(hashedKeys [][]byte) [][]byte {
	var storageKeys [][]byte
	for _, k := range hashedKeys {
		if len(k) > storageTrieDepth {
			storageKeys = append(storageKeys, k)
		}
	}
	return storageKeys
}

// walkTriePrefixes prefetches the trie levels below the flat depth and stops at the first read error.
func walkTriePrefixes(ctx context.Context, db kv.RoDB, ttx kv.TemporalTx, keys [][]byte, start int) error {
	active := keys
	for depth := start; len(active) > 0 && depth <= commitment.WarmupMaxDepth; depth++ {
		if depth > prefetchFlatDepth {
			if err := prefetchKeys(ctx, db, ttx, kv.CommitmentDomain, compactPrefixes(active, depth)); err != nil {
				return err
			}
		}
		if depth >= prefetchFlatDepth {
			active = keysWithChild(ctx, db, ttx, active, depth)
		}
	}
	return nil
}

// stateKeysCold reports which state domains a sample of the BAL's keys finds cold: the batch prefetch reads those
// domains ahead of the per-key warmup.
func stateKeysCold(ctx context.Context, db kv.RoDB, bal types.BlockAccessList) coldState {
	var cold coldState
	_ = withTemporalTx(ctx, db, func(ttx kv.TemporalTx) {
		if p, ok := ttx.AggTx().(latestPrefetcher); ok {
			accs, slots := balPrefetchSample(bal)
			cold = stateSampleCold(ctx, p, accs, slots, len(bal), balSlotCount(bal))
		}
	})
	return cold
}

// stateSampleCold reports whether accounts or storage are cold enough for the batch to read them. It decides on every
// second sampled key, after probing the others: pages that all keys share are read by the first probe and would make a
// resident domain look cold. A sample that holds every key of its domain has read all its pages, and a failed sample
// read leaves the residency unknown: both skip the batch.
func stateSampleCold(ctx context.Context, p latestPrefetcher, accs, slots [][]byte, nAcc, nSlot int) coldState {
	cold := func(domain kv.Domain, sample [][]byte, total int) (bool, error) {
		first, second := make([][]byte, 0, (len(sample)+1)/2), make([][]byte, 0, len(sample)/2)
		for i, k := range sample {
			if i%2 == 0 {
				first = append(first, k)
			} else {
				second = append(second, k)
			}
		}
		if _, err := sampleCold(ctx, p, domain, first); err != nil {
			return false, err
		}
		isCold, err := sampleCold(ctx, p, domain, second)
		return isCold && len(sample) < total, err
	}
	var accCold, slotCold bool
	var accErr, slotErr error
	var wg sync.WaitGroup
	wg.Go(func() { accCold, accErr = cold(kv.AccountsDomain, accs, nAcc) })
	wg.Go(func() { slotCold, slotErr = cold(kv.StorageDomain, slots, nSlot) })
	wg.Wait()
	logSampleFailure(kv.AccountsDomain, accErr)
	logSampleFailure(kv.StorageDomain, slotErr)
	if accErr != nil || slotErr != nil {
		return coldState{}
	}
	return coldState{accounts: accCold, storage: slotCold}
}

// coldState marks the state domains whose sample was cold enough for the batch to read them.
type coldState struct{ accounts, storage bool }

func (c coldState) any() bool { return c.accounts || c.storage }

// balPrefetchSample returns the 2*residencySampleKeys sampleEvenly of balPrefetchKeys without building all keys, so the residency check does
// not delay the per-key warmup on a large BAL.
func balPrefetchSample(bal types.BlockAccessList) (accs, slots [][]byte) {
	nAcc, nSlot := len(bal), balSlotCount(bal)
	kAcc, kSlot := min(nAcc, 2*residencySampleKeys), min(nSlot, 2*residencySampleKeys)
	for j := range kAcc {
		accs = append(accs, bal[j*nAcc/kAcc].Address[:])
	}
	first, j := 0, 0 // index of the account's first slot, next sample
	for i := 0; i < len(bal) && j < kSlot; i++ {
		a := &bal[i]
		nChanges, n := len(a.StorageChanges), len(a.StorageChanges)+len(a.StorageReads)
		for ; j < kSlot; j++ {
			off := j*nSlot/kSlot - first
			if off >= n {
				break
			}
			if off < nChanges {
				slots = append(slots, dbutils.GenerateStoragePlainKey(a.Address, a.StorageChanges[off].Slot.Value()))
			} else {
				slots = append(slots, dbutils.GenerateStoragePlainKey(a.Address, a.StorageReads[off-nChanges]))
			}
		}
		first += n
	}
	return accs, slots
}

// balPrefetchKeys returns the account keys and the address||slot storage keys the BAL touches. Account keys alias
// bal's addresses; storage keys share one allocation, each capped at its own length so an append cannot reach the next.
func balPrefetchKeys(bal types.BlockAccessList) (accs, slots [][]byte) {
	n := balSlotCount(bal)
	const keyLen = length.Addr + length.Hash
	arena := make([]byte, 0, n*keyLen)
	accs, slots = make([][]byte, len(bal)), make([][]byte, 0, n)
	addKey := func(addr []byte, slot common.Hash) {
		start := len(arena)
		arena = append(append(arena, addr...), slot[:]...)
		slots = append(slots, arena[start:len(arena):len(arena)])
	}
	for i := range bal {
		addr := bal[i].Address[:]
		accs[i] = addr
		for _, sc := range bal[i].StorageChanges {
			addKey(addr, sc.Slot.Value())
		}
		for _, sr := range bal[i].StorageReads {
			addKey(addr, sr)
		}
	}
	return accs, slots
}

func balSlotCount(bal types.BlockAccessList) int {
	n := 0
	for i := range bal {
		n += len(bal[i].StorageChanges) + len(bal[i].StorageReads)
	}
	return n
}

// sampleEvenly returns min(len(keys), n) evenly spaced keys.
func sampleEvenly(keys [][]byte, n int) [][]byte {
	k := min(len(keys), n)
	sample := make([][]byte, k)
	for j := range sample {
		sample[j] = keys[j*len(keys)/k]
	}
	return sample
}

// sampleCold probes the sample keys, reading their cold pages, and reports whether at least coldPagesPerMille pages per
// 1000 keys were cold.
func sampleCold(ctx context.Context, p latestPrefetcher, domain kv.Domain, sample [][]byte) (bool, error) {
	if len(sample) == 0 {
		return false, nil
	}
	cold, err := p.PrefetchLatest(ctx, domain, sample)
	if errors.Is(err, kv.ErrPrefetchIncomplete) {
		err = nil // the pages it read are a lower bound of the cold ones
	}
	return cold*1000 >= len(sample)*coldPagesPerMille, err
}

const (
	residencySampleKeys = 256
	// coldPagesPerMille is the cold-page rate of a sample below which prefetching costs more planner CPU than the
	// reads it saves.
	coldPagesPerMille = 50
)

// trieMostlyResident probes the flat prefixes of evenly spaced keys and reports whether too few were cold to walk the
// trie; a failed probe also skips the walk.
func trieMostlyResident(ctx context.Context, p latestPrefetcher, hashedKeys [][]byte) bool {
	keys := sampleEvenly(hashedKeys, residencySampleKeys)
	// Slots of one account share every account-trie level, so only their storage trie can tell a cold trie.
	sample := append(flatPrefixes(keys, 0), flatPrefixes(storageKeysOf(keys), storageTrieDepth)...)
	isCold, err := sampleCold(ctx, p, kv.CommitmentDomain, sample)
	logSampleFailure(kv.CommitmentDomain, err)
	return err != nil || !isCold
}
