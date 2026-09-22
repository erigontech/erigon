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
	"errors"
	"slices"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
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
		address := account.Address.Value()
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
	keys := balCommitmentWarmupKeys(bal)
	if len(keys) == 0 || workers <= 0 {
		return nil
	}
	workers = min(workers, len(keys))
	cacheStats := new(balCommitmentCacheStats)
	log.Info("[warmBAL] commitment warmup started", "keys", len(keys), "workers", workers)
	started := time.Now()

	factoryErrs := make(chan error, workers)
	factory := func(workerCtx context.Context) (commitment.PatriciaContext, func()) {
		tx, err := db.BeginRo(kv.WithNonBlockingAcquire(workerCtx)) //nolint:gocritic // The returned cleanup owns Rollback.
		if err != nil {
			factoryErrs <- err
			return nil, nil
		}
		txTemporal, ok := tx.(kv.TemporalTx)
		if !ok {
			tx.Rollback()
			factoryErrs <- errors.New("BAL commitment warmup requires a temporal read transaction")
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

	warmuper := commitment.NewWarmuper(ctx, commitment.WarmupConfig{
		Enabled:    true,
		CtxFactory: factory,
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
	for factoryErr := range factoryErrs {
		err = errors.Join(err, factoryErr)
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
