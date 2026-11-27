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

package integrity

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/estimate"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// CheckStorageOfAbsentAccount checks that an account absent before a txNum has no storage
// as of that txNum. A destroyed account's storage history must record the wipe in the
// destroying tx.
func CheckStorageOfAbsentAccount(ctx context.Context, sc SamplerCfg, db kv.TemporalRoDB, failFast bool, fromStep uint64, logger log.Logger) error {
	var probs problems
	err := walkAbsentAccounts(ctx, sc, db, fromStep, logger, func(tx kv.TemporalTx, addr []byte, txNum uint64) error {
		slot, found, err := firstStorageAsOf(tx, addr, txNum)
		if err != nil || !found {
			return err
		}
		return probs.report(failFast, fmt.Errorf("%w: %s: account %x is absent before txNum %d, but its slot %x is set",
			ErrIntegrity, StorageOfAbsentAccount, addr, txNum, slot))
	})
	if err != nil {
		return err
	}
	return probs.verdict(string(StorageOfAbsentAccount))
}

type absentAccountChange struct {
	addr  []byte
	txNum uint64
}

// walkAbsentAccounts calls visit for each change in the accounts history files that creates
// a sampled account which has a key in some storage history file. Only such an account can
// have storage when it is absent, so the other accounts are not visited. The history files
// are read sequentially, each by one goroutine, and the changes are visited by a pool of
// workers.
func walkAbsentAccounts(ctx context.Context, sc SamplerCfg, db kv.TemporalRoDB, fromStep uint64, logger log.Logger, visit func(tx kv.TemporalTx, addr []byte, txNum uint64) error) error {
	start := time.Now()
	owners, err := storageOwners(ctx, db, logger)
	if err != nil {
		return err
	}
	files, err := historyIndexFiles(ctx, db, kv.AccountsDomain, fromStep)
	if err != nil {
		return err
	}
	var scanned, visited atomic.Uint64
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	progress := func() {
		select {
		case <-logEvery.C:
			logger.Info(fmt.Sprintf("[integrity] %s", StorageOfAbsentAccount), "scannedFiles", fmt.Sprintf("%d/%d", scanned.Load(), len(files)), "creations", common.PrettyCounter(visited.Load()))
		default:
		}
	}

	work := make(chan []absentAccountChange, 1024)
	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(work)
		scans, scanCtx := errgroup.WithContext(gCtx)
		scans.SetLimit(estimate.AlmostAllCPUs())
		for _, f := range files {
			scans.Go(func() error {
				defer scanned.Add(1)
				return scanAbsentAccounts(scanCtx, sc.NewWindowSampler(f.StartRootNum()), db, f, owners, work)
			})
		}
		return scans.Wait()
	})
	for range estimate.AlmostAllCPUs() {
		g.Go(func() error { return visitAbsentAccounts(gCtx, db, work, visit, &visited, progress) })
	}
	err = g.Wait()
	logger.Info(fmt.Sprintf("[integrity] %s: history walked", StorageOfAbsentAccount), "files", len(files), "creations", common.PrettyCounter(visited.Load()), "took", time.Since(start))
	return err
}

func visitAbsentAccounts(ctx context.Context, db kv.TemporalRoDB, work <-chan []absentAccountChange, visit func(tx kv.TemporalTx, addr []byte, txNum uint64) error, visited *atomic.Uint64, progress func()) error {
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for batch := range work {
		for _, c := range batch {
			if err := visit(tx, c.addr, c.txNum); err != nil {
				return err
			}
		}
		visited.Add(uint64(len(batch)))
		if err := ctx.Err(); err != nil {
			return err
		}
		progress()
	}
	return nil
}

// storageOwners returns the sorted addresses that have a key in some storage history file.
func storageOwners(ctx context.Context, db kv.TemporalRoDB, logger log.Logger) ([]common.Address, error) {
	start := time.Now()
	files, err := historyIndexFiles(ctx, db, kv.StorageDomain, 0)
	if err != nil {
		return nil, err
	}
	var mu sync.Mutex
	var owners []common.Address
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(estimate.AlmostAllCPUs())
	for _, f := range files {
		g.Go(func() error {
			tx, err := db.BeginTemporalRo(gCtx)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			var addrs []common.Address
			err = state.AggTx(tx).ScanHistoryKeys(gCtx, kv.StorageDomain, f.StartRootNum(), f.EndRootNum(), func(k []byte) error {
				if len(k) < length.Addr {
					return fmt.Errorf("storage key %x is shorter than an address", k)
				}
				if a := common.Address(k[:length.Addr]); len(addrs) == 0 || addrs[len(addrs)-1] != a {
					addrs = append(addrs, a)
				}
				return nil
			})
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			owners = mergeSortedAddrs(owners, addrs)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	logger.Info(fmt.Sprintf("[integrity] %s: accounts with storage", StorageOfAbsentAccount), "files", len(files), "accounts", common.PrettyCounter(len(owners)), "took", time.Since(start))
	return owners, nil
}

// mergeSortedAddrs merges two sorted lists of distinct addresses into one.
func mergeSortedAddrs(a, b []common.Address) []common.Address {
	res := make([]common.Address, 0, len(a)+len(b))
	for len(a) > 0 && len(b) > 0 {
		switch c := a[0].Cmp(b[0]); {
		case c < 0:
			res, a = append(res, a[0]), a[1:]
		case c > 0:
			res, b = append(res, b[0]), b[1:]
		default:
			res, a, b = append(res, a[0]), a[1:], b[1:]
		}
	}
	return append(append(res, a...), b...)
}

// scanAbsentAccounts sends to work each change of the accounts history file f whose value
// before is empty, for the sampled accounts in owners.
func scanAbsentAccounts(ctx context.Context, sampler *Sampler, db kv.TemporalRoDB, f kv.VisibleFile, owners []common.Address, work chan<- []absentAccountChange) error {
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	const batchSize = 4096
	var batch []absentAccountChange
	send := func() error {
		if len(batch) == 0 {
			return nil
		}
		select {
		case work <- batch:
		case <-ctx.Done():
			return ctx.Err()
		}
		batch = nil
		return nil
	}
	var addr []byte
	next, sampled := 0, false
	err = state.AggTx(tx).ScanHistory(ctx, kv.AccountsDomain, f.StartRootNum(), f.EndRootNum(), func(k []byte, txNum uint64, before []byte) error {
		if !bytes.Equal(k, addr) {
			addr = append(addr[:0], k...)
			for next < len(owners) && bytes.Compare(owners[next][:], k) < 0 {
				next++
			}
			sampled = next < len(owners) && bytes.Equal(owners[next][:], k) && !sampler.CanSkip()
		}
		if !sampled || len(before) > 0 {
			return nil
		}
		if batch = append(batch, absentAccountChange{addr: bytes.Clone(k), txNum: txNum}); len(batch) >= batchSize {
			return send()
		}
		return nil
	})
	if err != nil {
		return err
	}
	return send()
}

func firstStorageAsOf(tx kv.TemporalTx, addr []byte, txNum uint64) (slot []byte, found bool, err error) {
	to, _ := kv.NextSubtree(addr)
	it, err := tx.RangeAsOf(kv.StorageDomain, addr, to, txNum, order.Asc, -1)
	if err != nil {
		return nil, false, err
	}
	defer it.Close()
	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return nil, false, err
		}
		if len(v) > 0 {
			return bytes.Clone(k[len(addr):]), true, nil
		}
	}
	return nil, false, nil
}

// CheckCodeHash checks that the code domain holds the code of the account's code hash: in
// the latest state, and after every code hash change in the accounts history.
func CheckCodeHash(ctx context.Context, sc SamplerCfg, db kv.TemporalRoDB, failFast bool, fromStep uint64, logger log.Logger) error {
	var probs problems
	err := walkAccountHistory(ctx, sc, db, fromStep, logger, CodeHash, func(tx kv.TemporalTx, addr []byte, txNum uint64, before, after []byte) error {
		codeHash := accounts.DeserialiseV3CodeHash(after)
		if bytes.Equal(accounts.DeserialiseV3CodeHash(before), codeHash) {
			return nil
		}
		code, _, err := tx.GetAsOf(kv.CodeDomain, addr, txNum+1)
		if err != nil {
			return err
		}
		return reportCodeMismatch(&probs, failFast, addr, codeHash, code, fmt.Sprintf("after txNum %d", txNum))
	})
	if err != nil {
		return err
	}
	err = scanLatestCode(ctx, sc, db, func(addr, codeHash, code []byte) error {
		return reportCodeMismatch(&probs, failFast, addr, codeHash, code, "in the latest state")
	})
	if err != nil {
		return err
	}
	return probs.verdict(string(CodeHash))
}

// scanLatestCode calls visit with the code hash of every sampled account that has code in
// the latest state.
func scanLatestCode(ctx context.Context, sc SamplerCfg, db kv.TemporalRoDB, visit func(addr, codeHash, code []byte) error) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(estimate.AlmostAllCPUs())
	for first := range 256 {
		g.Go(func() error {
			tx, err := db.BeginTemporalRo(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			from := []byte{byte(first)}
			to, _ := kv.NextSubtree(from)
			it, err := tx.Debug().RangeLatest(kv.CodeDomain, from, to, -1)
			if err != nil {
				return err
			}
			defer it.Close()
			sampler := sc.NewWindowSampler(uint64(first))
			for it.HasNext() {
				addr, code, err := it.Next()
				if err != nil {
					return err
				}
				if sampler.CanSkip() {
					continue
				}
				acc, _, err := tx.GetLatest(kv.AccountsDomain, addr, kv.GetLatestOptions{})
				if err != nil {
					return err
				}
				if err := visit(addr, accounts.DeserialiseV3CodeHash(acc), code); err != nil {
					return err
				}
			}
			return ctx.Err()
		})
	}
	return g.Wait()
}

func codeMatchesHash(codeHash, code []byte) bool {
	if codeHash == nil {
		return len(code) == 0
	}
	return crypto.Keccak256Hash(code) == common.Hash(codeHash)
}

func reportCodeMismatch(probs *problems, failFast bool, addr, codeHash, code []byte, when string) error {
	if codeMatchesHash(codeHash, code) {
		return nil
	}
	want := "no code"
	if codeHash != nil {
		want = fmt.Sprintf("code hash %x", codeHash)
	}
	return probs.report(failFast, fmt.Errorf("%w: %s: account %x %s has %s, but code %x",
		ErrIntegrity, CodeHash, addr, when, want, common.Shorten(code, 32)))
}

type accountChangeVisitor func(tx kv.TemporalTx, addr []byte, txNum uint64, before, after []byte) error

type accountChanges struct {
	addr    []byte
	txNums  []uint64
	befores [][]byte // the account before the change at txNums[i]
}

// walkAccountHistory calls visit for every change of the sampled accounts in the accounts
// history files, with the account before and after the change. Each file is read
// sequentially by one goroutine, and the sampled accounts are visited by a pool of workers.
func walkAccountHistory(ctx context.Context, sc SamplerCfg, db kv.TemporalRoDB, fromStep uint64, logger log.Logger, check Check, visit accountChangeVisitor) error {
	files, err := historyIndexFiles(ctx, db, kv.AccountsDomain, fromStep)
	if err != nil {
		return err
	}
	start := time.Now()
	var scanned, changes atomic.Uint64
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	progress := func() {
		select {
		case <-logEvery.C:
			logger.Info(fmt.Sprintf("[integrity] %s", check), "scannedFiles", fmt.Sprintf("%d/%d", scanned.Load(), len(files)), "changes", common.PrettyCounter(changes.Load()))
		default:
		}
	}

	work := make(chan []accountChanges, 1024)
	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(work)
		scans, scanCtx := errgroup.WithContext(gCtx)
		scans.SetLimit(estimate.AlmostAllCPUs())
		for _, f := range files {
			scans.Go(func() error {
				defer scanned.Add(1)
				return scanAccountHistoryFile(scanCtx, sc.NewWindowSampler(f.StartRootNum()), db, f, work)
			})
		}
		return scans.Wait()
	})
	for range estimate.AlmostAllCPUs() {
		g.Go(func() error { return visitAccountWork(gCtx, db, work, visit, &changes, progress) })
	}
	err = g.Wait()
	logger.Info(fmt.Sprintf("[integrity] %s: history walked", check), "files", len(files), "changes", common.PrettyCounter(changes.Load()), "took", time.Since(start))
	return err
}

func historyIndexFiles(ctx context.Context, db kv.TemporalRoDB, domain kv.Domain, fromStep uint64) ([]kv.VisibleFile, error) {
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	fromTxNum := fromStep * tx.Debug().StepSize()
	var files []kv.VisibleFile
	for _, f := range tx.Debug().DomainFiles(domain) {
		if strings.HasSuffix(f.Fullpath(), ".ef") && f.EndRootNum() > fromTxNum {
			files = append(files, f)
		}
	}
	return files, nil
}

func visitAccountWork(ctx context.Context, db kv.TemporalRoDB, work <-chan []accountChanges, visit accountChangeVisitor, changes *atomic.Uint64, progress func()) error {
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for batch := range work {
		for _, acc := range batch {
			if err := visitAccountChanges(tx, acc, visit); err != nil {
				return err
			}
			changes.Add(uint64(len(acc.txNums)))
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		progress()
	}
	return nil
}

func scanAccountHistoryFile(ctx context.Context, sampler *Sampler, db kv.TemporalRoDB, f kv.VisibleFile, work chan<- []accountChanges) error {
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	const batchChanges = 4096
	var batch []accountChanges
	var batchLen int
	send := func() error {
		if len(batch) == 0 {
			return nil
		}
		select {
		case work <- batch:
		case <-ctx.Done():
			return ctx.Err()
		}
		batch, batchLen = nil, 0
		return nil
	}
	sampled := false
	var addr []byte
	err = state.AggTx(tx).ScanHistory(ctx, kv.AccountsDomain, f.StartRootNum(), f.EndRootNum(), func(k []byte, txNum uint64, before []byte) error {
		if !bytes.Equal(k, addr) {
			addr = append(addr[:0], k...)
			if sampled = !sampler.CanSkip(); sampled {
				if batchLen >= batchChanges {
					if sendErr := send(); sendErr != nil {
						return sendErr
					}
				}
				batch = append(batch, accountChanges{addr: bytes.Clone(k)})
			}
		}
		if sampled {
			last := &batch[len(batch)-1]
			last.txNums, last.befores = append(last.txNums, txNum), append(last.befores, bytes.Clone(before))
			batchLen++
		}
		return nil
	})
	if err != nil {
		return err
	}
	return send()
}

// visitAccountChanges takes each change's after-value from the next change's before-value,
// so an account costs one history read per file: the after-value of its last change.
func visitAccountChanges(tx kv.TemporalTx, acc accountChanges, visit accountChangeVisitor) error {
	last := len(acc.txNums) - 1
	for i, txNum := range acc.txNums[:last] {
		if err := visit(tx, acc.addr, txNum, acc.befores[i], acc.befores[i+1]); err != nil {
			return err
		}
	}
	after, _, err := tx.GetAsOf(kv.AccountsDomain, acc.addr, acc.txNums[last]+1)
	if err != nil {
		return err
	}
	return visit(tx, acc.addr, acc.txNums[last], acc.befores[last], bytes.Clone(after))
}
