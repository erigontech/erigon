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
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/estimate"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// CheckStorageOfAbsentAccount checks that an account absent before a txNum has no storage
// as of that txNum. A destroyed account's storage history must record the wipe in the
// destroying tx.
func CheckStorageOfAbsentAccount(ctx context.Context, sc SamplerCfg, db kv.TemporalRoDB, failFast bool, fromStep uint64, logger log.Logger) error {
	var probs problems
	err := walkAccountHistory(ctx, sc, db, fromStep, logger, StorageOfAbsentAccount, func(tx kv.TemporalTx, addr []byte, txNum uint64, before, _ []byte) error {
		if len(before) > 0 {
			return nil
		}
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
	if err := checkLatestCodeHash(ctx, sc, db, failFast, &probs); err != nil {
		return err
	}
	return probs.verdict(string(CodeHash))
}

func checkLatestCodeHash(ctx context.Context, sc SamplerCfg, db kv.TemporalRoDB, failFast bool, probs *problems) error {
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
				if err := reportCodeMismatch(probs, failFast, addr, accounts.DeserialiseV3CodeHash(acc), code, "in the latest state"); err != nil {
					return err
				}
			}
			return ctx.Err()
		})
	}
	return g.Wait()
}

func reportCodeMismatch(probs *problems, failFast bool, addr, codeHash, code []byte, when string) error {
	if codeHash == nil && len(code) == 0 {
		return nil
	}
	if codeHash != nil && crypto.Keccak256Hash(code) == common.Hash(codeHash) {
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
	addr   []byte
	txNums []uint64
}

// walkAccountHistory calls visit for every change of the sampled accounts in the accounts
// history files, with the account before and after the change. Each file is scanned by one
// goroutine, and the sampled accounts are visited by a pool of workers.
func walkAccountHistory(ctx context.Context, sc SamplerCfg, db kv.TemporalRoDB, fromStep uint64, logger log.Logger, check Check, visit accountChangeVisitor) error {
	files, err := accountHistoryFiles(ctx, db, fromStep)
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

func accountHistoryFiles(ctx context.Context, db kv.TemporalRoDB, fromStep uint64) ([]kv.VisibleFile, error) {
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	fromTxNum := fromStep * tx.Debug().StepSize()
	var files []kv.VisibleFile
	for _, f := range tx.Debug().DomainFiles(kv.AccountsDomain) {
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
	it, err := tx.Debug().HistoryKeyTxNumRange(kv.AccountsDomain, int(f.StartRootNum()), int(f.EndRootNum()), order.Asc, -1)
	if err != nil {
		return err
	}
	defer it.Close()

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
	for it.HasNext() {
		k, txNum, err := it.Next()
		if err != nil {
			return err
		}
		if !bytes.Equal(k, addr) {
			addr = append(addr[:0], k...)
			if sampled = !sampler.CanSkip(); sampled {
				if batchLen >= batchChanges {
					if err := send(); err != nil {
						return err
					}
				}
				batch = append(batch, accountChanges{addr: bytes.Clone(k)})
			}
		}
		if sampled {
			last := &batch[len(batch)-1]
			last.txNums = append(last.txNums, txNum)
			batchLen++
		}
	}
	return send()
}

// visitAccountChanges reads each change's after-value as the next change's before-value, so
// an account costs one history read per change.
func visitAccountChanges(tx kv.TemporalTx, acc accountChanges, visit accountChangeVisitor) error {
	before, _, err := tx.GetAsOf(kv.AccountsDomain, acc.addr, acc.txNums[0])
	if err != nil {
		return err
	}
	before = bytes.Clone(before)
	for i, txNum := range acc.txNums {
		nextTxNum := txNum + 1
		if i+1 < len(acc.txNums) {
			nextTxNum = acc.txNums[i+1]
		}
		after, _, err := tx.GetAsOf(kv.AccountsDomain, acc.addr, nextTxNum)
		if err != nil {
			return err
		}
		after = bytes.Clone(after)
		if err := visit(tx, acc.addr, txNum, before, after); err != nil {
			return err
		}
		before = after
	}
	return nil
}
