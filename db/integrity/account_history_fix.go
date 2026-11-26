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
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// FixResult counts the keys that a fix patches and the problems that it cannot fix.
type FixResult struct {
	Keys      int
	Unfixable int
}

var errUnfixable = errors.New("unfixable")

// FixCodeHash rewrites the code history and code domain files so that the code of each
// account matches its code hash, after every code hash change and in the latest state. With
// dryRun it only plans the patches.
func FixCodeHash(ctx context.Context, db kv.TemporalRoDB, dryRun bool, logger log.Logger) (FixResult, error) {
	addrs, findErr := codeHashProblemAccounts(ctx, db, logger)
	if findErr != nil {
		return FixResult{}, findErr
	}
	plan, err := planFix(ctx, db, kv.CodeDomain, logger, func(tx kv.TemporalTx, p *fixPlan) error {
		for _, addr := range addrs {
			if err := p.add(planAccountCode(tx, addr)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return FixResult{}, err
	}
	return plan.apply(ctx, db, dryRun, logger)
}

// FixStorageOfAbsentAccount rewrites the storage history files so that the storage of each
// destroyed account is wiped at the destroy. With dryRun it only plans the patches.
func FixStorageOfAbsentAccount(ctx context.Context, db kv.TemporalRoDB, dryRun bool, logger log.Logger) (FixResult, error) {
	absent, findErr := absentAccountsWithStorage(ctx, db, logger)
	if findErr != nil {
		return FixResult{}, findErr
	}
	plan, err := planFix(ctx, db, kv.StorageDomain, logger, func(tx kv.TemporalTx, p *fixPlan) error {
		for _, a := range absent {
			if err := planStorageWipes(tx, p, a.addr, a.txNums); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return FixResult{}, err
	}
	return plan.apply(ctx, db, dryRun, logger)
}

func codeHashProblemAccounts(ctx context.Context, db kv.TemporalRoDB, logger log.Logger) ([][]byte, error) {
	sc := SamplerCfg{SampleRatio: 1}
	var mu sync.Mutex
	set := map[string]struct{}{}
	add := func(addr []byte) {
		mu.Lock()
		defer mu.Unlock()
		set[string(addr)] = struct{}{}
	}
	err := walkAccountHistory(ctx, sc, db, 0, logger, CodeHash, func(tx kv.TemporalTx, addr []byte, txNum uint64, before, after []byte) error {
		codeHash := accounts.DeserialiseV3CodeHash(after)
		if bytes.Equal(accounts.DeserialiseV3CodeHash(before), codeHash) {
			return nil
		}
		code, _, err := tx.GetAsOf(kv.CodeDomain, addr, txNum+1)
		if err != nil {
			return err
		}
		if !codeMatchesHash(codeHash, code) {
			add(addr)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	err = scanLatestCode(ctx, sc, db, func(addr, codeHash, code []byte) error {
		if !codeMatchesHash(codeHash, code) {
			add(addr)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return sortedKeys(set), nil
}

type absentAccount struct {
	addr   []byte
	txNums []uint64
}

// absentAccountsWithStorage returns, for each account, the txNums at which it is created
// while it still has storage.
func absentAccountsWithStorage(ctx context.Context, db kv.TemporalRoDB, logger log.Logger) ([]absentAccount, error) {
	var mu sync.Mutex
	byAddr := map[string][]uint64{}
	err := walkAccountHistory(ctx, SamplerCfg{SampleRatio: 1}, db, 0, logger, StorageOfAbsentAccount, func(tx kv.TemporalTx, addr []byte, txNum uint64, before, _ []byte) error {
		if len(before) > 0 {
			return nil
		}
		_, found, err := firstStorageAsOf(tx, addr, txNum)
		if err != nil || !found {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		byAddr[string(addr)] = append(byAddr[string(addr)], txNum)
		return nil
	})
	if err != nil {
		return nil, err
	}
	res := make([]absentAccount, 0, len(byAddr))
	for _, addr := range sortedKeys(byAddr) {
		txNums := byAddr[string(addr)]
		slices.Sort(txNums)
		res = append(res, absentAccount{addr: addr, txNums: txNums})
	}
	return res, nil
}

// keyHistory is the history of one key, which the planner changes in memory.
type keyHistory struct {
	key           []byte
	txNums        []uint64
	values        [][]byte // the value before the change at txNums[i]
	latest        []byte
	changed       map[uint64]valueChange
	latestChanged bool
	latestWas     []byte
}

// valueChange is the original value of a changed history entry.
type valueChange struct {
	added bool
	was   []byte
}

func loadKeyHistory(tx kv.TemporalTx, domain kv.Domain, idx kv.InvertedIdx, key []byte) (*keyHistory, error) {
	txNums, err := readIndex(tx, idx, key)
	if err != nil {
		return nil, err
	}
	h := &keyHistory{key: bytes.Clone(key), txNums: txNums, values: make([][]byte, len(txNums)), changed: map[uint64]valueChange{}}
	for i, txNum := range txNums {
		var v []byte
		if v, _, err = tx.GetAsOf(domain, key, txNum); err != nil {
			return nil, err
		}
		h.values[i] = bytes.Clone(v)
	}
	latest, _, err := tx.GetLatest(domain, key, kv.GetLatestOptions{})
	if err != nil {
		return nil, err
	}
	h.latest = bytes.Clone(latest)
	return h, nil
}

func readIndex(tx kv.TemporalTx, idx kv.InvertedIdx, key []byte) ([]uint64, error) {
	it, err := tx.IndexRange(idx, key, -1, -1, order.Asc, -1)
	if err != nil {
		return nil, err
	}
	return stream.ToArrayU64(it)
}

// asOf returns the value before txNum, like GetAsOf.
func (h *keyHistory) asOf(txNum uint64) []byte {
	if i, _ := slices.BinarySearch(h.txNums, txNum); i < len(h.txNums) {
		return h.values[i]
	}
	return h.latest
}

// setChange makes the key change to v at txNum.
func (h *keyHistory) setChange(txNum uint64, v []byte) {
	i, found := slices.BinarySearch(h.txNums, txNum)
	if !found {
		h.values = slices.Insert(h.values, i, h.asOf(txNum))
		h.txNums = slices.Insert(h.txNums, i, txNum)
		h.changed[txNum] = valueChange{added: true}
	}
	if i+1 < len(h.txNums) {
		h.setValue(i+1, v)
		return
	}
	h.setLatest(v)
}

func (h *keyHistory) setValue(i int, v []byte) {
	if _, ok := h.changed[h.txNums[i]]; !ok {
		h.changed[h.txNums[i]] = valueChange{was: h.values[i]}
	}
	h.values[i] = v
}

func (h *keyHistory) setLatest(v []byte) {
	if !h.latestChanged {
		h.latestWas, h.latestChanged = h.latest, true
	}
	h.latest = v
}

// withCodeHash returns a code of the history of addr whose hash is codeHash.
func (h *keyHistory) withCodeHash(addr, codeHash []byte) ([]byte, error) {
	if codeMatchesHash(codeHash, nil) {
		return nil, nil
	}
	for _, v := range append(slices.Clone(h.values), h.latest) {
		if len(v) > 0 && codeMatchesHash(codeHash, v) {
			return v, nil
		}
	}
	return nil, fmt.Errorf("%w: account %x: no code of hash %x in its code history", errUnfixable, addr, codeHash)
}

// planAccountCode changes the code history of addr so that the code matches the code hash
// after each code hash change of the account and in the latest state.
func planAccountCode(tx kv.TemporalTx, addr []byte) (*keyHistory, error) {
	code, loadErr := loadKeyHistory(tx, kv.CodeDomain, kv.CodeHistoryIdx, addr)
	if loadErr != nil {
		return nil, loadErr
	}
	changes, changesErr := codeHashChanges(tx, addr)
	if changesErr != nil {
		return nil, changesErr
	}
	for _, c := range changes {
		if codeMatchesHash(c.codeHash, code.asOf(c.txNum+1)) {
			continue
		}
		v, err := code.withCodeHash(addr, c.codeHash)
		if err != nil {
			return nil, err
		}
		code.setChange(c.txNum, v)
	}
	acc, _, accErr := tx.GetLatest(kv.AccountsDomain, addr, kv.GetLatestOptions{})
	if accErr != nil {
		return nil, accErr
	}
	if codeHash := accounts.DeserialiseV3CodeHash(acc); !codeMatchesHash(codeHash, code.latest) {
		v, err := code.withCodeHash(addr, codeHash)
		if err != nil {
			return nil, err
		}
		code.setLatest(v)
	}
	return code, nil
}

type codeHashChange struct {
	txNum    uint64
	codeHash []byte
}

// codeHashChanges returns the changes of the account that change its code hash, with the
// code hash after each change.
func codeHashChanges(tx kv.TemporalTx, addr []byte) ([]codeHashChange, error) {
	txNums, idxErr := readIndex(tx, kv.AccountsHistoryIdx, addr)
	if idxErr != nil || len(txNums) == 0 {
		return nil, idxErr
	}
	first, _, firstErr := tx.GetAsOf(kv.AccountsDomain, addr, txNums[0])
	if firstErr != nil {
		return nil, firstErr
	}
	var changes []codeHashChange
	before := bytes.Clone(accounts.DeserialiseV3CodeHash(first))
	for i, txNum := range txNums {
		next := txNum + 1
		if i+1 < len(txNums) {
			next = txNums[i+1]
		}
		after, _, err := tx.GetAsOf(kv.AccountsDomain, addr, next)
		if err != nil {
			return nil, err
		}
		codeHash := bytes.Clone(accounts.DeserialiseV3CodeHash(after))
		if !bytes.Equal(before, codeHash) {
			changes = append(changes, codeHashChange{txNum: txNum, codeHash: codeHash})
		}
		before = codeHash
	}
	return changes, nil
}

// planStorageWipes wipes the storage of addr at the destroy before each txNum, at which the
// account is created while it still has storage.
func planStorageWipes(tx kv.TemporalTx, p *fixPlan, addr []byte, created []uint64) error {
	accTxNums, err := readIndex(tx, kv.AccountsHistoryIdx, addr)
	if err != nil {
		return err
	}
	slots := map[string]*keyHistory{}
	for _, txNum := range created {
		destroy, found, err := destroyBefore(tx, addr, accTxNums, txNum)
		if err != nil {
			return err
		}
		if !found {
			p.unfixable(fmt.Errorf("%w: account %x is absent before txNum %d and has storage, but has no destroy before", errUnfixable, addr, txNum))
			continue
		}
		keys, err := storageKeysAsOf(tx, addr, destroy+1)
		if err != nil {
			return err
		}
		for _, k := range keys {
			slot, ok := slots[string(k)]
			if !ok {
				if slot, err = loadKeyHistory(tx, kv.StorageDomain, kv.StorageHistoryIdx, k); err != nil {
					return err
				}
				slots[string(k)] = slot
			}
			if len(slot.asOf(destroy+1)) > 0 {
				slot.setChange(destroy, nil)
			}
		}
	}
	for _, k := range sortedKeys(slots) {
		slot := slots[string(k)]
		if slot.latestChanged {
			p.unfixable(fmt.Errorf("%w: storage %x is kept after the destroy of its account in the latest state", errUnfixable, k))
			continue
		}
		if err := p.add(slot, nil); err != nil {
			return err
		}
	}
	return nil
}

// destroyBefore returns the last change of addr before txNum that removes the account.
func destroyBefore(tx kv.TemporalTx, addr []byte, accTxNums []uint64, txNum uint64) (uint64, bool, error) {
	end, _ := slices.BinarySearch(accTxNums, txNum)
	for i := end - 1; i >= 0; i-- {
		before, _, err := tx.GetAsOf(kv.AccountsDomain, addr, accTxNums[i])
		if err != nil {
			return 0, false, err
		}
		if len(before) > 0 {
			return accTxNums[i], true, nil
		}
	}
	return 0, false, nil
}

func storageKeysAsOf(tx kv.TemporalTx, addr []byte, txNum uint64) ([][]byte, error) {
	to, _ := kv.NextSubtree(addr)
	it, err := tx.RangeAsOf(kv.StorageDomain, addr, to, txNum, order.Asc, -1)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var keys [][]byte
	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return nil, err
		}
		if len(v) > 0 {
			keys = append(keys, bytes.Clone(k))
		}
	}
	return keys, nil
}

type fixPlan struct {
	domain    kv.Domain
	filesEnd  uint64
	tx        kv.TemporalTx
	history   []state.HistoryPatch
	latest    []state.LatestPatch
	res       FixResult
	logger    log.Logger
	maxLogged int
}

func planFix(ctx context.Context, db kv.TemporalRoDB, domain kv.Domain, logger log.Logger, plan func(tx kv.TemporalTx, p *fixPlan) error) (*fixPlan, error) {
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	p := &fixPlan{domain: domain, filesEnd: tx.Debug().TxNumsInFiles(domain), tx: tx, logger: logger, maxLogged: 100}
	if err := plan(tx, p); err != nil {
		return nil, err
	}
	p.tx = nil
	return p, nil
}

func (p *fixPlan) unfixable(err error) {
	p.res.Unfixable++
	p.logger.Warn(fmt.Sprintf("[fix] %s", p.domain), "err", err)
}

// add adds the changes of h, unless one of them is not in the files.
func (p *fixPlan) add(h *keyHistory, err error) error {
	if errors.Is(err, errUnfixable) {
		p.unfixable(err)
		return nil
	}
	if err != nil {
		return err
	}
	if len(h.changed) == 0 && !h.latestChanged {
		return nil
	}
	var history []state.HistoryPatch
	for i, txNum := range h.txNums {
		if _, ok := h.changed[txNum]; !ok {
			continue
		}
		if txNum >= p.filesEnd {
			p.unfixable(fmt.Errorf("%w: %x at txNum %d is not in files", errUnfixable, h.key, txNum))
			return nil
		}
		history = append(history, state.HistoryPatch{Key: h.key, TxNum: txNum, Value: h.values[i]})
	}
	if h.latestChanged {
		_, _, inDB, err := p.tx.Debug().GetLatestFromDB(p.domain, h.key)
		if err != nil {
			return err
		}
		if inDB {
			p.unfixable(fmt.Errorf("%w: latest %x is in the database", errUnfixable, h.key))
			return nil
		}
		p.latest = append(p.latest, state.LatestPatch{Key: h.key, Value: h.latest})
	}
	p.history = append(p.history, history...)
	p.res.Keys++
	if p.res.Keys <= p.maxLogged {
		p.log(h)
	}
	return nil
}

func (p *fixPlan) log(h *keyHistory) {
	for i, txNum := range h.txNums {
		c, ok := h.changed[txNum]
		if !ok {
			continue
		}
		was := fmt.Sprintf("%x", common.Shorten(c.was, 32))
		if c.added {
			was = "added"
		}
		p.logger.Info(fmt.Sprintf("[fix] %s history", p.domain), "key", fmt.Sprintf("%x", h.key), "txNum", txNum, "was", was, "now", fmt.Sprintf("%x", common.Shorten(h.values[i], 32)))
	}
	if h.latestChanged {
		p.logger.Info(fmt.Sprintf("[fix] %s latest", p.domain), "key", fmt.Sprintf("%x", h.key), "was", fmt.Sprintf("%x", common.Shorten(h.latestWas, 32)), "now", fmt.Sprintf("%x", common.Shorten(h.latest, 32)))
	}
}

func (p *fixPlan) apply(ctx context.Context, db kv.TemporalRoDB, dryRun bool, logger log.Logger) (FixResult, error) {
	logger.Info(fmt.Sprintf("[fix] %s planned", p.domain), "keys", p.res.Keys, "historyPatches", len(p.history), "latestPatches", len(p.latest), "unfixable", p.res.Unfixable, "dryRun", dryRun)
	if dryRun || p.res.Keys == 0 {
		return p.res, nil
	}
	hasAgg, ok := db.(state.HasAgg)
	if !ok {
		return p.res, fmt.Errorf("%T has no aggregator", db)
	}
	agg, ok := hasAgg.Agg().(*state.Aggregator)
	if !ok {
		return p.res, fmt.Errorf("%T is not an aggregator", hasAgg.Agg())
	}
	return p.res, agg.PatchDomainFiles(ctx, p.domain, p.history, p.latest, logger)
}

func sortedKeys[V any](m map[string]V) [][]byte {
	keys := make([][]byte, 0, len(m))
	for k := range m {
		keys = append(keys, []byte(k))
	}
	slices.SortFunc(keys, func(a, b []byte) int { return cmp.Compare(string(a), string(b)) })
	return keys
}
