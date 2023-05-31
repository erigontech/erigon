package state

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/log/v3"
	btree2 "github.com/tidwall/btree"

	"github.com/ledgerwatch/erigon/cmd/state/exec22"
	"github.com/ledgerwatch/erigon/core/types/accounts"

	"github.com/ledgerwatch/erigon-lib/commitment"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
)

type KVList struct {
	TxNum []uint64
	Vals  [][]byte
}

func NewKVList() *KVList {
	return &KVList{
		TxNum: make([]uint64, 0, 16),
		Vals:  make([][]byte, 0, 16),
	}
}

func (l *KVList) Latest() (tx uint64, v []byte) {
	sz := len(l.TxNum)
	if sz == 0 {
		return 0, nil
	}
	sz--

	tx = l.TxNum[sz]
	v = l.Vals[sz]
	return tx, v
}

func (l *KVList) Put(tx uint64, v []byte) (prevTx uint64, prevV []byte) {
	prevTx, prevV = l.Latest()
	l.TxNum = append(l.TxNum, tx)
	l.Vals = append(l.Vals, common.Copy(v))
	return
}

func (l *KVList) Len() int {
	return len(l.TxNum)
}

func (l *KVList) Apply(f func(txn uint64, v []byte, isLatest bool) error) error {
	for i, tx := range l.TxNum {
		if err := f(tx, l.Vals[i], i == len(l.TxNum)-1); err != nil {
			return err
		}
	}
	return nil
}

func (l *KVList) Reset() {
	if len(l.TxNum) > 0 {
		topNum := l.TxNum[len(l.TxNum)-1]
		topVal := l.Vals[len(l.Vals)-1]
		defer l.Put(topNum, topVal) // store the latest value
	}
	l.TxNum = l.TxNum[:0]
	l.Vals = l.Vals[:0]
}

func splitKey(key []byte) (k1, k2 []byte) {
	switch {
	case len(key) <= length.Addr:
		return key, nil
	case len(key) >= length.Addr+length.Hash:
		return key[:length.Addr], key[length.Addr:]
	default:
		panic(fmt.Sprintf("invalid key length %d", len(key)))
	}
	return
}

type SharedDomains struct {
	aggCtx *AggregatorV3Context
	roTx   kv.Tx

	txNum    atomic.Uint64
	blockNum atomic.Uint64
	estSize  atomic.Uint64
	updates  *UpdatesWithCommitment

	sync.RWMutex
	account    *btree2.Map[string, []byte]
	code       *btree2.Map[string, []byte]
	storage    *btree2.Map[string, []byte]
	commitment *btree2.Map[string, []byte]
	Account    *Domain
	Storage    *Domain
	Code       *Domain
	Commitment *DomainCommitted
}

func (sd *SharedDomains) Updates() *UpdatesWithCommitment {
	return sd.updates
}

func NewSharedDomains(a, c, s *Domain, comm *DomainCommitted) *SharedDomains {
	sd := &SharedDomains{
		Account:    a,
		account:    btree2.NewMap[string, []byte](128),
		Code:       c,
		code:       btree2.NewMap[string, []byte](128),
		Storage:    s,
		storage:    btree2.NewMap[string, []byte](128),
		Commitment: comm,
		commitment: btree2.NewMap[string, []byte](128),
	}

	sd.updates = NewUpdatesWithCommitment(sd)
	sd.Commitment.ResetFns(sd.BranchFn, sd.AccountFn, sd.StorageFn)
	return sd
}

func (sd *SharedDomains) AddUpdates(keys [][]byte, updates []commitment.Update) error {
	sd.Lock()
	defer sd.Unlock()
	sd.updates.AddUpdates(keys, updates)
	return nil
}

func (sd *SharedDomains) put(table string, key, val []byte) {
	sd.puts(table, hex.EncodeToString(key), val)
}

func (sd *SharedDomains) puts(table string, key string, val []byte) {
	switch table {
	case kv.AccountDomain:
		if old, ok := sd.account.Set(key, val); ok {
			sd.estSize.Add(uint64(len(val) - len(old)))
		} else {
			sd.estSize.Add(uint64(len(key) + len(val)))
		}
	case kv.CodeDomain:
		if old, ok := sd.code.Set(key, val); ok {
			sd.estSize.Add(uint64(len(val) - len(old)))
		} else {
			sd.estSize.Add(uint64(len(key) + len(val)))
		}
	case kv.StorageDomain:
		if old, ok := sd.storage.Set(key, val); ok {
			sd.estSize.Add(uint64(len(val) - len(old)))
		} else {
			sd.estSize.Add(uint64(len(key) + len(val)))
		}
	case kv.CommitmentDomain:
		if old, ok := sd.commitment.Set(key, val); ok {
			sd.estSize.Add(uint64(len(val) - len(old)))
		} else {
			sd.estSize.Add(uint64(len(key) + len(val)))
		}
	default:
		panic(fmt.Errorf("sharedDomains put to invalid table %s", table))
	}
}

func (sd *SharedDomains) Get(table string, key []byte) (v []byte, ok bool) {
	sd.RWMutex.RLock()
	v, ok = sd.get(table, key)
	sd.RWMutex.RUnlock()
	return v, ok
}

func (sd *SharedDomains) get(table string, key []byte) (v []byte, ok bool) {
	//keyS := *(*string)(unsafe.Pointer(&key))
	keyS := hex.EncodeToString(key)
	switch table {
	case kv.AccountDomain:
		v, ok = sd.account.Get(keyS)
	case kv.CodeDomain:
		v, ok = sd.code.Get(keyS)
	case kv.StorageDomain:
		v, ok = sd.storage.Get(keyS)
	case kv.CommitmentDomain:
		v, ok = sd.commitment.Get(keyS)
	default:
		panic(table)
	}
	return v, ok
}

func (sd *SharedDomains) SizeEstimate() uint64 {
	return sd.estSize.Load()
}

func (sd *SharedDomains) LatestCommitment(prefix []byte) ([]byte, error) {
	v0, ok := sd.Get(kv.CommitmentDomain, prefix)
	if ok {
		return v0, nil
	}
	v, _, err := sd.aggCtx.CommitmentLatest(prefix, sd.roTx)
	if err != nil {
		return nil, fmt.Errorf("commitment prefix %x read error: %w", prefix, err)
	}
	return v, nil
}

func (sd *SharedDomains) LatestCode(addr []byte) ([]byte, error) {
	v0, ok := sd.Get(kv.CodeDomain, addr)
	if ok {
		return v0, nil
	}
	v, _, err := sd.aggCtx.CodeLatest(addr, sd.roTx)
	if err != nil {
		return nil, fmt.Errorf("code %x read error: %w", addr, err)
	}
	return v, nil
}

func (sd *SharedDomains) LatestAccount(addr []byte) ([]byte, error) {
	v0, ok := sd.Get(kv.AccountDomain, addr)
	if ok {
		return v0, nil
	}
	v, _, err := sd.aggCtx.AccountLatest(addr, sd.roTx)
	if err != nil {
		return nil, fmt.Errorf("account %x read error: %w", addr, err)
	}
	return v, nil
}

func (sd *SharedDomains) ReadsValidBtree(table string, list *exec22.KvList) bool {
	sd.RWMutex.RLock()
	defer sd.RWMutex.RUnlock()

	var m *btree2.Map[string, []byte]
	switch table {
	case kv.AccountDomain:
		m = sd.account
	case kv.CodeDomain:
		m = sd.code
	case kv.StorageDomain:
		m = sd.storage
	default:
		panic(table)
	}

	for i, key := range list.Keys {
		if val, ok := m.Get(key); ok {
			if !bytes.Equal(list.Vals[i], val) {
				return false
			}
		}
	}
	return true
}

func (sd *SharedDomains) LatestStorage(addr, loc []byte) ([]byte, error) {
	v0, ok := sd.Get(kv.StorageDomain, common.Append(addr, loc))
	if ok {
		return v0, nil
	}
	v, _, err := sd.aggCtx.StorageLatest(addr, loc, sd.roTx)
	if err != nil {
		return nil, fmt.Errorf("storage %x|%x read error: %w", addr, loc, err)
	}
	return v, nil
}

func (sd *SharedDomains) BranchFn(pref []byte) ([]byte, error) {
	v, err := sd.LatestCommitment(pref)
	if err != nil {
		return nil, fmt.Errorf("branchFn failed: %w", err)
	}
	if v == nil {
		return nil, nil
	}
	// skip touchmap
	return v[2:], nil
}

func (sd *SharedDomains) AccountFn(plainKey []byte, cell *commitment.Cell) error {
	encAccount, err := sd.LatestAccount(plainKey)
	if err != nil {
		return fmt.Errorf("accountFn failed: %w", err)
	}
	cell.Nonce = 0
	cell.Balance.Clear()
	if len(encAccount) > 0 {
		//nonce, balance, chash := DecodeAccountBytes(encAccount)
		nonce, balance, chash := DecodeAccountBytes2(encAccount)
		cell.Nonce = nonce
		cell.Balance.Set(balance)
		if len(chash) > 0 {
			copy(cell.CodeHash[:], chash)
		}
	}

	code, err := sd.LatestCode(plainKey)
	if err != nil {
		return fmt.Errorf("accountFn: failed to read latest code: %w", err)
	}
	if len(code) > 0 {
		fmt.Printf("accountFn: code %x - %x\n", plainKey, code)
		sd.Commitment.updates.keccak.Reset()
		sd.Commitment.updates.keccak.Write(code)
		copy(cell.CodeHash[:], sd.Commitment.updates.keccak.Sum(nil))
	} else {
		copy(cell.CodeHash[:], commitment.EmptyCodeHash)
	}
	cell.Delete = len(encAccount) == 0 && len(code) == 0
	return nil
}

func (sd *SharedDomains) StorageFn(plainKey []byte, cell *commitment.Cell) error {
	// Look in the summary table first
	addr, loc := splitKey(plainKey)
	enc, _, err := sd.aggCtx.StorageLatest(addr, loc, sd.roTx)
	if err != nil {
		return err
	}
	cell.StorageLen = len(enc)
	copy(cell.Storage[:], enc)
	cell.Delete = cell.StorageLen == 0
	return nil
}

func (sd *SharedDomains) UpdateAccountData(addr []byte, account, prevAccount []byte) error {
	sd.Commitment.TouchPlainKey(addr, account, sd.Commitment.TouchAccount)
	sd.put(kv.AccountDomain, addr, account)
	return sd.Account.PutWithPrev(addr, nil, account, prevAccount)
}

func (sd *SharedDomains) UpdateAccountCode(addr []byte, code, codeHash []byte) error {
	sd.Commitment.TouchPlainKey(addr, code, sd.Commitment.TouchCode)
	prevCode, _ := sd.LatestCode(addr)
	if bytes.Equal(prevCode, code) {
		return nil
	}
	sd.put(kv.CodeDomain, addr, code)
	if len(code) == 0 {
		return sd.Code.DeleteWithPrev(addr, nil, prevCode)
	}
	return sd.Code.PutWithPrev(addr, nil, code, prevCode)
}

func (sd *SharedDomains) UpdateCommitmentData(prefix []byte, data []byte) error {
	sd.put(kv.CommitmentDomain, prefix, data)
	return sd.Commitment.Put(prefix, nil, data)
}

func (sd *SharedDomains) DeleteAccount(addr, prev []byte) error {
	sd.Commitment.TouchPlainKey(addr, nil, sd.Commitment.TouchAccount)

	sd.put(kv.AccountDomain, addr, nil)
	if err := sd.Account.DeleteWithPrev(addr, nil, prev); err != nil {
		return err
	}

	sd.put(kv.CodeDomain, addr, nil)
	sd.Commitment.TouchPlainKey(addr, nil, sd.Commitment.TouchCode)
	if err := sd.Code.Delete(addr, nil); err != nil {
		return err
	}

	var err error
	type pair struct{ k, v []byte }
	tombs := make([]pair, 0, 8)
	err = sd.IterateStoragePrefix(addr, func(k, v []byte) {
		if !bytes.HasPrefix(k, addr) {
			return
		}
		sd.put(kv.StorageDomain, k, nil)
		sd.Commitment.TouchPlainKey(k, nil, sd.Commitment.TouchStorage)
		err = sd.Storage.DeleteWithPrev(k, nil, v)

		tombs = append(tombs, pair{k, v})
	})

	for _, tomb := range tombs {
		sd.Commitment.TouchPlainKey(tomb.k, nil, sd.Commitment.TouchStorage)
		err = sd.Storage.DeleteWithPrev(tomb.k, nil, tomb.v)
	}
	return err
}

func (sd *SharedDomains) WriteAccountStorage(addr, loc []byte, value, preVal []byte) error {
	composite := common.Append(addr, loc)

	sd.Commitment.TouchPlainKey(composite, value, sd.Commitment.TouchStorage)
	sd.put(kv.StorageDomain, composite, value)
	if len(value) == 0 {
		return sd.Storage.DeleteWithPrev(addr, loc, preVal)
	}
	return sd.Storage.PutWithPrev(addr, loc, value, preVal)
}

func (sd *SharedDomains) SetContext(ctx *AggregatorV3Context) {
	sd.aggCtx = ctx
}

func (sd *SharedDomains) SetTx(tx kv.RwTx) {
	sd.roTx = tx
	sd.Commitment.SetTx(tx)
	sd.Code.SetTx(tx)
	sd.Account.SetTx(tx)
	sd.Storage.SetTx(tx)
}

func (sd *SharedDomains) SetTxNum(txNum uint64) {
	sd.txNum.Store(txNum)
	sd.Account.SetTxNum(txNum)
	sd.Code.SetTxNum(txNum)
	sd.Storage.SetTxNum(txNum)
	sd.Commitment.SetTxNum(txNum)
}

func (sd *SharedDomains) SetBlockNum(blockNum uint64) {
	sd.blockNum.Store(blockNum)
}

func (sd *SharedDomains) Final() error {
	return sd.updates.Flush()
}

func (sd *SharedDomains) Unwind() {
	sd.updates.Unwind()
}

func (sd *SharedDomains) Commit(saveStateAfter, trace bool) (rootHash []byte, err error) {
	if sd.updates.Size() != 0 {
		rh, err := sd.updates.CommitmentUpdates()
		if err != nil {
			return nil, err
		}
		return rh, nil
	}

	// if commitment mode is Disabled, there will be nothing to compute on.
	rootHash, branchNodeUpdates, err := sd.Commitment.ComputeCommitment(trace)
	if err != nil {
		return nil, err
	}

	defer func(t time.Time) { mxCommitmentWriteTook.UpdateDuration(t) }(time.Now())

	for pref, update := range branchNodeUpdates {
		prefix := []byte(pref)

		stateValue, err := sd.LatestCommitment(prefix)
		if err != nil {
			return nil, err
		}
		stated := commitment.BranchData(stateValue)
		merged, err := sd.Commitment.branchMerger.Merge(stated, update)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(stated, merged) {
			continue
		}
		if trace {
			fmt.Printf("sd computeCommitment merge [%x] [%x]+[%x]=>[%x]\n", prefix, stated, update, merged)
		}
		if err = sd.UpdateCommitmentData(prefix, merged); err != nil {
			return nil, err
		}
		mxCommitmentUpdatesApplied.Inc()
	}

	if saveStateAfter {
		if err := sd.Commitment.storeCommitmentState(sd.blockNum.Load()); err != nil {
			return nil, err
		}
	}
	if trace {
		fmt.Printf("rootHash %x\n", rootHash)
	}

	return rootHash, nil
}

// IterateStoragePrefix iterates over key-value pairs of the storage domain that start with given prefix
// Such iteration is not intended to be used in public API, therefore it uses read-write transaction
// inside the domain. Another version of this for public API use needs to be created, that uses
// roTx instead and supports ending the iterations before it reaches the end.
func (sd *SharedDomains) IterateStoragePrefix(prefix []byte, it func(k, v []byte)) error {
	sd.Storage.stats.FilesQueries.Add(1)

	var cp CursorHeap
	heap.Init(&cp)
	var k, v []byte
	var err error

	iter := sd.storage.Iter()
	cnt := 0
	if iter.Seek(string(prefix)) {
		kx := iter.Key()
		v = iter.Value()
		cnt++
		//fmt.Printf("c %d kx: %s, k: %x\n", cnt, kx, v)
		k, _ = hex.DecodeString(kx)

		if len(kx) > 0 && bytes.HasPrefix(k, prefix) {
			heap.Push(&cp, &CursorItem{t: RAM_CURSOR, key: common.Copy(k), val: common.Copy(v), iter: iter, endTxNum: sd.txNum.Load(), reverse: true})
		}
	}

	keysCursor, err := sd.roTx.CursorDupSort(sd.Storage.keysTable)
	if err != nil {
		return err
	}
	defer keysCursor.Close()
	if k, v, err = keysCursor.Seek(prefix); err != nil {
		return err
	}
	if k != nil && bytes.HasPrefix(k, prefix) {
		keySuffix := make([]byte, len(k)+8)
		copy(keySuffix, k)
		copy(keySuffix[len(k):], v)
		step := ^binary.BigEndian.Uint64(v)
		txNum := step * sd.Storage.aggregationStep
		if v, err = sd.roTx.GetOne(sd.Storage.valsTable, keySuffix); err != nil {
			return err
		}
		heap.Push(&cp, &CursorItem{t: DB_CURSOR, key: common.Copy(k), val: common.Copy(v), c: keysCursor, endTxNum: txNum, reverse: true})
	}

	sctx := sd.aggCtx.storage
	for i, item := range sctx.files {
		bg := sctx.statelessBtree(i)
		if bg.Empty() {
			continue
		}

		cursor, err := bg.Seek(prefix)
		if err != nil {
			continue
		}

		g := sctx.statelessGetter(i)
		key := cursor.Key()
		if key != nil && bytes.HasPrefix(key, prefix) {
			val := cursor.Value()
			heap.Push(&cp, &CursorItem{t: FILE_CURSOR, key: key, val: val, dg: g, endTxNum: item.endTxNum, reverse: true})
		}
	}

	for cp.Len() > 0 {
		lastKey := common.Copy(cp[0].key)
		lastVal := common.Copy(cp[0].val)
		// Advance all the items that have this key (including the top)
		for cp.Len() > 0 && bytes.Equal(cp[0].key, lastKey) {
			ci1 := cp[0]
			switch ci1.t {
			case RAM_CURSOR:
				if ci1.iter.Next() {
					k, _ = hex.DecodeString(ci1.iter.Key())
					if k != nil && bytes.HasPrefix(k, prefix) {
						ci1.key = common.Copy(k)
						ci1.val = common.Copy(ci1.iter.Value())
					}
					heap.Fix(&cp, 0)
				} else {
					heap.Pop(&cp)
				}
			case FILE_CURSOR:
				if ci1.dg.HasNext() {
					ci1.key, _ = ci1.dg.Next(ci1.key[:0])
					if ci1.key != nil && bytes.HasPrefix(ci1.key, prefix) {
						ci1.val, _ = ci1.dg.Next(ci1.val[:0])
						heap.Fix(&cp, 0)
					} else {
						heap.Pop(&cp)
					}
				} else {
					heap.Pop(&cp)
				}
			case DB_CURSOR:
				k, v, err = ci1.c.NextNoDup()
				if err != nil {
					return err
				}
				if k != nil && bytes.HasPrefix(k, prefix) {
					ci1.key = common.Copy(k)
					keySuffix := make([]byte, len(k)+8)
					copy(keySuffix, k)
					copy(keySuffix[len(k):], v)
					if v, err = sd.roTx.GetOne(sd.Storage.valsTable, keySuffix); err != nil {
						return err
					}
					ci1.val = common.Copy(v)
					heap.Fix(&cp, 0)
				} else {
					heap.Pop(&cp)
				}
			}
		}
		if len(lastVal) > 0 {
			it(lastKey, lastVal)
		}
	}
	return nil
}

func (sd *SharedDomains) Close() {
	sd.aggCtx.Close()
	sd.account.Clear()
	sd.code.Clear()
	sd.storage.Clear()
	sd.commitment.Clear()
	sd.Account.Close()
	sd.Storage.Close()
	sd.Code.Close()
	sd.Commitment.Close()
}

func (sd *SharedDomains) flushMap(ctx context.Context, rwTx kv.RwTx, table string, m map[string][]byte, logPrefix string, logEvery *time.Ticker) error {
	collector := etl.NewCollector(logPrefix, "", etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer collector.Close()

	var count int
	total := len(m)
	for k, v := range m {
		if err := collector.Collect([]byte(k), v); err != nil {
			return err
		}
		count++
		select {
		default:
		case <-logEvery.C:
			progress := fmt.Sprintf("%.1fM/%.1fM", float64(count)/1_000_000, float64(total)/1_000_000)
			log.Info("Write to db", "progress", progress, "current table", table)
			rwTx.CollectMetrics()
		}
	}
	if err := collector.Load(rwTx, table, etl.IdentityLoadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	return nil
}
func (sd *SharedDomains) flushBtree(ctx context.Context, rwTx kv.RwTx, table string, m *btree2.Map[string, []byte], logPrefix string, logEvery *time.Ticker) error {
	c, err := rwTx.RwCursor(table)
	if err != nil {
		return err
	}
	defer c.Close()
	iter := m.Iter()
	for ok := iter.First(); ok; ok = iter.Next() {
		if len(iter.Value()) == 0 {
			if err = c.Delete([]byte(iter.Key())); err != nil {
				return err
			}
		} else {
			if err = c.Put([]byte(iter.Key()), iter.Value()); err != nil {
				return err
			}
		}

		select {
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Flush", logPrefix), "table", table, "current_prefix", hex.EncodeToString([]byte(iter.Key())[:4]))
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

// todo do we really need that? we already got this values in domainWAL
func (sd *SharedDomains) Flush(ctx context.Context, rwTx kv.RwTx, logPrefix string, logEvery *time.Ticker) error {
	sd.RWMutex.Lock()
	defer sd.RWMutex.Unlock()

	if err := sd.flushBtree(ctx, rwTx, kv.AccountDomain, sd.account, logPrefix, logEvery); err != nil {
		return err
	}
	sd.account.Clear()
	if err := sd.flushBtree(ctx, rwTx, kv.StorageDomain, sd.storage, logPrefix, logEvery); err != nil {
		return err
	}
	sd.storage.Clear()
	if err := sd.flushBtree(ctx, rwTx, kv.CodeDomain, sd.code, logPrefix, logEvery); err != nil {
		return err
	}
	sd.code.Clear()
	if err := sd.flushBtree(ctx, rwTx, kv.CommitmentDomain, sd.commitment, logPrefix, logEvery); err != nil {
		return err
	}
	sd.commitment.Clear()
	sd.estSize.Store(0)
	return nil
}

type UpdatesWithCommitment struct {
	updates *UpdateTree

	domains           *SharedDomains
	initPatriciaState sync.Once
	patricia          commitment.Trie
	commitment        *btree2.Map[string, []byte]
	branchMerger      *commitment.BranchMerger
}

func (w *UpdatesWithCommitment) Size() uint64 {
	return uint64(w.updates.tree.Len())
}

func (w *UpdatesWithCommitment) Get(addr []byte) (*commitment.Update, bool) {
	item, ok := w.updates.GetWithDomain(addr, w.domains)
	if ok {
		return &item.update, ok
	}
	return nil, ok
}

func (w *UpdatesWithCommitment) TouchAccount(addr []byte, enc []byte) {
	w.updates.TouchPlainKeyDom(w.domains, addr, enc, w.updates.TouchAccount)
}

func (w *UpdatesWithCommitment) TouchCode(addr []byte, enc []byte) {
	w.updates.TouchPlainKeyDom(w.domains, addr, enc, w.updates.TouchCode)
}

func (w *UpdatesWithCommitment) TouchStorage(fullkey []byte, enc []byte) {
	w.updates.TouchPlainKeyDom(w.domains, fullkey, enc, w.updates.TouchStorage)
}

func (w *UpdatesWithCommitment) Unwind() {
	w.updates.tree.Clear(true)
	w.patricia.Reset()
	w.commitment.Clear()

}
func (w *UpdatesWithCommitment) Flush() error {
	pk, _, upd := w.updates.List(true)
	for k, update := range upd {
		upd := update
		key := pk[k]
		if upd.Flags == commitment.DeleteUpdate {

			prev, err := w.domains.LatestAccount(key)
			if err != nil {
				return fmt.Errorf("latest account %x: %w", key, err)
			}
			if err := w.domains.DeleteAccount(key, prev); err != nil {
				return fmt.Errorf("delete account %x: %w", key, err)
			}
			fmt.Printf("apply - delete account %x\n", key)
		} else {
			if upd.Flags&commitment.BalanceUpdate != 0 || upd.Flags&commitment.NonceUpdate != 0 {
				prev, err := w.domains.LatestAccount(key)
				if err != nil {
					return fmt.Errorf("latest account %x: %w", key, err)
				}
				old := accounts.NewAccount()
				if len(prev) > 0 {
					accounts.DeserialiseV3(&old, prev)
				}

				if upd.Flags&commitment.BalanceUpdate != 0 {
					old.Balance.Set(&upd.Balance)
				}
				if upd.Flags&commitment.NonceUpdate != 0 {
					old.Nonce = upd.Nonce
				}

				acc := UpdateToAccount(upd)
				fmt.Printf("apply - update account %x b %v n %d\n", key, upd.Balance.Uint64(), upd.Nonce)
				if err := w.domains.UpdateAccountData(key, accounts.SerialiseV3(acc), prev); err != nil {
					return err
				}
			}
			if upd.Flags&commitment.CodeUpdate != 0 {
				if len(upd.CodeValue[:]) != 0 && !bytes.Equal(upd.CodeHashOrStorage[:], commitment.EmptyCodeHash) {
					fmt.Printf("apply - update code %x h %x v %x\n", key, upd.CodeHashOrStorage[:], upd.CodeValue[:])
					if err := w.domains.UpdateAccountCode(key, upd.CodeValue, upd.CodeHashOrStorage[:]); err != nil {
						return err
					}
				}
			}
			if upd.Flags&commitment.StorageUpdate != 0 {
				prev, err := w.domains.LatestStorage(key[:length.Addr], key[length.Addr:])
				if err != nil {
					return fmt.Errorf("latest code %x: %w", key, err)
				}
				fmt.Printf("apply - storage %x h %x\n", key, upd.CodeHashOrStorage[:upd.ValLength])
				err = w.domains.WriteAccountStorage(key[:length.Addr], key[length.Addr:], upd.CodeHashOrStorage[:upd.ValLength], prev)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func UpdateToAccount(u commitment.Update) *accounts.Account {
	acc := accounts.NewAccount()
	acc.Initialised = true
	acc.Balance.Set(&u.Balance)
	acc.Nonce = u.Nonce
	if u.ValLength > 0 {
		acc.CodeHash = common.BytesToHash(u.CodeHashOrStorage[:u.ValLength])
	}
	return &acc
}
func NewUpdatesWithCommitment(domains *SharedDomains) *UpdatesWithCommitment {
	return &UpdatesWithCommitment{
		updates:      NewUpdateTree(),
		domains:      domains,
		commitment:   btree2.NewMap[string, []byte](128),
		branchMerger: commitment.NewHexBranchMerger(8192),
		patricia:     commitment.InitializeTrie(commitment.VariantHexPatriciaTrie),
	}
}

func (w *UpdatesWithCommitment) AddUpdates(keys [][]byte, updates []commitment.Update) {
	for i, u := range updates {
		w.updates.TouchUpdate(keys[i], u)
	}
}

// CommitmentUpdates returns the commitment updates for the current state of w.updates.
// Commitment is based on sharedDomains commitment tree
// All branch changes are stored inside Update4ReadWriter in commitment map.
// Those updates got priority over sharedDomains commitment updates.
func (w *UpdatesWithCommitment) CommitmentUpdates() ([]byte, error) {
	setup := false
	w.initPatriciaState.Do(func() {
		setup = true
		// get commitment state from commitment domain (like we're adding updates to it)
		stateBytes, err := w.domains.Commitment.PatriciaState()
		if err != nil {
			panic(err)
		}
		switch pt := w.patricia.(type) {
		case *commitment.HexPatriciaHashed:
			if err := pt.SetState(stateBytes); err != nil {
				panic(fmt.Errorf("set HPH state: %w", err))
			}
			rh, err := pt.RootHash()
			if err != nil {
				panic(fmt.Errorf("HPH root hash: %w", err))
			}
			fmt.Printf("HPH state set: %x\n", rh)
		default:
			panic(fmt.Errorf("unsupported patricia type: %T", pt))
		}
	})
	pk, hk, updates := w.updates.List(false)
	if len(updates) == 0 {
		return w.patricia.RootHash()
	}
	if !setup {
		w.patricia.Reset()
	}

	w.patricia.SetTrace(true)
	w.patricia.ResetFns(w.branchFn, w.accountFn, w.storageFn)
	rh, branches, err := w.patricia.ProcessUpdates(pk, hk, updates)
	if err != nil {
		return nil, err
	}
	fmt.Printf("\n rootHash %x\n", rh)
	_ = branches
	//for k, update := range branches {
	//	//w.commitment.Set(k, b)
	//	prefix := []byte(k)
	//
	//	stateValue, err := w.branchFn(prefix)
	//	if err != nil {
	//		return nil, err
	//	}
	//	stated := commitment.BranchData(stateValue)
	//	merged, err := w.branchMerger.Merge(stated, update)
	//	if err != nil {
	//		return nil, err
	//	}
	//	if bytes.Equal(stated, merged) {
	//		continue
	//	}
	//	w.commitment.Set(hex.EncodeToString(prefix), merged)
	//}
	return rh, nil
}

func (w *UpdatesWithCommitment) accountFn(plainKey []byte, cell *commitment.Cell) error {
	item, found := w.updates.GetWithDomain(plainKey, w.domains)
	if found {
		upd := item.Update()

		cell.Nonce = upd.Nonce
		cell.Balance.Set(&upd.Balance)
		if upd.ValLength == length.Hash {
			copy(cell.CodeHash[:], upd.CodeHashOrStorage[:])
		}
		return nil
	}
	panic("DFFJKA")
	//return w.domains.AccountFn(plainKey, cell)
}

func (w *UpdatesWithCommitment) storageFn(plainKey []byte, cell *commitment.Cell) error {
	item, found := w.updates.GetWithDomain(plainKey, w.domains)
	if found {
		upd := item.Update()
		cell.StorageLen = upd.ValLength
		copy(cell.Storage[:], upd.CodeHashOrStorage[:upd.ValLength])
		cell.Delete = cell.StorageLen == 0
		return nil
	}
	panic("dK:AJNDFS:DKjb")
	//return w.domains.StorageFn(plainKey, cell)

}

func (w *UpdatesWithCommitment) branchFn(key []byte) ([]byte, error) {
	b, ok := w.commitment.Get(string(key))
	if !ok {
		return w.domains.BranchFn(key)
	}
	return b, nil
}
