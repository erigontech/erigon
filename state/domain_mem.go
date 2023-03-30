package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/ledgerwatch/erigon-lib/commitment"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/etl"
)

type DomainMem struct {
	*Domain

	tmpdir string
	etl    *etl.Collector
	mu     sync.RWMutex
	values map[string]*KVList
}

type KVList struct {
	TxNum []uint64
	//Keys  []string
	Vals [][]byte
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
	l.Vals = append(l.Vals, v)
	return
}

func (l *KVList) Len() int {
	return len(l.TxNum)
}

func (l *KVList) Apply(f func(txn uint64, v []byte) error) error {
	for i, tx := range l.TxNum {
		if err := f(tx, l.Vals[i]); err != nil {
			return err
		}
	}
	return nil
}

func (l *KVList) Reset() {
	//l.Keys = l.Keys[:0]
	l.TxNum = l.TxNum[:0]
	l.Vals = l.Vals[:0]
}

func NewDomainMem(d *Domain, tmpdir string) *DomainMem {
	return &DomainMem{
		Domain: d,
		tmpdir: tmpdir,
		etl:    etl.NewCollector(d.valsTable, tmpdir, etl.NewSortableBuffer(WALCollectorRAM)),
		values: make(map[string]*KVList, 128),
	}
}

func (d *DomainMem) Get(k1, k2 []byte) ([]byte, error) {
	key := common.Append(k1, k2)

	d.mu.RLock()
	//value, _ := d.latest[string(key)]
	value, ok := d.values[string(key)]
	d.mu.RUnlock()

	if ok {
		_, v := value.Latest()
		return v, nil
	}
	v, found := d.Domain.MakeContext().readFromFiles(key, d.txNum)
	if !found {
		return nil, nil
	}
	return v, nil
}

// TODO:
// 1. Add prev value to WAL
// 2. read prev value correctly from domain
// 3. load from etl to table, process on the fly to avoid domain pruning

func (d *DomainMem) Flush() error {
	//etl.TransformArgs{Quit: ctx.Done()}
	err := d.etl.Load(d.tx, d.valsTable, d.etlLoader(), etl.TransformArgs{})
	if err != nil {
		return err
	}
	if d.etl != nil {
		d.etl.Close()
	}
	d.etl = etl.NewCollector(d.valsTable, d.tmpdir, etl.NewSortableBuffer(WALCollectorRAM))
	return nil
}

func (d *DomainMem) Close() {
	d.etl.Close()
	// domain is closed outside since it is shared
}

func (d *DomainMem) etlLoader() etl.LoadFunc {
	return func(k []byte, value []byte, _ etl.CurrentTableReader, next etl.LoadNextFunc) error {
		return next(k, k, value)
	}
}

func (d *DomainMem) Put(k1, k2, value []byte) error {
	key := common.Append(k1, k2)
	ks := *(*string)(unsafe.Pointer(&key))

	//invertedStep := ^(d.txNum / d.aggregationStep)
	keySuffix := make([]byte, len(key)+8)
	copy(keySuffix, key)
	binary.BigEndian.PutUint64(keySuffix[len(key):], d.txNum)

	if err := d.etl.Collect(keySuffix, value); err != nil {
		return err
	}

	d.mu.Lock()
	kvl, ok := d.values[ks]
	if !ok {
		kvl = &KVList{
			TxNum: make([]uint64, 0, 10),
			Vals:  make([][]byte, 0, 10),
		}
		d.values[ks] = kvl
	}

	ltx, prev := kvl.Put(d.txNum, value)
	_ = ltx
	d.mu.Unlock()

	//if len(prev) == 0 {
	//	var ok bool
	//	prev, ok = d.defaultDc.readFromFiles(key, 0)
	//	if !ok {
	//		return fmt.Errorf("failed to read from files: %x", key)
	//	}
	//}

	if err := d.History.AddPrevValue(k1, k2, prev); err != nil {
		return err
	}

	return nil
}

func (d *DomainMem) Delete(k1, k2 []byte) error {
	if err := d.Put(k1, k2, nil); err != nil {
		return err
	}
	return nil
	//key := common.Append(k1, k2)
	//return d.DeleteWithPrev(k1, k2, prev)
}

func (d *DomainMem) Reset() {
	//d.mu.Lock()
	////d.values.Reset()
	//d.mu.Unlock()
}

type SharedDomains struct {
	Account    *DomainMem
	Storage    *DomainMem
	Code       *DomainMem
	Commitment *DomainMemCommit

	Updates *UpdateTree
}

func (sd *SharedDomains) Close() {
	sd.Account.Close()
	sd.Storage.Close()
	sd.Code.Close()
	sd.Commitment.Close()
	sd.Updates.tree.Clear(true)
}

func (sd *SharedDomains) ComputeCommitment(txNum uint64, pk, hk [][]byte, upd []commitment.Update, saveStateAfter, trace bool) (rootHash []byte, err error) {
	// if commitment mode is Disabled, there will be nothing to compute on.
	//mxCommitmentRunning.Inc()
	rootHash, branchNodeUpdates, err := sd.Commitment.ComputeCommitment(pk, hk, upd, trace)
	//mxCommitmentRunning.Dec()
	if err != nil {
		return nil, err
	}
	//if sd.seekTxNum > sd.txNum {
	//	saveStateAfter = false
	//}

	//mxCommitmentKeys.Add(int(sd.commitment.comKeys))
	//mxCommitmentTook.Update(sd.commitment.comTook.Seconds())

	defer func(t time.Time) { mxCommitmentWriteTook.UpdateDuration(t) }(time.Now())

	//sortedPrefixes := make([]string, len(branchNodeUpdates))
	//for pref := range branchNodeUpdates {
	//	sortedPrefixes = append(sortedPrefixes, pref)
	//}
	//sort.Strings(sortedPrefixes)

	cct := sd.Commitment //.MakeContext()
	//defer cct.Close()

	for pref, update := range branchNodeUpdates {
		prefix := []byte(pref)
		//update := branchNodeUpdates[pref]

		stateValue, err := cct.Get(prefix, nil)
		if err != nil {
			return nil, err
		}
		//mxCommitmentUpdates.Inc()
		stated := commitment.BranchData(stateValue)
		merged, err := sd.Commitment.c.branchMerger.Merge(stated, update)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(stated, merged) {
			continue
		}
		if trace {
			fmt.Printf("computeCommitment merge [%x] [%x]+[%x]=>[%x]\n", prefix, stated, update, merged)
		}
		if err = sd.Commitment.Put(prefix, nil, merged); err != nil {
			return nil, err
		}
		//mxCommitmentUpdatesApplied.Inc()
	}

	if saveStateAfter {
		if err := sd.Commitment.c.storeCommitmentState(0, txNum); err != nil {
			return nil, err
		}
	}

	return rootHash, nil
}

func (sd *SharedDomains) Commit(txNum uint64, saveStateAfter, trace bool) (rootHash []byte, err error) {
	// if commitment mode is Disabled, there will be nothing to compute on.
	rootHash, branchNodeUpdates, err := sd.Commitment.c.ComputeCommitment(trace)
	if err != nil {
		return nil, err
	}

	defer func(t time.Time) { mxCommitmentWriteTook.UpdateDuration(t) }(time.Now())

	for pref, update := range branchNodeUpdates {
		prefix := []byte(pref)

		stateValue, err := sd.Commitment.Get(prefix, nil)
		if err != nil {
			return nil, err
		}
		stated := commitment.BranchData(stateValue)
		merged, err := sd.Commitment.c.branchMerger.Merge(stated, update)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(stated, merged) {
			continue
		}
		if trace {
			fmt.Printf("computeCommitment merge [%x] [%x]+[%x]=>[%x]\n", prefix, stated, update, merged)
		}
		if err = sd.UpdateCommitmentData(prefix, merged); err != nil {
			return nil, err
		}
		mxCommitmentUpdatesApplied.Inc()
	}

	if saveStateAfter {
		if err := sd.Commitment.c.storeCommitmentState(0, sd.Commitment.txNum); err != nil {
			return nil, err
		}
	}

	return rootHash, nil
}

func (sd *SharedDomains) SetTxNum(txNum uint64) {
	sd.Account.SetTxNum(txNum)
	sd.Storage.SetTxNum(txNum)
	sd.Code.SetTxNum(txNum)
	sd.Commitment.SetTxNum(txNum)
}

func (sd *SharedDomains) Flush() error {
	if err := sd.Account.Flush(); err != nil {
		return err
	}
	if err := sd.Storage.Flush(); err != nil {
		return err
	}
	if err := sd.Code.Flush(); err != nil {
		return err
	}
	if err := sd.Commitment.Flush(); err != nil {
		return err
	}
	return nil
}

func NewSharedDomains(tmp string, a, c, s *Domain, comm *DomainCommitted) *SharedDomains {
	sd := &SharedDomains{
		Updates:    NewUpdateTree(comm.mode),
		Account:    NewDomainMem(a, tmp),
		Storage:    NewDomainMem(s, tmp),
		Code:       NewDomainMem(c, tmp),
		Commitment: &DomainMemCommit{DomainMem: NewDomainMem(comm.Domain, tmp), c: comm},
	}
	sd.Commitment.c.ResetFns(sd.BranchFn, sd.AccountFn, sd.StorageFn)
	return sd
}

type DomainMemCommit struct {
	*DomainMem
	c *DomainCommitted
}

func (d *DomainMemCommit) ComputeCommitment(pk, hk [][]byte, upd []commitment.Update, trace bool) (rootHash []byte, branchNodeUpdates map[string]commitment.BranchData, err error) {
	return d.c.CommitmentOver(pk, hk, upd, trace)
}

func (sd *SharedDomains) BranchFn(pref []byte) ([]byte, error) {
	v, err := sd.Commitment.Get(pref, nil)
	if err != nil {
		return nil, fmt.Errorf("branchFn: no value for prefix %x: %w", pref, err)
	}
	if v == nil {
		return nil, nil
	}
	// skip touchmap
	return v[2:], nil
}

func (sd *SharedDomains) AccountFn(plainKey []byte, cell *commitment.Cell) error {
	encAccount, err := sd.Account.Get(plainKey, nil)
	if err != nil {
		return fmt.Errorf("accountFn: no value for address %x : %w", plainKey, err)
	}
	cell.Nonce = 0
	cell.Balance.Clear()
	copy(cell.CodeHash[:], commitment.EmptyCodeHash)
	if len(encAccount) > 0 {
		nonce, balance, chash := DecodeAccountBytes(encAccount)
		cell.Nonce = nonce
		cell.Balance.Set(balance)
		if chash != nil {
			copy(cell.CodeHash[:], chash)
		}
	}

	code, _ := sd.Code.Get(plainKey, nil)
	if code != nil {
		sd.Updates.keccak.Reset()
		sd.Updates.keccak.Write(code)
		copy(cell.CodeHash[:], sd.Updates.keccak.Sum(nil))
	}
	cell.Delete = len(encAccount) == 0 && len(code) == 0
	return nil
}

func (sd *SharedDomains) StorageFn(plainKey []byte, cell *commitment.Cell) error {
	// Look in the summary table first
	enc, err := sd.Storage.Get(plainKey[:length.Addr], plainKey[length.Addr:])
	if err != nil {
		return err
	}
	cell.StorageLen = len(enc)
	copy(cell.Storage[:], enc)
	cell.Delete = cell.StorageLen == 0
	return nil
}

func (sd *SharedDomains) UpdateAccountData(addr []byte, account []byte) error {
	sd.Commitment.c.TouchPlainKey(addr, account, sd.Commitment.c.TouchPlainKeyAccount)
	return sd.Account.Put(addr, nil, account)
}

func (sd *SharedDomains) UpdateAccountCode(addr []byte, code []byte) error {
	sd.Commitment.c.TouchPlainKey(addr, code, sd.Commitment.c.TouchPlainKeyCode)
	if len(code) == 0 {
		return sd.Code.Delete(addr, nil)
	}
	return sd.Code.Put(addr, nil, code)
}

func (sd *SharedDomains) UpdateCommitmentData(prefix []byte, code []byte) error {
	return sd.Commitment.Put(prefix, nil, code)
}

func (sd *SharedDomains) DeleteAccount(addr []byte) error {
	sd.Commitment.c.TouchPlainKey(addr, nil, sd.Commitment.c.TouchPlainKeyAccount)

	if err := sd.Account.Delete(addr, nil); err != nil {
		return err
	}
	if err := sd.Code.Delete(addr, nil); err != nil {
		return err
	}
	var e error
	if err := sd.Storage.defaultDc.IteratePrefix(addr, func(k, _ []byte) {
		sd.Commitment.c.TouchPlainKey(k, nil, sd.Commitment.c.TouchPlainKeyStorage)
		if e == nil {
			e = sd.Storage.Delete(k, nil)
		}
	}); err != nil {
		return err
	}
	return e
}

func (sd *SharedDomains) WriteAccountStorage(addr, loc []byte, value []byte) error {
	composite := make([]byte, len(addr)+len(loc))
	copy(composite, addr)
	copy(composite[length.Addr:], loc)

	sd.Commitment.c.TouchPlainKey(composite, value, sd.Commitment.c.TouchPlainKeyStorage)
	if len(value) == 0 {
		return sd.Storage.Delete(addr, loc)
	}
	return sd.Storage.Put(addr, loc, value)
}
