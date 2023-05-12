package state

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon-lib/commitment"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
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

	txNum      atomic.Uint64
	blockNum   atomic.Uint64
	Account    *Domain
	Storage    *Domain
	Code       *Domain
	Commitment *DomainCommitted
}

func NewSharedDomains(a, c, s *Domain, comm *DomainCommitted) *SharedDomains {
	sd := &SharedDomains{
		Account:    a,
		Code:       c,
		Storage:    s,
		Commitment: comm,
	}
	sd.Commitment.ResetFns(sd.BranchFn, sd.AccountFn, sd.StorageFn)
	return sd
}

func (sd *SharedDomains) SizeEstimate() uint64 {
	return sd.Account.wal.size() + sd.Storage.wal.size() + sd.Code.wal.size() + sd.Commitment.wal.size()
}

func (sd *SharedDomains) LatestCommitment(prefix []byte) ([]byte, error) {
	v, _, err := sd.aggCtx.CommitmentLatest(prefix, sd.roTx)
	if err != nil {
		return nil, fmt.Errorf("commitment prefix %x read error: %w", prefix, err)
	}
	return v, nil
}

func (sd *SharedDomains) LatestCode(addr []byte) ([]byte, error) {
	v, _, err := sd.aggCtx.CodeLatest(addr, sd.roTx)
	if err != nil {
		return nil, fmt.Errorf("code %x read error: %w", addr, err)
	}
	return v, nil
}

func (sd *SharedDomains) LatestAccount(addr []byte) ([]byte, error) {
	v, _, err := sd.aggCtx.AccountLatest(addr, sd.roTx)
	if err != nil {
		return nil, fmt.Errorf("account %x read error: %w", addr, err)
	}
	return v, nil
}

func (sd *SharedDomains) LatestStorage(addr, loc []byte) ([]byte, error) {
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
		nonce, balance, chash := DecodeAccountBytes(encAccount)
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
	return sd.Account.PutWithPrev(addr, nil, account, prevAccount)
}

func (sd *SharedDomains) UpdateAccountCode(addr []byte, code, _ []byte) error {
	sd.Commitment.TouchPlainKey(addr, code, sd.Commitment.TouchCode)
	prevCode, _ := sd.Code.values.Get(hex.EncodeToString(addr))
	if len(code) == 0 {
		return sd.Code.DeleteWithPrev(addr, nil, prevCode)
	}
	return sd.Code.PutWithPrev(addr, nil, code, prevCode)
}

func (sd *SharedDomains) UpdateCommitmentData(prefix []byte, data []byte) error {
	return sd.Commitment.Put(prefix, nil, data)
}

func (sd *SharedDomains) DeleteAccount(addr, prev []byte) error {
	sd.Commitment.TouchPlainKey(addr, nil, sd.Commitment.TouchAccount)

	if err := sd.Account.DeleteWithPrev(addr, nil, prev); err != nil {
		return err
	}

	sd.Commitment.TouchPlainKey(addr, nil, sd.Commitment.TouchCode)
	if err := sd.Code.Delete(addr, nil); err != nil {
		return err
	}

	var err error
	type pair struct{ k, v []byte }
	tombs := make([]pair, 0, 8)
	err = sd.aggCtx.storage.IteratePrefix(addr, func(k, v []byte) {
		if !bytes.HasPrefix(k, addr) {
			return
		}
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

func (sd *SharedDomains) Commit(saveStateAfter, trace bool) (rootHash []byte, err error) {
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
			fmt.Printf("computeCommitment merge [%x] [%x]+[%x]=>[%x]\n", prefix, stated, update, merged)
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

	return rootHash, nil
}

func (sd *SharedDomains) Close() {
	sd.aggCtx.Close()
	sd.Account.Close()
	sd.Storage.Close()
	sd.Code.Close()
	sd.Commitment.Close()
}
