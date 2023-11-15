package getters

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/lru"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/services"
)

type cacheEntry struct {
	number uint64
	hash   libcommon.Hash
}
type ExecutionSnapshotReader struct {
	ctx context.Context

	blockReader services.FullBlockReader

	db               kv.RoDB
	txsCache         *lru.Cache[cacheEntry, []byte]
	withdrawalsCache *lru.Cache[cacheEntry, []byte]
}

func NewExecutionSnapshotReader(ctx context.Context, blockReader services.FullBlockReader, db kv.RoDB) *ExecutionSnapshotReader {
	txsCache, err := lru.New[cacheEntry, []byte]("txsCache", 96)
	if err != nil {
		panic(err)
	}
	withdrawalsCache, err := lru.New[cacheEntry, []byte]("wsCache", 96)
	if err != nil {
		panic(err)
	}
	return &ExecutionSnapshotReader{ctx: ctx, blockReader: blockReader, withdrawalsCache: withdrawalsCache, txsCache: txsCache, db: db}
}

func (r *ExecutionSnapshotReader) TransactionsSSZ(w io.Writer, number uint64, hash libcommon.Hash) error {
	ok, err := r.lookupTransactionsInCache(w, number, hash)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	tx, err := r.db.BeginRo(r.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Get the body and fill both caches
	body, err := r.blockReader.BodyWithTransactions(r.ctx, tx, hash, number)
	if err != nil {
		return err
	}
	if body == nil {
		return fmt.Errorf("transactions not found for block %d", number)
	}
	// compute txs flats
	txs, err := types.MarshalTransactionsBinary(body.Transactions)
	if err != nil {
		return err
	}
	flattenedTxs := convertTxsToBytesSSZ(txs)
	r.txsCache.Add(cacheEntry{number: number, hash: hash}, flattenedTxs)
	// compute withdrawals flat
	ws := body.Withdrawals
	flattenedWs := convertWithdrawalsToBytesSSZ(ws)

	r.withdrawalsCache.Add(cacheEntry{number: number, hash: hash}, flattenedWs)
	_, err = w.Write(flattenedTxs)
	return err
}

func convertTxsToBytesSSZ(txs [][]byte) []byte {
	sumLenTxs := 0
	for _, tx := range txs {
		sumLenTxs += len(tx)
	}
	flat := make([]byte, 0, 4*len(txs)+sumLenTxs)
	offset := len(txs) * 4
	for _, tx := range txs {
		flat = append(flat, ssz.OffsetSSZ(uint32(offset))...)
		offset += len(tx)
	}
	for _, tx := range txs {
		flat = append(flat, tx...)
	}
	return flat
}

func convertWithdrawalsToBytesSSZ(ws []*types.Withdrawal) []byte {
	ret := make([]byte, 44*len(ws))
	for i, w := range ws {
		currentPos := i * 44
		binary.LittleEndian.PutUint64(ret[currentPos:currentPos+8], w.Index)
		binary.LittleEndian.PutUint64(ret[currentPos+8:currentPos+16], w.Validator)
		copy(ret[currentPos+16:currentPos+36], w.Address[:])
		binary.LittleEndian.PutUint64(ret[currentPos+36:currentPos+44], w.Amount)
	}
	return ret
}

func (r *ExecutionSnapshotReader) WithdrawalsSZZ(w io.Writer, number uint64, hash libcommon.Hash) error {
	ok, err := r.lookupWithdrawalsInCache(w, number, hash)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	tx, err := r.db.BeginRo(r.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Get the body and fill both caches
	body, err := r.blockReader.BodyWithTransactions(r.ctx, tx, hash, number)
	if err != nil {
		return err
	}
	if body == nil {
		return fmt.Errorf("transactions not found for block %d", number)
	}
	// compute txs flats
	txs, err := types.MarshalTransactionsBinary(body.Transactions)
	if err != nil {
		return err
	}
	flattenedTxs := convertTxsToBytesSSZ(txs)
	r.txsCache.Add(cacheEntry{number: number, hash: hash}, flattenedTxs)
	// compute withdrawals flat
	ws := body.Withdrawals
	flattenedWs := convertWithdrawalsToBytesSSZ(ws)

	r.withdrawalsCache.Add(cacheEntry{number: number, hash: hash}, flattenedWs)
	_, err = w.Write(flattenedWs)

	return err
}

func (r *ExecutionSnapshotReader) lookupWithdrawalsInCache(w io.Writer, number uint64, hash libcommon.Hash) (bool, error) {
	var wsBytes []byte
	var ok bool
	if wsBytes, ok = r.withdrawalsCache.Get(cacheEntry{number: number, hash: hash}); !ok {
		return false, nil
	}
	_, err := w.Write(wsBytes)
	return true, err
}

func (r *ExecutionSnapshotReader) lookupTransactionsInCache(w io.Writer, number uint64, hash libcommon.Hash) (bool, error) {
	var wsBytes []byte
	var ok bool
	if wsBytes, ok = r.txsCache.Get(cacheEntry{number: number, hash: hash}); !ok {
		return false, nil
	}
	_, err := w.Write(wsBytes)
	return true, err
}
