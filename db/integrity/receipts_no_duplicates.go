package integrity

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/services"
)

// CheckReceiptsNoDups performs integrity checks on receipts to ensure no duplicates exist.
// This function uses parallel processing for improved performance.
func CheckReceiptsNoDups(ctx context.Context, db kv.TemporalRoDB, blockReader services.FullBlockReader, failFast bool) (err error) {
	defer func() {
		log.Info("[integrity] ReceiptsNoDups: done", "err", err)
	}()

	txNumsReader := blockReader.TxnumReader()

	if err := ValidateDomainProgress(ctx, db, kv.ReceiptDomain, txNumsReader); err != nil {
		return err
	}

	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	receiptProgress := tx.Debug().DomainProgress(kv.ReceiptDomain)
	fromBlock := uint64(1)
	toBlock, _, _ := txNumsReader.FindBlockNum(ctx, tx, receiptProgress)

	log.Info("[integrity] ReceiptsNoDups starting", "fromBlock", fromBlock, "toBlock", toBlock)
	return parallelChunkCheck(ctx, fromBlock, toBlock, db, blockReader, failFast, "CheckReceiptsNoDups", ReceiptsNoDupsRange)
}

func checkCumGas(ctx context.Context, fromBlock, toBlock uint64, db kv.TemporalRoDB, blockReader services.FullBlockReader, failFast bool) (err error) {
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	txNumsReader := blockReader.TxnumReader()
	fromTxNum, err := txNumsReader.Min(ctx, tx, fromBlock)
	if err != nil {
		return err
	}

	if toBlock > 0 {
		toBlock-- // [fromBlock,toBlock)
	}

	toTxNum, err := txNumsReader.Max(ctx, tx, toBlock)
	if err != nil {
		return err
	}

	prevCumGasUsed := -1
	blockNum := fromBlock
	var _min, _max uint64
	_min = fromTxNum
	_max, _ = txNumsReader.Max(ctx, tx, fromBlock)

	cumGasTx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer cumGasTx.Rollback()
	cumGasIt, err := cumGasTx.Debug().TraceKey(kv.ReceiptDomain, rawtemporaldb.CumulativeGasUsedInBlockKey, fromTxNum, toTxNum+1)
	if err != nil {
		return err
	}
	defer cumGasIt.Close()

	for cumGasIt.HasNext() {
		txNum, v, err := cumGasIt.Next()
		if err != nil {
			return err
		}
		cumGasUsed := uvarint(v)
		blockChanged := false

		for txNum >= _max {
			blockNum++
			_min = _max + 1
			_max, _ = txNumsReader.Max(ctx, tx, blockNum)
			blockChanged = true
		}
		//fmt.Println("txNum:", txNum, "cumGasUsed:", cumGasUsed)
		if blockChanged {
			prevCumGasUsed = 0
		}

		strongMonotonicCumGasUsed := int(cumGasUsed) > prevCumGasUsed
		if !strongMonotonicCumGasUsed && !blockChanged { // system tx can be skipped
			err := fmt.Errorf("CheckReceiptsNoDups: non-monotonic cumGasUsed at txnum: %d, block: %d(%d-%d), cumGasUsed=%d, prevCumGasUsed=%d", txNum, blockNum, _min, _max, cumGasUsed, prevCumGasUsed)
			if failFast {
				return err
			}
			log.Error(err.Error())
		}

		if !blockChanged {
			prevCumGasUsed = int(cumGasUsed)
		}

		if txNum%1000 == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
	}
	return nil
}

func checkLogIdx(ctx context.Context, fromBlock, toBlock uint64, db kv.TemporalRoDB, blockReader services.FullBlockReader, failFast bool) (err error) {
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	txNumsReader := blockReader.TxnumReader()
	fromTxNum, err := txNumsReader.Min(ctx, tx, fromBlock)
	if err != nil {
		return err
	}

	if toBlock > 0 {
		toBlock-- // [fromBlock,toBlock)
	}

	toTxNum, err := txNumsReader.Max(ctx, tx, toBlock)
	if err != nil {
		return err
	}

	prevLogIdxAfterTx := uint32(0)
	blockNum := fromBlock
	var _min, _max uint64
	_min = fromTxNum
	_max, _ = txNumsReader.Max(ctx, tx, fromBlock)

	logIdxTx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer logIdxTx.Rollback()

	logIdxAfterTxIt, err := logIdxTx.Debug().TraceKey(kv.ReceiptDomain, rawtemporaldb.LogIndexAfterTxKey, fromTxNum, toTxNum+1)
	if err != nil {
		return err
	}
	defer logIdxAfterTxIt.Close()
	for logIdxAfterTxIt.HasNext() {
		txNum, v, err := logIdxAfterTxIt.Next()
		if err != nil {
			return err
		}
		logIdxAfterTx := uint32(uvarint(v))
		blockChanged := false

		if txNum >= _max {
			blockNum, _, _ = txNumsReader.FindBlockNum(ctx, tx, txNum)
			_min, _ = txNumsReader.Min(ctx, tx, blockNum)
			_max, _ = txNumsReader.Max(ctx, tx, blockNum)
			blockChanged = true
		}

		if blockChanged {
			prevLogIdxAfterTx = 0
		}

		monotonicLogIdx := logIdxAfterTx >= prevLogIdxAfterTx
		if !monotonicLogIdx && !blockChanged {
			err := fmt.Errorf("CheckReceiptsNoDups: non-monotonic logIndex at txnum: %d, block: %d(%d-%d), logIdxAfterTx=%d, prevLogIdxAfterTx=%d", txNum, blockNum, _min, _max, logIdxAfterTx, prevLogIdxAfterTx)
			if failFast {
				return err
			}
			log.Error(err.Error())
		}

		if !blockChanged {
			prevLogIdxAfterTx = logIdxAfterTx
		}

		if txNum%1000 == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
	}
	return nil
}

func ReceiptsNoDupsRange(ctx context.Context, fromBlock, toBlock uint64, db kv.TemporalRoDB, blockReader services.FullBlockReader, failFast bool) (err error) {
	if err := checkCumGas(ctx, fromBlock, toBlock, db, blockReader, failFast); err != nil {
		return err
	}
	if err := checkLogIdx(ctx, fromBlock, toBlock, db, blockReader, failFast); err != nil {
		return err
	}
	return nil
}

func ValidateDomainProgress(ctx context.Context, db kv.TemporalRoDB, domain kv.Domain, txNumsReader rawdbv3.TxNumsReader) (err error) {
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	receiptProgress := tx.Debug().DomainProgress(domain)
	accProgress := tx.Debug().DomainProgress(kv.AccountsDomain)
	if accProgress > receiptProgress {
		e1, _, _ := txNumsReader.FindBlockNum(ctx, tx, receiptProgress)
		e2, _, _ := txNumsReader.FindBlockNum(ctx, tx, accProgress)

		// accProgress can be greater than domainProgress in some scenarios..
		// e.g. account vs receipt
		// like systemTx can update accounts, but no receipt is added for those tx.
		// Similarly a series of empty blocks towards the end can cause big gaps...
		// The message is kept because it might also happen due to problematic cases
		// like StageCustomTrace execution not having gone through to the end leading to missing data in receipt/rcache.
		msg := fmt.Sprintf("[integrity] %s=%d (%d) is behind AccountDomain=%d(%d); this might be okay, please check", domain.String(), receiptProgress, e1, accProgress, e2)
		log.Warn(msg)
		return nil
	}
	// else if accProgress < receiptProgress {
	// 	// something very wrong
	// 	e1, _, _ := txNumsReader.FindBlockNum(tx, receiptProgress)
	// 	e2, _, _ := txNumsReader.FindBlockNum(tx, accProgress)

	// 	err := fmt.Errorf("[integrity] %s=%d (%d) is ahead of AccountDomain=%d(%d)", domain.String(), receiptProgress, e1, accProgress, e2)
	// 	log.Error(err.Error())
	// 	return err

	// }
	return nil
}

func uvarint(in []byte) (res uint64) {
	res, _ = binary.Uvarint(in)
	return res
}
