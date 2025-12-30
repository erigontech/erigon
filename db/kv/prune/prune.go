package prune

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/diagnostics/metrics"
)

type Stat struct {
	MinTxNum         uint64
	MaxTxNum         uint64
	PruneCountTx     uint64
	PruneCountValues uint64
	DupsDeleted      uint64
	LastPrunedValue  []byte
}

type StorageMode int

const (
	DefaultStorageMode StorageMode = iota
	KeyStorageMode
	SmallHistoryMode //TODO: change name
)

func HashSeekingPrune(
	ctx context.Context,
	name, filenameBase, tmp string,
	txFrom, txTo, limit, stepSize uint64,
	logEvery *time.Ticker,
	logger log.Logger,
	keysCursor kv.RwCursorDupSort, valDelCursor kv.PseudoDupSortRwCursor,
	asserts bool,
	mxPruneInProgress metrics.Gauge, mxPruneTookIndex metrics.Histogram, mxPruneSizeIndex metrics.Counter,
	mode StorageMode,
) (stat *Stat, err error) {
	stat = &Stat{MinTxNum: math.MaxUint64}
	start := time.Now()
	mxPruneInProgress.Inc()
	defer mxPruneInProgress.Dec()
	defer func(t time.Time) { mxPruneTookIndex.ObserveDuration(t) }(time.Now())

	if limit == 0 { // limits amount of txn to be pruned
		limit = math.MaxUint64
	}

	logger.Info("ii pruning", "name", name, "txFrom", txFrom, "txTo", txTo, "limit", limit)

	collector := etl.NewCollectorWithAllocator(filenameBase+".prune.ii", tmp, etl.SmallSortableBuffers, logger)
	defer collector.Close()
	collector.LogLvl(log.LvlTrace)
	collector.SortAndFlushInBackground(true)

	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)

	// Invariant: if some `txNum=N` pruned - it's pruned Fully
	// Means: can use DeleteCurrentDuplicates all values of given `txNum`
	for k, v, err := keysCursor.Seek(txKey[:]); k != nil; k, v, err = keysCursor.NextNoDup() {
		if err != nil {
			return nil, fmt.Errorf("iterate over %s index keys: %w", filenameBase, err)
		}
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo || limit == 0 {
			break
		}
		if asserts && txNum < txFrom {
			panic(fmt.Errorf("assert: index pruning txn=%d [%d-%d)", txNum, txFrom, txTo))
		}

		limit--
		stat.MinTxNum = min(stat.MinTxNum, txNum)
		stat.MaxTxNum = max(stat.MaxTxNum, txNum)

		for ; v != nil; _, v, err = keysCursor.NextDup() {
			if err != nil {
				return nil, fmt.Errorf("iterate over %s index keys: %w", filenameBase, err)
			}
			if err := collector.Collect(v, k); err != nil {
				return nil, err
			}
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}

	err = collector.Load(nil, "", func(key, txnm []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		switch mode {
		case KeyStorageMode:
			seek := make([]byte, 8, 256)
			seek = append(bytes.Clone(key), txnm...)
			if err := valDelCursor.Delete(seek); err != nil {
				return err
			}
		case SmallHistoryMode:
			vv, err := valDelCursor.(kv.RwCursorDupSort).SeekBothRange(key, txnm)
			if err != nil {
				return err
			}
			if len(vv) < 8 {
				return fmt.Errorf("prune history %s got invalid value length: %d < 8", filenameBase, len(vv))
			}
			if vtx := binary.BigEndian.Uint64(vv); vtx != binary.BigEndian.Uint64(txnm) {
				return fmt.Errorf("prune history %s got invalid txNum: found %d != %d wanted", filenameBase, vtx, 1132)
			}
			if err = valDelCursor.DeleteCurrent(); err != nil {
				return err
			}
		case DefaultStorageMode:
			err = valDelCursor.DeleteExact(key, txnm)
			if err != nil {
				return err
			}
		}
		mxPruneSizeIndex.Inc()
		stat.PruneCountValues++

		select {
		case <-logEvery.C:
			txNum := binary.BigEndian.Uint64(txnm)
			logger.Info("[snapshots] prune index", "name", filenameBase, "pruned tx", stat.PruneCountTx,
				"pruned values", stat.PruneCountValues,
				"steps", fmt.Sprintf("%.2f-%.2f", float64(txFrom)/float64(stepSize), float64(txNum)/float64(stepSize)))
		default:
		}
		return nil
	}, etl.TransformArgs{Quit: ctx.Done()})

	if stat.MinTxNum != math.MaxUint64 {
		binary.BigEndian.PutUint64(txKey[:], stat.MinTxNum)
		// This deletion iterator goes last to preserve invariant: if some `txNum=N` pruned - it's pruned Fully
		for txnb, _, err := keysCursor.Seek(txKey[:]); txnb != nil; txnb, _, err = keysCursor.NextNoDup() {
			if err != nil {
				return nil, fmt.Errorf("iterate over %s index keys: %w", filenameBase, err)
			}
			if binary.BigEndian.Uint64(txnb) > stat.MaxTxNum {
				break
			}
			stat.PruneCountTx++
			if err = keysCursor.Delete(txnb); err != nil {
				return nil, err
			}
		}
	}

	logger.Info("ii hash prune res", "name", name, "txFrom", txFrom, "txTo", txTo, "limit", limit, "keys", stat.PruneCountTx, "vals", stat.PruneCountValues, "spent ms", time.Since(start).Milliseconds())

	return stat, err
}

func TableScanningPrune(
	ctx context.Context,
	name, filenameBase string,
	txFrom, txTo, limit, stepSize uint64,
	logEvery *time.Ticker,
	logger log.Logger,
	keysCursor kv.RwCursorDupSort, valDelCursor kv.PseudoDupSortRwCursor,
	asserts bool,
	mxPruneInProgress metrics.Gauge, mxPruneTookIndex metrics.Histogram, mxPruneSizeIndex, mxDupsPruneSizeIndex metrics.Counter,
	startKey, startVal []byte,
	mode StorageMode,
) (stat *Stat, err error) {
	stat = &Stat{MinTxNum: math.MaxUint64}
	start := time.Now()
	mxPruneInProgress.Inc()
	defer mxPruneInProgress.Dec()
	defer func(t time.Time) { mxPruneTookIndex.ObserveDuration(t) }(time.Now())

	if limit == 0 { // limits amount of txn to be pruned
		limit = math.MaxUint64
	}

	timeOut := 999 * time.Hour
	if limit < 1000 { //TODO: change after tests
		timeOut = 100 * time.Millisecond
	}

	var lastPrunedVal []byte

	//defer func() {
	//	err = SavePruneValProgress(rwTx, ii.ValuesTable, lastPrunedVal)
	//	if err != nil {
	//		logger.Error("prune val progress", "name", name, "err", err)
	//	}
	//}()

	var txKey [8]byte

	binary.BigEndian.PutUint64(txKey[:], txFrom)
	pairs := uint64(0)
	// This deletion iterator goes last to preserve invariant: if some `txNum=N` pruned - it's pruned Fully
	for txnb, _, err := keysCursor.Seek(txKey[:]); txnb != nil; txnb, _, err = keysCursor.NextNoDup() {
		if err != nil {
			return nil, fmt.Errorf("iterate over %s index keys: %w", filenameBase, err)
		}
		txNum := binary.BigEndian.Uint64(txnb)
		if txNum >= txTo {
			break
		}
		stat.PruneCountTx++
		dups, err := keysCursor.CountDuplicates()
		if err != nil {
			return nil, err
		}
		pairs += dups
		if err = keysCursor.DeleteCurrentDuplicates(); err != nil {
			return nil, err
		}
	}

	// Invariant: if some `txNum=N` pruned - it's pruned Fully
	// Means: can use DeleteCurrentDuplicates all values of given `txNum`
	valLen := uint64(0)
	txNumBytes, iiVal := common.Copy(startKey), common.Copy(startVal)

	isNotDone := false

	txNumGetter := func(key, val []byte) uint64 { // key == valCursor key, val – usually txnum
		switch mode {
		case KeyStorageMode:
			return binary.BigEndian.Uint64(key[len(key)-8:])
		case SmallHistoryMode:
			return binary.BigEndian.Uint64(val)
		case DefaultStorageMode:
			return binary.BigEndian.Uint64(val)
		default:
			return 0
		}
	}

	for ; iiVal != nil; iiVal, txNumBytes, err = valDelCursor.NextNoDup() {
		if err != nil {
			return nil, fmt.Errorf("iterate over %s index keys: %w", filenameBase, err)
		}
		dups, err := valDelCursor.CountDuplicates() //TODO: delete when analytics would be ready
		if err != nil {
			return nil, fmt.Errorf("iterate over %s index keys: %w", filenameBase, err)
		}
		valLen += dups
		if time.Since(start) > timeOut {
			logger.Info("prune val timed out", "name", filenameBase)
			lastPrunedVal = iiVal
			isNotDone = true
			break
		}
		txNum := txNumGetter(iiVal, txNumBytes)
		lastDupTxNumB, err := valDelCursor.LastDup()
		if err != nil {
			return nil, fmt.Errorf("LastDup iterate over %s index keys: %w", filenameBase, err)
		}
		_, err = valDelCursor.FirstDup()
		if err != nil {
			return nil, fmt.Errorf("FirstDup iterate over %s index keys: %w", filenameBase, err)
		}
		lastDupTxNum := txNumGetter(iiVal, lastDupTxNumB)
		if txNum >= txTo {
			break
		}
		dupsDelete := lastDupTxNum < txTo && txNum >= txFrom
		if asserts && txNum < txFrom {
			panic(fmt.Errorf("assert: index pruning txn=%d [%d-%d)", txNum, txFrom, txTo))
		}

		stat.MinTxNum = min(stat.MinTxNum, txNum)
		stat.MaxTxNum = max(stat.MaxTxNum, txNum)
		if dupsDelete {
			err = valDelCursor.DeleteCurrentDuplicates()
			if err != nil {
				return nil, fmt.Errorf("iterate over %s index keys: %w", filenameBase, err)
			}
			mxPruneSizeIndex.AddUint64(dups)
			mxDupsPruneSizeIndex.AddUint64(dups)
			stat.PruneCountValues += dups
			stat.DupsDeleted += dups
		} else {
			for ; txNumBytes != nil; _, txNumBytes, err = valDelCursor.NextDup() {
				if err != nil {
					return nil, fmt.Errorf("iterate over %s index keys: %w", filenameBase, err)
				}
				txNumDup := txNumGetter(iiVal, txNumBytes)
				if txNumDup < txFrom {
					continue
				}
				if txNumDup >= txTo {
					break
				}
				if time.Since(start) > timeOut {
					lastPrunedVal = iiVal
					isNotDone = true
					break
				}
				stat.MinTxNum = min(stat.MinTxNum, txNumDup)
				stat.MaxTxNum = max(stat.MaxTxNum, txNumDup)
				if err = valDelCursor.DeleteCurrent(); err != nil {
					return nil, err
				}
				mxPruneSizeIndex.Inc()
				stat.PruneCountValues++
			}
			if isNotDone {
				break
			}
		}

		select {
		case <-logEvery.C:
			txNum := binary.BigEndian.Uint64(txNumBytes)
			logger.Info("[snapshots] prune index", "name", filenameBase, "pruned tx", stat.PruneCountTx,
				"pruned values", stat.PruneCountValues,
				"steps", fmt.Sprintf("%.2f-%.2f", float64(txFrom)/float64(stepSize), float64(txNum)/float64(stepSize)))
		default:
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}

	if isNotDone {
		lastPrunedVal = nil
	}

	logger.Info("scan pruning res", "name", name, "txFrom", txFrom, "txTo", txTo, "limit", limit, "keys", stat.PruneCountTx, "vals", stat.PruneCountValues, "all vals", valLen, "dups", stat.DupsDeleted, "pairs", pairs, "spent ms", time.Since(start).Milliseconds(), "prune ended", lastPrunedVal == nil)

	return stat, err
}
