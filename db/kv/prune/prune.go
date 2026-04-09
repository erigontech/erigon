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
)

type Stat struct {
	MinTxNum         uint64
	MaxTxNum         uint64
	PruneCountTx     uint64
	PruneCountValues uint64
	DupsDeleted      uint64
	LastPrunedValue  []byte
	LastPrunedKey    []byte
	KeyProgress      Progress
	ValueProgress    Progress
	TxFrom           uint64
	TxTo             uint64
}

type Progress int

const (
	First Progress = iota
	InProgress
	Done
)

func (p Progress) String() string {
	switch p {
	case First:
		return "First"
	case InProgress:
		return "InProgress"
	case Done:
		return "Done"
	default:
		return "Unknown"
	}
}

type StorageMode int

const (
	DefaultStorageMode StorageMode = iota
	KeyStorageMode
	PrefixValStorageMode //TODO: change name
	StepValueStorageMode
	StepKeyStorageMode
	ValueOffset8StorageMode // txNum at val[8:16], used by TxLookup
)

func HashSeekingPrune(
	ctx context.Context,
	name, filenameBase, tmp string,
	txFrom, txTo, limit, stepSize uint64,
	logEvery *time.Ticker,
	logger log.Logger,
	keysCursor kv.RwCursorDupSort, valDelCursor kv.PseudoDupSortRwCursor,
	asserts bool,
	mode StorageMode,
) (stat *Stat, err error) {
	stat = &Stat{MinTxNum: math.MaxUint64}
	start := time.Now()

	if limit == 0 { // limits amount of txn to be pruned
		limit = math.MaxUint64
	}

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
			//seek := make([]byte, 8, 256)
			seek := append(bytes.Clone(key), txnm...)
			if err := valDelCursor.Delete(seek); err != nil {
				return err
			}
		case PrefixValStorageMode:
			vv, err := valDelCursor.(kv.RwCursorDupSort).SeekBothRange(key, txnm)
			if err != nil {
				return err
			}
			if len(vv) < 8 {
				return fmt.Errorf("prune history %s got invalid value length: %d < 8", filenameBase, len(vv))
			}
			if vtx := binary.BigEndian.Uint64(vv); vtx != binary.BigEndian.Uint64(txnm) {
				return fmt.Errorf("prune history %s got invalid txNum: found %d != %d wanted", filenameBase, vtx, binary.BigEndian.Uint64(txnm))
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

	logger.Debug("hash prune res", "name", name, "txFrom", txFrom, "txTo", txTo, "limit", limit, "keys", stat.PruneCountTx, "vals", stat.PruneCountValues, "spent ms", time.Since(start).Milliseconds())

	return stat, err
}

type StartPos struct {
	StartKey []byte
	StartVal []byte
}

func TableScanningPrune(
	ctx context.Context,
	name, filenameBase string,
	txFrom, txTo, limit, stepSize uint64,
	logEvery *time.Ticker,
	logger log.Logger,
	keysCursor kv.RwCursorDupSort, valDelCursor kv.PseudoDupSortRwCursor,
	asserts bool,
	prevStat *Stat,
	mode StorageMode,
) (stat *Stat, err error) {
	stat = &Stat{MinTxNum: math.MaxUint64}
	start := time.Now()
	defer func() {
		logger.Trace("scan prune res", "name", name, "txFrom", txFrom, "txTo", txTo, "limit", limit, "keys",
			stat.PruneCountTx, "vals", stat.PruneCountValues, "dups", stat.DupsDeleted,
			"spent ms", time.Since(start).Milliseconds(),
			"key prune status", stat.KeyProgress.String(),
			"val prune status", stat.ValueProgress.String())
	}()

	if limit == 0 { // limits amount of txn to be pruned
		limit = math.MaxUint64
	}
	var throttling *time.Duration
	if v := ctx.Value("throttle"); v != nil {
		throttling = v.(*time.Duration)
	}

	// invalidate progress if new params here
	if !(prevStat.TxFrom == txFrom && prevStat.TxTo == txTo) {
		prevStat.ValueProgress = First
		if keysCursor != nil {
			prevStat.KeyProgress = First
		}
	}

	var keyCursorPosition = &StartPos{}
	if keysCursor != nil {
		if prevStat.KeyProgress == InProgress {
			keyCursorPosition.StartKey, keyCursorPosition.StartVal, err = keysCursor.Seek(prevStat.LastPrunedKey) //nolint:govet
		} else if prevStat.KeyProgress == First {
			var txKey [8]byte
			binary.BigEndian.PutUint64(txKey[:], txFrom)
			keyCursorPosition.StartKey, _, err = keysCursor.Seek(txKey[:])
		}
	}

	if prevStat.KeyProgress != Done {
		txnb := common.Copy(keyCursorPosition.StartKey)
		// This deletion iterator goes last to preserve invariant: if some `txNum=N` pruned - it's pruned Fully
		for ; txnb != nil; txnb, _, err = keysCursor.NextNoDup() {
			if err != nil {
				return nil, fmt.Errorf("iterate over %s index keys: %w", filenameBase, err)
			}
			select {
			case <-ctx.Done():
				stat.LastPrunedKey = common.Copy(txnb)
				stat.KeyProgress = InProgress
				return stat, nil
			default:
			}
			txNum := binary.BigEndian.Uint64(txnb)
			if txNum >= txTo {
				break
			}
			stat.PruneCountTx++
			if throttling != nil {
				time.Sleep(*throttling)
			}
			//println("key", hex.EncodeToString(txnb), "value", hex.EncodeToString(val))
			if err = keysCursor.DeleteCurrentDuplicates(); err != nil {
				return nil, err
			}
		}
	}

	stat.KeyProgress = Done
	stat.LastPrunedKey = nil

	// Invariant: if some `txNum=N` pruned - it's pruned Fully
	// Means: can use DeleteCurrentDuplicates all values of given `txNum`
	txNumGetter := func(key, val []byte) uint64 { // key == valCursor key, val – usually txnum
		switch mode {
		case KeyStorageMode:
			return binary.BigEndian.Uint64(key[len(key)-8:])
		case PrefixValStorageMode:
			return binary.BigEndian.Uint64(val)
		case StepValueStorageMode:
			return kv.Step(^binary.BigEndian.Uint64(val)).ToTxNum(stepSize)
		case StepKeyStorageMode:
			return kv.Step(^binary.BigEndian.Uint64(key[len(key)-8:])).ToTxNum(stepSize)
		case DefaultStorageMode:
			return binary.BigEndian.Uint64(val)
		case ValueOffset8StorageMode:
			return binary.BigEndian.Uint64(val[8:])
		default:
			return 0
		}
	}

	lastVal, err := tableScanningPrune(ctx, stat, filenameBase, txFrom, txTo, txNumGetter, valDelCursor, keysCursor, asserts, throttling, logEvery, logger, prevStat.ValueProgress, prevStat.LastPrunedValue)
	if err != nil {
		return nil, err
	}
	if lastVal != nil {
		stat.LastPrunedValue = lastVal
		stat.ValueProgress = InProgress
	} else {
		stat.LastPrunedValue = nil
		stat.ValueProgress = Done
	}
	return stat, nil
}

// tableScanningPrune scans values and deletes those in [txFrom, txTo).
// Returns the last cursor position (non-nil) if interrupted by ctx, or nil if completed.
func tableScanningPrune(
	ctx context.Context,
	stat *Stat,
	filenameBase string,
	txFrom, txTo uint64,
	txNumGetter func(key, val []byte) uint64,
	valDelCursor kv.PseudoDupSortRwCursor,
	keysCursor kv.RwCursorDupSort,
	asserts bool,
	throttling *time.Duration,
	logEvery *time.Ticker,
	logger log.Logger,
	valueProgress Progress,
	lastPrunedValue []byte,
) (interrupted []byte, err error) {
	var val, txNumBytes []byte
	switch valueProgress {
	case InProgress:
		val, txNumBytes, err = valDelCursor.Seek(lastPrunedValue)
	case First:
		val, txNumBytes, err = valDelCursor.First()
	default: // Done or unknown — nothing to scan
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("cursor position %s: %w", filenameBase, err)
	}
	for ; val != nil; val, txNumBytes, err = valDelCursor.NextNoDup() {
		if err != nil {
			return nil, fmt.Errorf("iterate over %s index keys: %w", filenameBase, err)
		}

		if ctx.Err() != nil {
			return common.Copy(val), nil
		}

		txNum := txNumGetter(val, txNumBytes)
		// Early skip: avoid LastDup/FirstDup/CountDuplicates cursor ops for out-of-range entries
		if txNum >= txTo {
			continue
		}

		if asserts && txNum < txFrom {
			panic(fmt.Errorf("assert: index pruning txn=%d [%d-%d)", txNum, txFrom, txTo))
		}

		lastDupTxNumB, err := valDelCursor.LastDup()
		if err != nil {
			return nil, fmt.Errorf("LastDup iterate over %s index keys: %w", filenameBase, err)
		}
		lastDupTxNum := txNumGetter(val, lastDupTxNumB)

		stat.MinTxNum = min(stat.MinTxNum, txNum)
		stat.MaxTxNum = max(stat.MaxTxNum, txNum)

		// All dups in prune range: bulk delete without repositioning cursor
		if lastDupTxNum < txTo && txNum >= txFrom {
			if throttling != nil {
				time.Sleep(*throttling)
			}
			dups, err := valDelCursor.CountDuplicates()
			if err != nil {
				return nil, fmt.Errorf("count dups %s: %w", filenameBase, err)
			}
			err = valDelCursor.DeleteCurrentDuplicates()
			if err != nil {
				return nil, fmt.Errorf("iterate over %s index keys: %w", filenameBase, err)
			}
			if dups > 1 {
				stat.DupsDeleted += dups
			}
			stat.PruneCountValues += dups
		} else {
			// Selective per-dup deletion: reposition to first dup for iteration
			_, err = valDelCursor.FirstDup()
			if err != nil {
				return nil, fmt.Errorf("FirstDup iterate over %s index keys: %w", filenameBase, err)
			}
			for ; txNumBytes != nil; _, txNumBytes, err = valDelCursor.NextDup() {
				if err != nil {
					return nil, fmt.Errorf("iterate over %s index keys: %w", filenameBase, err)
				}
				txNumDup := txNumGetter(val, txNumBytes)
				if txNumDup < txFrom {
					continue
				}
				if txNumDup >= txTo {
					break
				}
				if throttling != nil {
					time.Sleep(*throttling)
				}
				if ctx.Err() != nil {
					return common.Copy(val), nil
				}

				stat.MinTxNum = min(stat.MinTxNum, txNumDup)
				stat.MaxTxNum = max(stat.MaxTxNum, txNumDup)
				if err = valDelCursor.DeleteCurrent(); err != nil {
					return nil, err
				}
				stat.PruneCountValues++
			}
		}

		select {
		case <-logEvery.C:
			args := []interface{}{"name", filenameBase, "pruned values", stat.PruneCountValues}
			if keysCursor != nil {
				args = append(args, "pruned tx", stat.PruneCountTx)
			}
			args = append(args, "val status", stat.ValueProgress.String())
			logger.Info("[snapshots] prune index", args...)
		default:
		}
	}

	return nil, nil
}
