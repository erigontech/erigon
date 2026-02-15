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

	if limit == 0 { // limits amount of txn to be pruned
		limit = math.MaxUint64
	}
	var throttling *time.Duration
	if v := ctx.Value("throttle"); v != nil {
		throttling = v.(*time.Duration)
	}

	timeOut := 999 * time.Hour
	if limit < 1000 { //TODO: change after tests
		timeOut = 200 * time.Millisecond
	}

	var keyCursorPosition, valCursorPosition = &StartPos{}, &StartPos{}
	// invalidate progress if new params here
	if !(prevStat.TxFrom == txFrom && prevStat.TxTo == txTo) {
		prevStat.ValueProgress = First
		if keysCursor != nil {
			prevStat.KeyProgress = First
		}
	}
	if prevStat.ValueProgress == InProgress {
		valCursorPosition.StartVal, valCursorPosition.StartKey, err = valDelCursor.Seek(prevStat.LastPrunedValue)
	} else if prevStat.ValueProgress == First {
		valCursorPosition.StartVal, valCursorPosition.StartKey, err = valDelCursor.First()
	}

	if prevStat.KeyProgress == InProgress {
		keyCursorPosition.StartKey, keyCursorPosition.StartVal, err = keysCursor.Seek(prevStat.LastPrunedKey) //nolint:govet
	} else if prevStat.KeyProgress == First {
		var txKey [8]byte
		binary.BigEndian.PutUint64(txKey[:], txFrom)
		keyCursorPosition.StartKey, _, err = keysCursor.Seek(txKey[:])
	}

	var pairs, valLen uint64

	defer func() {
		logger.Debug("scan pruning res", "name", name, "txFrom", txFrom, "txTo", txTo, "limit", limit, "keys",
			stat.PruneCountTx, "vals", stat.PruneCountValues, "all vals", valLen, "dups", stat.DupsDeleted,
			"spent ms", time.Since(start).Milliseconds(),
			"key prune status", stat.KeyProgress.String(),
			"val prune status", stat.ValueProgress.String())
	}()
	if prevStat.KeyProgress != Done {
		txnb := common.Copy(keyCursorPosition.StartKey)
		// This deletion iterator goes last to preserve invariant: if some `txNum=N` pruned - it's pruned Fully
		for ; txnb != nil; txnb, _, err = keysCursor.NextNoDup() {
			if err != nil {
				return nil, fmt.Errorf("iterate over %s index keys: %w", filenameBase, err)
			}
			if time.Since(start) > timeOut {
				stat.LastPrunedKey = common.Copy(txnb)
				stat.KeyProgress = InProgress
				return stat, nil
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
	txNumBytes, val := common.Copy(valCursorPosition.StartKey), common.Copy(valCursorPosition.StartVal)

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

		default:
			return 0
		}
	}
	for ; val != nil; val, txNumBytes, err = valDelCursor.NextNoDup() {
		if err != nil {
			return nil, fmt.Errorf("iterate over %s index keys: %w", filenameBase, err)
		}
		dups, err := valDelCursor.CountDuplicates() //TODO: delete when analytics would be ready
		if err != nil {
			return nil, fmt.Errorf("iterate over %s index keys: %w", filenameBase, err)
		}
		valLen += dups
		if time.Since(start) > timeOut {
			stat.LastPrunedValue = common.Copy(val)
			stat.ValueProgress = InProgress
			return stat, nil
		}

		txNum := txNumGetter(val, txNumBytes)
		//println("txnum first", txNum, txFrom, txTo)
		lastDupTxNumB, err := valDelCursor.LastDup()
		if err != nil {
			return nil, fmt.Errorf("LastDup iterate over %s index keys: %w", filenameBase, err)
		}
		_, err = valDelCursor.FirstDup()
		if err != nil {
			return nil, fmt.Errorf("FirstDup iterate over %s index keys: %w", filenameBase, err)
		}
		lastDupTxNum := txNumGetter(val, lastDupTxNumB)
		//println("txnum last", lastDupTxNum)
		if txNum >= txTo {
			continue
		}
		dupsDelete := lastDupTxNum < txTo && txNum >= txFrom
		if asserts && txNum < txFrom {
			panic(fmt.Errorf("assert: index pruning txn=%d [%d-%d)", txNum, txFrom, txTo))
		}

		stat.MinTxNum = min(stat.MinTxNum, txNum)
		stat.MaxTxNum = max(stat.MaxTxNum, txNum)
		if dupsDelete {
			if throttling != nil {
				time.Sleep(*throttling)
			}
			//println("deleted", hex.EncodeToString(val), txNumGetter(val, txNumBytes), dups)
			err = valDelCursor.DeleteCurrentDuplicates()
			if err != nil {
				return nil, fmt.Errorf("iterate over %s index keys: %w", filenameBase, err)
			}
			if dups > 1 {
				stat.DupsDeleted += dups
			}
			stat.PruneCountValues += dups
		} else {
			for ; txNumBytes != nil; _, txNumBytes, err = valDelCursor.NextDup() {
				if err != nil {
					return nil, fmt.Errorf("iterate over %s index keys: %w", filenameBase, err)
				}
				txNumDup := txNumGetter(val, txNumBytes)
				//println("txnum in loop", txNumDup)
				if txNumDup < txFrom {
					continue
				}
				if txNumDup >= txTo {
					break
				}
				if throttling != nil {
					time.Sleep(*throttling)
				}
				if time.Since(start) > timeOut {
					stat.LastPrunedValue = common.Copy(val)
					stat.ValueProgress = InProgress
					return stat, nil
				}
				//println("txnum passed checks loop", txNumDup)

				stat.MinTxNum = min(stat.MinTxNum, txNumDup)
				stat.MaxTxNum = max(stat.MaxTxNum, txNumDup)
				//println("deleted loop", hex.EncodeToString(val))
				if err = valDelCursor.DeleteCurrent(); err != nil {
					return nil, err
				}
				stat.PruneCountValues++
			}
		}

		select {
		case <-logEvery.C:
			if len(txNumBytes) >= 8 {
				txNum = binary.BigEndian.Uint64(txNumBytes)
			}
			logger.Info("[snapshots] prune index", "name", filenameBase, "pruned tx", stat.PruneCountTx,
				"pruned values", stat.PruneCountValues,
				"steps", fmt.Sprintf("%.2f-%.2f", float64(txFrom)/float64(stepSize), float64(txNum)/float64(stepSize)))
		default:
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}

	stat.LastPrunedValue = nil
	stat.ValueProgress = Done
	return stat, err
}
