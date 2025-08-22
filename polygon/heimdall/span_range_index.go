package heimdall

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/polygon/polygoncommon"
)

type spanRangeIndex struct {
	db    *polygoncommon.Database
	table string
}

func NewSpanRangeIndex(db *polygoncommon.Database, table string) *spanRangeIndex {
	return &spanRangeIndex{db, table}
}

func (i *spanRangeIndex) WithTx(tx kv.Tx) RangeIndexer {
	return &txSpanRangeIndex{i, tx}
}

// Put a mapping from a range to an id.
func (i *spanRangeIndex) Put(ctx context.Context, r ClosedRange, id uint64) error {
	tx, err := i.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := i.WithTx(tx).Put(ctx, r, id); err != nil {
		return err
	}

	return tx.Commit()
}

// Lookup an id of a span given by blockNum within that range.
func (i *spanRangeIndex) Lookup(ctx context.Context, blockNum uint64) (uint64, bool, error) {
	var id uint64
	var ok bool

	err := i.db.View(ctx, func(tx kv.Tx) error {
		var err error
		id, ok, err = i.WithTx(tx).Lookup(ctx, blockNum)
		return err
	})
	return id, ok, err
}

func (i *spanRangeIndex) Last(ctx context.Context) (uint64, bool, error) {
	var lastKey uint64
	var ok bool

	err := i.db.View(ctx, func(tx kv.Tx) error {
		var err error
		lastKey, ok, err = i.WithTx(tx).Last(ctx)
		return err
	})
	return lastKey, ok, err
}

// Lookup ids for the given range [blockFrom, blockTo). Return boolean which checks if the result is reliable to use, because
// heimdall data can be not published yet for [blockFrom, blockTo), in that case boolean OK will be false
func (i *spanRangeIndex) GetIDsBetween(ctx context.Context, blockFrom, blockTo uint64) ([]uint64, bool, error) {
	var ids []uint64
	var ok bool

	err := i.db.View(ctx, func(tx kv.Tx) error {
		var err error
		ids, ok, err = i.WithTx(tx).GetIDsBetween(ctx, blockFrom, blockTo)
		return err
	})
	return ids, ok, err
}

type txSpanRangeIndex struct {
	*spanRangeIndex
	tx kv.Tx
}

func NewTxSpanRangeIndex(db kv.RoDB, table string, tx kv.Tx) *txSpanRangeIndex {
	return &txSpanRangeIndex{&spanRangeIndex{db: polygoncommon.AsDatabase(db.(kv.RwDB)), table: table}, tx}
}

func (i *txSpanRangeIndex) Put(ctx context.Context, r ClosedRange, id uint64) error {
	key := rangeIndexKey(r.Start) // use span.StartBlock as key
	tx, ok := i.tx.(kv.RwTx)

	if !ok {
		return errors.New("tx not writable")
	}
	valuePair := writeSpanIdEndBlockPair(id, r.End) // write (spanId, EndBlock) pair to buf
	return tx.Put(i.table, key[:], valuePair[:])
}

// Returns max span.Id such that span.StartBlock <= blockNum &&  blockNum <= span.EndBlock
func (i *txSpanRangeIndex) Lookup(ctx context.Context, blockNum uint64) (uint64, bool, error) {
	cursor, err := i.tx.Cursor(i.table)
	if err != nil {
		return 0, false, err
	}
	defer cursor.Close()

	// use Seek(blockNum) as the starting point for the search.
	key := rangeIndexKey(blockNum)
	startBlockRaw, valuePair, err := cursor.Seek(key[:])
	if err != nil {
		return 0, false, err
	}
	// seek not found, check the last entry as the only candidate
	if valuePair == nil {
		// get latest then
		lastStartBlockRaw, lastValuePair, err := cursor.Last()
		if err != nil {
			return 0, false, err
		}
		if lastValuePair == nil {
			return 0, false, nil
		}
		lastStartBlock := rangeIndexKeyParse(lastStartBlockRaw)
		lastSpanId, lastEndBlock := rangeIndexValuePairParse(lastValuePair)
		// sanity check
		isInRange := blockNumInRange(blockNum, lastStartBlock, lastEndBlock)
		if !isInRange {
			return 0, false, fmt.Errorf("SpanIndexLookup(%d) returns Span{Id:%d, StartBlock:%d, EndBlock:%d } not containing blockNum=%d", blockNum, lastSpanId, lastStartBlock, lastEndBlock, blockNum)
		}
		// happy case
		return lastSpanId, true, nil

	}

	var lastSpanIdInRange = uint64(0)
	currStartBlock := rangeIndexKeyParse(startBlockRaw)
	currSpanId, currEndBlock := rangeIndexValuePairParse(valuePair)
	isInRange := blockNumInRange(blockNum, currStartBlock, currEndBlock)
	if isInRange { // cursor.Seek(blockNum) is in range
		lastSpanIdInRange = currSpanId
	}

	for { // from this point walk backwards the table until the blockNum is out of range
		prevStartBlockRaw, prevValuePair, err := cursor.Prev()
		if err != nil {
			return 0, false, err
		}
		// this could happen if we've walked all the way to the first entry in the table, and there is no more Prev()
		if prevValuePair == nil {
			break
		}
		prevStartBlock := rangeIndexKeyParse(prevStartBlockRaw)
		prevSpanId, prevBlock := rangeIndexValuePairParse(prevValuePair)
		isInRange := blockNumInRange(blockNum, prevStartBlock, prevBlock)
		if !isInRange {
			break // we have walked out of range, break to return current known lastSpanIdInRange
		}
		if isInRange && prevSpanId > lastSpanIdInRange { // a span in range with higher span id was found
			lastSpanIdInRange = prevSpanId
		}
	}
	return lastSpanIdInRange, true, nil
}

// last key in the index
func (i *txSpanRangeIndex) Last(ctx context.Context) (uint64, bool, error) {
	cursor, err := i.tx.Cursor(i.table)
	if err != nil {
		return 0, false, err
	}
	defer cursor.Close()
	key, value, err := cursor.Last()
	if err != nil {
		return 0, false, err
	}

	if value == nil || key == nil { // table is empty
		return 0, false, nil
	}

	lastKey := rangeIndexKeyParse(key)
	return lastKey, true, nil
}

func (i *txSpanRangeIndex) GetIDsBetween(ctx context.Context, blockFrom, blockTo uint64) ([]uint64, bool, error) {
	startId, ok, err := i.Lookup(ctx, blockFrom)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	endId, ok, err := i.Lookup(ctx, blockTo)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	return []uint64{startId, endId}, true, nil
}

func blockNumInRange(blockNum, startBlock, endBlock uint64) bool {
	return startBlock <= blockNum && blockNum <= endBlock
}

// Write (spanId, endBlock) to buffer
func writeSpanIdEndBlockPair(spanId uint64, spanEndBlock uint64) [16]byte {
	result := [16]byte{}
	binary.BigEndian.PutUint64(result[:], spanId)
	binary.BigEndian.PutUint64(result[8:], spanEndBlock)
	return result
}

// Parse to pair (uint64,uint64)
func rangeIndexValuePairParse(valuePair []byte) (uint64, uint64) {
	first := binary.BigEndian.Uint64(valuePair[:8])
	second := binary.BigEndian.Uint64(valuePair[8:])
	return first, second
}
