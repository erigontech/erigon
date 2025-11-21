package state

import (
	"fmt"
	"sort"

	"github.com/erigontech/erigon/db/kv"
)

// cursor over sharded tables

type MultiCursor struct {
	tables  []multiCursorTbl
	cursors []kv.Cursor
	//stepSize  uint64

	currentPos int
}

type multiCursorTbl struct {
	table string
	step  uint64
}

var _ kv.Cursor = (*MultiCursor)(nil)

func NewMultiCursor(tx kv.Tx, tblSchema string, tables []string) (*MultiCursor, error) {
	return newMultiCursor(tx, tblSchema, tables, true)
}

func newMultiCursor(tx kv.Tx, tblSchema string, tables []string, initCursors bool) (*MultiCursor, error) {
	mc := &MultiCursor{
		tables:  make([]multiCursorTbl, len(tables)),
		cursors: make([]kv.Cursor, len(tables)),
	}
	var step uint64
	var err error
	for i, tbl := range tables {
		_, err := fmt.Sscanf(tbl, tblSchema+"_%d", &step)
		if err != nil {
			return nil, err
		}
		mc.tables[i] = multiCursorTbl{table: tbl, step: step}
	}

	sort.Slice(mc.tables, func(i int, j int) bool {
		return mc.tables[i].step < mc.tables[j].step
	})

	if initCursors {
		for i, tbl := range mc.tables {
			mc.cursors[i], err = tx.Cursor(tbl.table)
			if err != nil {
				return nil, err
			}
		}
	}

	return mc, nil
}

func (mc *MultiCursor) First() ([]byte, []byte, error) {
	return mc.cursors[0].First()
}

func (mc *MultiCursor) Seek(seek []byte) ([]byte, []byte, error) {
	for i, _ := range mc.tables {
		k, v, err := mc.cursors[i].Seek(seek)
		if err != nil {
			return nil, nil, err
		}
		if k != nil || v != nil {
			mc.currentPos = i
			return k, v, nil
		}
	}

	return nil, nil, nil
}

func (mc *MultiCursor) SeekExact(seek []byte) ([]byte, []byte, error) {
	for i, _ := range mc.tables {
		k, v, err := mc.cursors[i].SeekExact(seek)
		if err != nil {
			return nil, nil, err
		}
		if k != nil || v != nil {
			mc.currentPos = i
			return k, v, nil
		}
	}
	return nil, nil, nil
}

func (mc *MultiCursor) Next() ([]byte, []byte, error) {
	for i := mc.currentPos; i < len(mc.tables); i++ {
		mc.currentPos = i
		k, v, err := mc.cursors[i].Next()
		if err != nil {
			return nil, nil, err
		}
		if k != nil || v != nil {
			return k, v, nil
		}
	}
	mc.currentPos = len(mc.tables)
	return nil, nil, nil
}

func (mc *MultiCursor) Prev() ([]byte, []byte, error) {
	for i := mc.currentPos; i >= 0; i-- {
		mc.currentPos = i
		k, v, err := mc.cursors[i].Prev()
		if err != nil {
			return nil, nil, err
		}
		if k != nil || v != nil {
			return k, v, nil
		}
	}
	mc.currentPos = 0
	return nil, nil, nil
}

func (mc *MultiCursor) Last() ([]byte, []byte, error) {
	return mc.cursors[len(mc.tables)-1].Last()
}

func (mc *MultiCursor) Current() ([]byte, []byte, error) {
	return mc.cursors[mc.currentPos].Current()
}

func (mc *MultiCursor) Close() {
	for _, c := range mc.cursors {
		c.Close()
	}
}

func (mc *MultiCursor) CursorFromStep(step uint64) (kv.Cursor, error) {
	for i, s := range mc.tables {
		if s.step == step {
			return mc.cursors[i], nil
		}
	}
	return nil, fmt.Errorf("step %d not found", step)
}

func (mc *MultiCursor) CurrentCursor() kv.Cursor {
	return mc.cursors[mc.currentPos]
}

// now the dupsort version
type MultiCursorDupSort struct {
	*MultiCursor
}

var _ kv.CursorDupSort = (*MultiCursorDupSort)(nil)

func NewMultiCursorDupSort(tx kv.Tx, tblSchema string, tables []string) (*MultiCursorDupSort, error) {
	mc, err := newMultiCursor(tx, tblSchema, tables, false)
	if err != nil {
		return nil, err
	}

	mcd := &MultiCursorDupSort{
		MultiCursor: mc,
	}

	for i, tbl := range mc.tables {
		mcd.MultiCursor.cursors[i], err = tx.CursorDupSort(tbl.table)
		if err != nil {
			return nil, err
		}
	}
	return mcd, nil
}

func (mcd *MultiCursorDupSort) SeekBothExact(key, value []byte) ([]byte, []byte, error) {
	for i, _ := range mcd.tables {
		k, v, err := mcd.cursor(i).SeekBothExact(key, value)
		if err != nil {
			return nil, nil, err
		}
		if k != nil || v != nil {
			mcd.currentPos = i
			return k, v, nil
		}
	}
	return nil, nil, nil
}

func (mcd *MultiCursorDupSort) SeekBothRange(key, value []byte) ([]byte, error) {
	for i, _ := range mcd.tables {
		k, err := mcd.cursor(i).SeekBothRange(key, value)
		if err != nil {
			return nil, err
		}
		if k != nil {
			mcd.currentPos = i
			return k, nil
		}
	}
	return nil, nil
}

func (mcd *MultiCursorDupSort) FirstDup() ([]byte, error) {
	return mcd.cursor(mcd.currentPos).FirstDup()
}

func (mcd *MultiCursorDupSort) NextDup() ([]byte, []byte, error) {
	return mcd.cursor(mcd.currentPos).NextDup()
}

func (mcd *MultiCursorDupSort) NextNoDup() ([]byte, []byte, error) {
	k, v, err := mcd.cursor(mcd.currentPos).NextNoDup()
	if err != nil {
		return nil, nil, err
	}
	if k != nil || v != nil {
		return k, v, nil
	}
	if mcd.currentPos == len(mcd.tables)-1 {
		return nil, nil, nil
	}
	mcd.currentPos++
	return mcd.NextNoDup()
}

func (mcd *MultiCursorDupSort) PrevDup() ([]byte, []byte, error) {
	return mcd.cursor(mcd.currentPos).PrevDup()
}

func (mcd *MultiCursorDupSort) PrevNoDup() ([]byte, []byte, error) {
	k, v, err := mcd.cursor(mcd.currentPos).PrevNoDup()
	if err != nil {
		return nil, nil, err
	}
	if k != nil || v != nil {
		return k, v, nil
	}
	if mcd.currentPos == 0 {
		return nil, nil, nil
	}
	mcd.currentPos--
	return mcd.PrevNoDup()
}

func (mcd *MultiCursorDupSort) LastDup() ([]byte, error) {
	return mcd.cursor(mcd.currentPos).LastDup()
}

func (mcd *MultiCursorDupSort) CountDuplicates() (uint64, error) {
	return mcd.cursor(mcd.currentPos).CountDuplicates()
}

func (mcd *MultiCursorDupSort) cursor(i int) kv.CursorDupSort {
	return mcd.MultiCursor.cursors[i].(kv.CursorDupSort)
}

func (mc *MultiCursorDupSort) CursorDupSortFromStep(step uint64) (kv.CursorDupSort, error) {
	for i, s := range mc.tables {
		if s.step == step {
			return mc.cursor(i), nil
		}
	}
	return nil, fmt.Errorf("step %d not found", step)
}

func (mc *MultiCursorDupSort) CurrentCursorDupSort() kv.CursorDupSort {
	return mc.cursor(mc.currentPos)
}
