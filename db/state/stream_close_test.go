package state

import (
	"container/heap"
	"context"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func TestHistoryRangeAsOfDBCloseClosesDupSortCursor(t *testing.T) {
	c := &testCursorDupSort{}
	hi := &HistoryRangeAsOfDB{valsCDup: c}

	hi.Close()

	require.Equal(t, 1, c.closed)
}

func TestDomainLatestIterFileCloseClosesHeldDbCursors(t *testing.T) {
	dup := &testCursorDupSort{}
	nonDup := &testCursor{}
	h := CursorHeap{
		&CursorItem{t: DB_CURSOR, key: []byte("a"), cDup: dup},
		&CursorItem{t: DB_CURSOR, key: []byte("b"), cNonDup: nonDup},
	}
	heap.Init(&h)
	hi := &DomainLatestIterFile{h: &h}

	hi.Close()

	require.Equal(t, 1, dup.closed)
	require.Equal(t, 1, nonDup.closed)
	require.Zero(t, h.Len())
}

func TestDomainLatestIterFileInitCursorMDBXClosesDupCursorOutsideRange(t *testing.T) {
	c := &testCursorDupSort{testCursor: testCursor{seekKey: []byte("b")}}
	tx := &testTx{dupCursor: c}
	hi := &DomainLatestIterFile{
		roTx: tx,
		to:   []byte("b"),
		h:    &CursorHeap{},
	}
	domainRoTx := &DomainRoTx{
		stepSize: 1,
		d:        &Domain{DomainCfg: statecfg.DomainCfg{ValuesTable: "vals"}},
	}

	err := hi.initCursorOnDB(domainRoTx)

	require.NoError(t, err)
	require.Equal(t, 1, c.closed)
	require.Zero(t, hi.h.Len())
}

func TestDomainLatestIterFileInitCursorMDBXClosesCursorOutsideRangeForLargeValues(t *testing.T) {
	c := &testCursor{seekKey: []byte("b\x00\x00\x00\x00\x00\x00\x00\x00")}
	tx := &testTx{cursor: c}
	hi := &DomainLatestIterFile{
		roTx: tx,
		to:   []byte("b"),
		h:    &CursorHeap{},
	}
	domainRoTx := &DomainRoTx{
		stepSize: 1,
		d:        &Domain{DomainCfg: statecfg.DomainCfg{LargeValues: true, ValuesTable: "vals"}},
	}

	err := hi.initCursorOnDB(domainRoTx)

	require.NoError(t, err)
	require.Equal(t, 1, c.closed)
	require.Zero(t, hi.h.Len())
}

type testTx struct {
	cursor    kv.Cursor
	dupCursor kv.CursorDupSort
}

func (tx *testTx) Has(table string, key []byte) (bool, error)      { return false, nil }
func (tx *testTx) GetOne(table string, key []byte) ([]byte, error) { return nil, nil }
func (tx *testTx) Rollback()                                       {}
func (tx *testTx) ReadSequence(table string) (uint64, error)       { return 0, nil }
func (tx *testTx) ForEach(table string, fromPrefix []byte, walker func(k, v []byte) error) error {
	return nil
}
func (tx *testTx) ForAmount(table string, prefix []byte, amount uint32, walker func(k, v []byte) error) error {
	return nil
}
func (tx *testTx) Cursor(table string) (kv.Cursor, error)               { return tx.cursor, nil }
func (tx *testTx) CursorDupSort(table string) (kv.CursorDupSort, error) { return tx.dupCursor, nil }
func (tx *testTx) Range(table string, fromPrefix, toPrefix []byte, asc order.By, limit int) (stream.KV, error) {
	return nil, nil
}
func (tx *testTx) Prefix(table string, prefix []byte) (stream.KV, error) {
	return nil, nil
}
func (tx *testTx) RangeDupSort(table string, key []byte, fromPrefix, toPrefix []byte, asc order.By, limit int) (stream.KV, error) {
	return nil, nil
}
func (tx *testTx) BucketSize(table string) (uint64, error) { return 0, nil }
func (tx *testTx) Count(bucket string) (uint64, error)     { return 0, nil }
func (tx *testTx) ListTables() ([]string, error)           { return nil, nil }
func (tx *testTx) ViewID() uint64                          { return 0 }
func (tx *testTx) CHandle() unsafe.Pointer                 { return nil }
func (tx *testTx) Apply(ctx context.Context, f func(tx kv.Tx) error) error {
	return f(tx)
}

type testCursor struct {
	seekKey []byte
	seekVal []byte
	closed  int
}

func (c *testCursor) First() ([]byte, []byte, error)               { return nil, nil, nil }
func (c *testCursor) Seek(seek []byte) ([]byte, []byte, error)     { return c.seekKey, c.seekVal, nil }
func (c *testCursor) SeekExact(key []byte) ([]byte, []byte, error) { return nil, nil, nil }
func (c *testCursor) Next() ([]byte, []byte, error)                { return nil, nil, nil }
func (c *testCursor) Prev() ([]byte, []byte, error)                { return nil, nil, nil }
func (c *testCursor) Last() ([]byte, []byte, error)                { return nil, nil, nil }
func (c *testCursor) Current() ([]byte, []byte, error)             { return nil, nil, nil }
func (c *testCursor) Close()                                       { c.closed++ }

type testCursorDupSort struct {
	testCursor
}

func (c *testCursorDupSort) SeekBothExact(key, value []byte) ([]byte, []byte, error) {
	return nil, nil, nil
}
func (c *testCursorDupSort) SeekBothRange(key, value []byte) ([]byte, error) { return nil, nil }
func (c *testCursorDupSort) FirstDup() ([]byte, error)                       { return nil, nil }
func (c *testCursorDupSort) NextDup() ([]byte, []byte, error)                { return nil, nil, nil }
func (c *testCursorDupSort) NextNoDup() ([]byte, []byte, error)              { return nil, nil, nil }
func (c *testCursorDupSort) PrevDup() ([]byte, []byte, error)                { return nil, nil, nil }
func (c *testCursorDupSort) PrevNoDup() ([]byte, []byte, error)              { return nil, nil, nil }
func (c *testCursorDupSort) LastDup() ([]byte, error)                        { return nil, nil }
func (c *testCursorDupSort) CountDuplicates() (uint64, error)                { return 0, nil }
