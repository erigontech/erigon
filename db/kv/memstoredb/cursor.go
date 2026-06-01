// Copyright 2026 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0-or-later

package memstoredb

import (
	"bytes"

	btree2 "github.com/tidwall/btree"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
)

// cursor is the cursor over a single tx-local table. Single-goroutine,
// matching MDBX. The cursor does NOT hold a live btree iterator across calls
// (creating one per call is cheap with NoLocks btrees); instead it tracks
// its position via `current` and re-seeks each operation.
type cursor struct {
	tx      *tx
	table   *table
	bucket  string
	valid   bool
	current entry
}

var (
	_ kv.Cursor          = (*cursor)(nil)
	_ kv.RwCursor        = (*cursor)(nil)
	_ kv.CursorDupSort   = (*cursor)(nil)
	_ kv.RwCursorDupSort = (*cursor)(nil)
)

func (t *tx) newCursor(table string) *cursor {
	tab := t.getOrCreateTable(table)
	return &cursor{tx: t, table: tab, bucket: table}
}

// seekToCurrent positions iter at c.current; returns false if the entry no
// longer exists (e.g. it was deleted by the same tx after the cursor moved).
func (c *cursor) seekToCurrent(iter *btree2.IterG[entry]) bool {
	if c.table.dupSort {
		if !iter.Seek(entry{k: c.current.k, v: c.current.v}) {
			return false
		}
		it := iter.Item()
		return bytes.Equal(it.k, c.current.k) && bytes.Equal(it.v, c.current.v)
	}
	if !iter.Seek(entry{k: c.current.k}) {
		return false
	}
	return bytes.Equal(iter.Item().k, c.current.k)
}

func (c *cursor) setPos(it entry) ([]byte, []byte) {
	c.valid = true
	// Stored entries are never mutated in place — Set always inserts a
	// new entry; Delete unlinks the node from the tree but the entry's
	// slices stay alive in Go memory while any cursor / snapshot holds a
	// reference. Returning the slices by reference matches MDBX's
	// "cursor-position memory, valid until next op" contract and avoids
	// a per-positioning common.Copy that was a big GC-pressure source.
	c.current = it
	return it.k, it.v
}

func (c *cursor) clearPos() ([]byte, []byte) {
	c.valid = false
	c.current = entry{}
	return nil, nil
}

// --- kv.Cursor (positioning) ---

func (c *cursor) First() ([]byte, []byte, error) {
	iter := c.table.tree.Iter()
	defer iter.Release()
	if !iter.First() {
		k, v := c.clearPos()
		return k, v, nil
	}
	k, v := c.setPos(iter.Item())
	return k, v, nil
}

func (c *cursor) Last() ([]byte, []byte, error) {
	iter := c.table.tree.Iter()
	defer iter.Release()
	if !iter.Last() {
		k, v := c.clearPos()
		return k, v, nil
	}
	k, v := c.setPos(iter.Item())
	return k, v, nil
}

func (c *cursor) Seek(seek []byte) ([]byte, []byte, error) {
	iter := c.table.tree.Iter()
	defer iter.Release()
	if !iter.Seek(entry{k: seek}) {
		k, v := c.clearPos()
		return k, v, nil
	}
	k, v := c.setPos(iter.Item())
	return k, v, nil
}

func (c *cursor) SeekExact(key []byte) ([]byte, []byte, error) {
	k, v, err := c.Seek(key)
	if err != nil || k == nil {
		return k, v, err
	}
	if !bytes.Equal(k, key) {
		k2, v2 := c.clearPos()
		return k2, v2, nil
	}
	return k, v, nil
}

func (c *cursor) Next() ([]byte, []byte, error) {
	if !c.valid {
		// MDBX: Next on a fresh cursor returns the first entry.
		return c.First()
	}
	iter := c.table.tree.Iter()
	defer iter.Release()
	if !c.seekToCurrent(&iter) {
		// Entry vanished; advance using the SAVED key as the seek point.
		if c.table.dupSort {
			iter = c.table.tree.Iter()
			if !iter.Seek(entry{k: c.current.k, v: c.current.v}) {
				k, v := c.clearPos()
				return k, v, nil
			}
		} else {
			iter = c.table.tree.Iter()
			if !iter.Seek(entry{k: c.current.k}) {
				k, v := c.clearPos()
				return k, v, nil
			}
		}
		k, v := c.setPos(iter.Item())
		return k, v, nil
	}
	if !iter.Next() {
		k, v := c.clearPos()
		return k, v, nil
	}
	k, v := c.setPos(iter.Item())
	return k, v, nil
}

func (c *cursor) Prev() ([]byte, []byte, error) {
	if !c.valid {
		// MDBX: Prev on a fresh cursor returns the last entry.
		return c.Last()
	}
	iter := c.table.tree.Iter()
	defer iter.Release()
	if !c.seekToCurrent(&iter) {
		k, v := c.clearPos()
		return k, v, nil
	}
	if !iter.Prev() {
		k, v := c.clearPos()
		return k, v, nil
	}
	k, v := c.setPos(iter.Item())
	return k, v, nil
}

func (c *cursor) Current() ([]byte, []byte, error) {
	if !c.valid {
		return nil, nil, nil
	}
	return c.current.k, c.current.v, nil
}

// --- kv.CursorDupSort ---

func (c *cursor) NextDup() ([]byte, []byte, error) {
	if !c.valid {
		return nil, nil, nil
	}
	curKey := c.current.k
	iter := c.table.tree.Iter()
	defer iter.Release()
	if !c.seekToCurrent(&iter) {
		// Current entry vanished; keep cursor at saved position so the next
		// NextNoDup can advance from c.current.k.
		return nil, nil, nil
	}
	if !iter.Next() {
		// End of tree. Cursor stays at last dup of curKey — same as MDBX
		// MDBX_NEXT_DUP returning MDBX_NOTFOUND.
		return nil, nil, nil
	}
	it := iter.Item()
	if !bytes.Equal(it.k, curKey) {
		// Past all dups of curKey. Stay at last dup of curKey.
		return nil, nil, nil
	}
	k, v := c.setPos(it)
	return k, v, nil
}

func (c *cursor) NextNoDup() ([]byte, []byte, error) {
	if !c.valid {
		// MDBX behaviour: NextNoDup with no current position behaves like First.
		return c.First()
	}
	curKey := c.current.k
	iter := c.table.tree.Iter()
	defer iter.Release()
	// Non-DupSort fast path: each key has exactly one entry, so Seek(curKey)
	// + Next is correct and O(log N).
	if !c.table.dupSort {
		if !iter.Seek(entry{k: curKey}) {
			k, v := c.clearPos()
			return k, v, nil
		}
		if !iter.Next() {
			k, v := c.clearPos()
			return k, v, nil
		}
		k, v := c.setPos(iter.Item())
		return k, v, nil
	}
	// DupSort: walk forward past every dup of curKey. Can't use
	// Seek(NextSubtree(curKey)) as a shortcut because variable-length keys
	// admit entries lexicographically between curKey and NextSubtree(curKey)
	// (e.g. "aa" < "aab" < "ab") that would be missed.
	if !iter.Seek(entry{k: curKey, v: c.current.v}) {
		k, v := c.clearPos()
		return k, v, nil
	}
	for {
		it := iter.Item()
		if !bytes.Equal(it.k, curKey) {
			// Seek already landed past curKey's dups (no dup at >=
			// c.current.v existed, or every match was returned earlier).
			k, v := c.setPos(it)
			return k, v, nil
		}
		if !iter.Next() {
			k, v := c.clearPos()
			return k, v, nil
		}
	}
}

func (c *cursor) PrevDup() ([]byte, []byte, error) {
	if !c.valid {
		return nil, nil, nil
	}
	curKey := c.current.k
	iter := c.table.tree.Iter()
	defer iter.Release()
	if !c.seekToCurrent(&iter) {
		k, v := c.clearPos()
		return k, v, nil
	}
	if !iter.Prev() {
		k, v := c.clearPos()
		return k, v, nil
	}
	it := iter.Item()
	if !bytes.Equal(it.k, curKey) {
		return nil, nil, nil
	}
	k, v := c.setPos(it)
	return k, v, nil
}

func (c *cursor) PrevNoDup() ([]byte, []byte, error) {
	if !c.valid {
		return c.Last()
	}
	// Seek to current key's start, then Prev once to land on the prior key's
	// last dup (Iter is ordered by (k,v) so the last dup of K-1 sits directly
	// before the first dup of K).
	iter := c.table.tree.Iter()
	defer iter.Release()
	if !iter.Seek(entry{k: c.current.k}) {
		// Current key is past the end of the tree; back up from the end.
		if !iter.Last() {
			k, v := c.clearPos()
			return k, v, nil
		}
		k, v := c.setPos(iter.Item())
		return k, v, nil
	}
	if !iter.Prev() {
		k, v := c.clearPos()
		return k, v, nil
	}
	k, v := c.setPos(iter.Item())
	return k, v, nil
}

func (c *cursor) SeekBothRange(key, value []byte) ([]byte, error) {
	iter := c.table.tree.Iter()
	defer iter.Release()
	if !iter.Seek(entry{k: key, v: value}) {
		c.clearPos()
		return nil, nil
	}
	it := iter.Item()
	if !bytes.Equal(it.k, key) {
		c.clearPos()
		return nil, nil
	}
	c.setPos(it)
	return it.v, nil
}

func (c *cursor) SeekBothExact(key, value []byte) ([]byte, []byte, error) {
	iter := c.table.tree.Iter()
	defer iter.Release()
	if !iter.Seek(entry{k: key, v: value}) {
		k, v := c.clearPos()
		return k, v, nil
	}
	it := iter.Item()
	if !bytes.Equal(it.k, key) || !bytes.Equal(it.v, value) {
		k, v := c.clearPos()
		return k, v, nil
	}
	k, v := c.setPos(it)
	return k, v, nil
}

func (c *cursor) FirstDup() ([]byte, error) {
	if !c.valid {
		return nil, nil
	}
	curKey := c.current.k
	iter := c.table.tree.Iter()
	defer iter.Release()
	if !iter.Seek(entry{k: curKey}) {
		return nil, nil
	}
	it := iter.Item()
	if !bytes.Equal(it.k, curKey) {
		return nil, nil
	}
	c.setPos(it)
	return it.v, nil
}

func (c *cursor) LastDup() ([]byte, error) {
	if !c.valid {
		return nil, nil
	}
	curKey := c.current.k
	iter := c.table.tree.Iter()
	defer iter.Release()
	// Walk forward from the first dup of curKey, tracking the last entry
	// whose key still matches. Using Seek({k: NextSubtree(curKey)}) + Prev
	// is unsafe with variable-length keys (a key strictly between curKey
	// and NextSubtree(curKey) — e.g. curKey="aa" admits "aab" before "ab" —
	// would make Prev land outside curKey and produce a nil return).
	if !iter.Seek(entry{k: curKey}) {
		return nil, nil
	}
	var last entry
	var found bool
	for {
		it := iter.Item()
		if !bytes.Equal(it.k, curKey) {
			break
		}
		last = it
		found = true
		if !iter.Next() {
			break
		}
	}
	if !found {
		return nil, nil
	}
	c.setPos(last)
	return last.v, nil
}

func (c *cursor) CountDuplicates() (uint64, error) {
	if !c.valid {
		return 0, nil
	}
	curKey := c.current.k
	iter := c.table.tree.Iter()
	defer iter.Release()
	if !iter.Seek(entry{k: curKey}) {
		return 0, nil
	}
	var n uint64
	for {
		it := iter.Item()
		if !bytes.Equal(it.k, curKey) {
			break
		}
		n++
		if !iter.Next() {
			break
		}
	}
	return n, nil
}

// --- kv.RwCursor ---

func (c *cursor) Put(k, v []byte) error    { return c.tx.Put(c.bucket, k, v) }
func (c *cursor) Append(k, v []byte) error { return c.tx.Append(c.bucket, k, v) }
func (c *cursor) Delete(k []byte) error    { return c.tx.Delete(c.bucket, k) }

func (c *cursor) DeleteCurrent() error {
	if !c.valid {
		return nil
	}
	c.table.tree.Delete(c.current)
	return nil
}

// --- kv.RwCursorDupSort ---

func (c *cursor) AppendDup(k, v []byte) error { return c.tx.AppendDup(c.bucket, k, v) }

func (c *cursor) DeleteExact(k, v []byte) error {
	c.table.tree.Delete(entry{k: k, v: v})
	return nil
}

func (c *cursor) DeleteCurrentDuplicates() error {
	if !c.valid {
		return nil
	}
	return c.tx.Delete(c.bucket, c.current.k)
}

// PutNoDupData: insert only if the (key, value) pair does not already exist.
// MDBX semantics: if it does exist, returns no error (MDBX_NODUPDATA → it
// returns MDBX_KEYEXIST in the C layer, but the higher-level Go binding
// surfaces a nil-no-op for the typical usage). We match the no-op behaviour.
func (c *cursor) PutNoDupData(key, value []byte) error {
	if _, found := c.table.tree.Get(entry{k: key, v: value}); found {
		return nil
	}
	c.table.tree.Set(entry{k: common.Copy(key), v: common.Copy(value)})
	return nil
}

// PutCurrent replaces the value at the current cursor position. For DupSort
// tables this is equivalent to DeleteCurrent + Put with the same key.
func (c *cursor) PutCurrent(key, value []byte) error {
	if !c.valid {
		return nil
	}
	c.table.tree.Delete(c.current)
	it := entry{k: common.Copy(key), v: common.Copy(value)}
	c.table.tree.Set(it)
	c.current = it
	return nil
}

func (c *cursor) Close() {}

// --- Range / Stream helpers ---

func collectRange(t *table, fromPrefix, toPrefix []byte, asc bool, limit int64) *sliceIter {
	entries := make([]entry, 0)
	iter := t.tree.Iter()
	defer iter.Release()

	if asc {
		var ok bool
		if fromPrefix != nil {
			ok = iter.Seek(entry{k: fromPrefix})
		} else {
			ok = iter.First()
		}
		for ok && (limit < 0 || int64(len(entries)) < limit) {
			it := iter.Item()
			if toPrefix != nil && bytes.Compare(it.k, toPrefix) >= 0 {
				break
			}
			entries = append(entries, entry{k: it.k, v: it.v})
			ok = iter.Next()
		}
	} else {
		// Desc semantics: [from, to) with from > to — iterate from `from` going
		// down to (but not including) `to`. fromPrefix=nil means start at last;
		// toPrefix=nil means no lower bound.
		var ok bool
		if fromPrefix != nil {
			nextPrefix, hasNext := kv.NextSubtree(fromPrefix)
			if hasNext {
				if iter.Seek(entry{k: nextPrefix}) {
					ok = iter.Prev()
				} else {
					ok = iter.Last()
				}
			} else {
				ok = iter.Last()
			}
		} else {
			ok = iter.Last()
		}
		for ok && (limit < 0 || int64(len(entries)) < limit) {
			it := iter.Item()
			if toPrefix != nil && bytes.Compare(it.k, toPrefix) <= 0 {
				break
			}
			entries = append(entries, entry{k: it.k, v: it.v})
			ok = iter.Prev()
		}
	}
	return &sliceIter{entries: entries}
}

func collectRangeDupSort(t *table, key, fromPrefix, toPrefix []byte, asc bool, limit int64) *sliceIter {
	entries := make([]entry, 0)
	iter := t.tree.Iter()
	defer iter.Release()

	if asc {
		ok := iter.Seek(entry{k: key, v: fromPrefix})
		for ok && (limit < 0 || int64(len(entries)) < limit) {
			it := iter.Item()
			if !bytes.Equal(it.k, key) {
				break
			}
			if toPrefix != nil && bytes.Compare(it.v, toPrefix) >= 0 {
				break
			}
			entries = append(entries, entry{k: it.k, v: it.v})
			ok = iter.Next()
		}
	} else {
		// Desc within a single key: fromPrefix is the upper-bound value (inclusive
		// going down); toPrefix is the lower-bound value (exclusive).
		var ok bool
		if fromPrefix != nil {
			nextV, hasNext := kv.NextSubtree(fromPrefix)
			if hasNext {
				if iter.Seek(entry{k: key, v: nextV}) {
					ok = iter.Prev()
				} else {
					nextKey, hasNextK := kv.NextSubtree(key)
					if hasNextK {
						if iter.Seek(entry{k: nextKey}) {
							ok = iter.Prev()
						} else {
							ok = iter.Last()
						}
					} else {
						ok = iter.Last()
					}
				}
			} else {
				nextKey, hasNextK := kv.NextSubtree(key)
				if hasNextK {
					if iter.Seek(entry{k: nextKey}) {
						ok = iter.Prev()
					} else {
						ok = iter.Last()
					}
				} else {
					ok = iter.Last()
				}
			}
		} else {
			nextKey, hasNext := kv.NextSubtree(key)
			if hasNext {
				if iter.Seek(entry{k: nextKey}) {
					ok = iter.Prev()
				} else {
					ok = iter.Last()
				}
			} else {
				ok = iter.Last()
			}
		}
		for ok && (limit < 0 || int64(len(entries)) < limit) {
			it := iter.Item()
			if !bytes.Equal(it.k, key) {
				break
			}
			if toPrefix != nil && bytes.Compare(it.v, toPrefix) <= 0 {
				break
			}
			entries = append(entries, entry{k: it.k, v: it.v})
			ok = iter.Prev()
		}
	}
	return &sliceIter{entries: entries}
}

// sliceIter is a thread-safe iterator over a pre-collected slice of entries.
type sliceIter struct {
	entries []entry
	pos     int
}

func (s *sliceIter) HasNext() bool { return s.pos < len(s.entries) }

func (s *sliceIter) Next() ([]byte, []byte, error) {
	if s.pos >= len(s.entries) {
		return nil, nil, nil
	}
	e := s.entries[s.pos]
	s.pos++
	return e.k, e.v, nil
}

func (s *sliceIter) Close() {}
