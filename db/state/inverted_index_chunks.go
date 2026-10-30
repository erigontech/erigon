// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"sort"
	"time"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/prune"
)

// The values table of an inverted index is DupSort: key -> chunks of ascending txNums.
// A chunk is BE8(max txNum) followed by native-endian uint16 deltas (txNum - min) of every txNum but min,
// so min = max - last delta, the dups of a key sort by their max and SeekBothRange(key, BE8(t))
// finds the chunk with the first txNum >= t. A one-txNum chunk is the 8-byte value of the format
// before chunking. Deltas use the native byte order because chaindata is not portable across
// byte orders anyway, so readers can use them as []uint16.

// iiChunkMaxBytes keeps a chunk far below the inline value limit of a 4 KB page.
const iiChunkMaxBytes = 8 + 512

func appendIIChunk(dst []byte, txNums []uint64) []byte {
	dst = binary.BigEndian.AppendUint64(dst, txNums[len(txNums)-1])
	for _, n := range txNums[1:] {
		dst = binary.NativeEndian.AppendUint16(dst, uint16(n-txNums[0]))
	}
	return dst
}

// iiChunk reads the txNums of a chunk in place.
type iiChunk struct {
	min    uint64
	deltas []byte
}

func parseIIChunk(v []byte) (iiChunk, error) {
	if len(v) < 8 || len(v)%2 != 0 {
		return iiChunk{}, fmt.Errorf("inverted index chunk has bad length: %x", v)
	}
	c := iiChunk{min: binary.BigEndian.Uint64(v), deltas: v[8:]}
	if len(c.deltas) > 0 {
		last := uint64(binary.NativeEndian.Uint16(c.deltas[len(c.deltas)-2:]))
		if last > c.min {
			return iiChunk{}, fmt.Errorf("inverted index chunk corrupted: %x", v)
		}
		c.min -= last
	}
	return c, nil
}

func (c iiChunk) len() int { return 1 + len(c.deltas)/2 }

func (c iiChunk) at(i int) uint64 {
	if i == 0 {
		return c.min
	}
	return c.min + uint64(binary.NativeEndian.Uint16(c.deltas[2*i-2:]))
}

// search returns the index of the first txNum >= t, or len() if there is none.
func (c iiChunk) search(t uint64) int {
	return sort.Search(c.len(), func(i int) bool { return c.at(i) >= t })
}

func decodeIIChunk(dst []uint64, v []byte) ([]uint64, error) {
	c, err := parseIIChunk(v)
	if err != nil {
		return dst, err
	}
	for i := range c.len() {
		n := c.at(i)
		if i > 0 && n <= dst[len(dst)-1] {
			return dst, fmt.Errorf("inverted index chunk corrupted: %x", v)
		}
		dst = append(dst, n)
	}
	return dst, nil
}

// iiChunkFit returns how many leading txNums fit into one chunk.
func iiChunkFit(txNums []uint64) int {
	n := min(len(txNums), 1+(iiChunkMaxBytes-8)/2)
	for i := 1; i < n; i++ {
		if txNums[i]-txNums[0] > math.MaxUint16 {
			return i
		}
	}
	return n
}

// iiChunkWriter adds txNums arriving in ETL order (grouped by key) to the values table.
type iiChunkWriter struct {
	c      kv.RwCursorDupSort
	key    []byte
	txNums []uint64
	old    []uint64
	buf    []byte
}

func (w *iiChunkWriter) add(key, txNum []byte) error {
	if len(w.txNums) > 0 && !bytes.Equal(key, w.key) {
		if err := w.flush(); err != nil {
			return err
		}
	}
	if len(w.txNums) == 0 {
		w.key = append(w.key[:0], key...)
	}
	w.txNums = append(w.txNums, binary.BigEndian.Uint64(txNum))
	return nil
}

func (w *iiChunkWriter) flush() error {
	if len(w.txNums) == 0 {
		return nil
	}
	slices.Sort(w.txNums)
	err := w.put(w.key, slices.Compact(w.txNums))
	w.txNums = w.txNums[:0]
	return err
}

func (w *iiChunkWriter) put(key []byte, txNums []uint64) error {
	var seek [8]byte
	binary.BigEndian.PutUint64(seek[:], txNums[0])
	v, err := w.c.SeekBothRange(key, seek[:])
	if err != nil {
		return err
	}
	if v != nil { // txNums go into or before existing chunks
		return w.mergeFrom(key, seek[:], v, txNums)
	}
	return w.appendToLast(key, txNums)
}

// mergeFrom rewrites the chunks of key from v, the one at the cursor, on together with txNums.
func (w *iiChunkWriter) mergeFrom(key, seek, v []byte, txNums []uint64) error {
	w.old = w.old[:0]
	for v != nil {
		if err := w.takeChunk(v); err != nil {
			return err
		}
		next, err := w.c.SeekBothRange(key, seek)
		if err != nil {
			return err
		}
		v = next
	}
	w.old = append(w.old, txNums...)
	slices.Sort(w.old)
	return w.putChunks(key, slices.Compact(w.old))
}

// takeChunk moves the txNums of chunk v, the one at the cursor, to w.old.
func (w *iiChunkWriter) takeChunk(v []byte) error {
	old, err := decodeIIChunk(w.old, v)
	if err != nil {
		return err
	}
	w.old = old
	return w.c.DeleteCurrent()
}

// appendToLast adds txNums above every chunk of key to its last chunk and new chunks after it.
func (w *iiChunkWriter) appendToLast(key []byte, txNums []uint64) error {
	k, _, err := w.c.SeekExact(key)
	if err != nil {
		return err
	}
	if k == nil {
		return w.putChunks(key, txNums)
	}
	last, err := w.c.LastDup()
	if err != nil {
		return err
	}
	if w.old, err = decodeIIChunk(w.old[:0], last); err != nil {
		return err
	}
	lastLen := len(w.old)
	w.old = append(w.old, txNums...)
	n := iiChunkFit(w.old)
	if n > lastLen {
		w.buf = appendIIChunk(w.buf[:0], w.old[:n])
		if err := w.c.PutCurrent(key, w.buf); err != nil {
			return err
		}
	}
	return w.putChunks(key, w.old[n:])
}

func (w *iiChunkWriter) putChunks(key []byte, txNums []uint64) error {
	for len(txNums) > 0 {
		n := iiChunkFit(txNums)
		w.buf = appendIIChunk(w.buf[:0], txNums[:n])
		if err := w.c.Put(key, w.buf); err != nil {
			return err
		}
		txNums = txNums[n:]
	}
	return nil
}

// iiChunkDeleter removes txNums from the values table and counts what it removed.
type iiChunkDeleter struct {
	c        kv.RwCursorDupSort
	key      []byte
	vals     []uint64
	buf      []byte
	count    uint64
	min, max uint64
}

func newIIChunkDeleter(c kv.RwCursorDupSort) *iiChunkDeleter {
	return &iiChunkDeleter{c: c, min: math.MaxUint64}
}

// deleteRange removes the txNums of key in [from, to).
func (d *iiChunkDeleter) deleteRange(key []byte, from, to uint64) error {
	var seek [8]byte
	binary.BigEndian.PutUint64(seek[:], from)
	for {
		v, err := d.c.SeekBothRange(key, seek[:])
		if err != nil || v == nil {
			return err
		}
		if d.vals, err = decodeIIChunk(d.vals[:0], v); err != nil {
			return err
		}
		chunk := d.vals
		i, _ := slices.BinarySearch(chunk, from)
		j, _ := slices.BinarySearch(chunk, to)
		if i == j { // chunk[i] >= to
			return nil
		}
		d.count += uint64(j - i)
		d.min, d.max = min(d.min, chunk[i]), max(d.max, chunk[j-1])
		reachedTo := j < len(chunk) // chunk[j] >= to: later chunks are out of range
		chunk = slices.Delete(chunk, i, j)
		if len(chunk) == 0 {
			if err := d.c.DeleteCurrent(); err != nil {
				return err
			}
			continue
		}
		d.buf = appendIIChunk(d.buf[:0], chunk)
		if err := d.c.PutCurrent(key, d.buf); err != nil {
			return err
		}
		if reachedTo {
			return nil
		}
	}
}

// pruneKey removes [from, to) of the key at k and returns the next key.
func (d *iiChunkDeleter) pruneKey(k []byte, from, to uint64) ([]byte, error) {
	d.key = append(d.key[:0], k...)
	if err := d.deleteRange(d.key, from, to); err != nil {
		return nil, err
	}
	next, _, err := d.c.Seek(d.key)
	if err == nil && next != nil && bytes.Equal(next, d.key) {
		next, _, err = d.c.NextNoDup()
	}
	return next, err
}

// iiChunkPruneCursor lets prune.HashSeekingPrune delete single txNums from chunks.
type iiChunkPruneCursor struct {
	kv.RwCursorDupSort
	d *iiChunkDeleter
}

func (c iiChunkPruneCursor) DeleteExact(key, txNum []byte) error {
	t := binary.BigEndian.Uint64(txNum)
	return c.d.deleteRange(key, t, t+1)
}

// pruneChunks scans the values table of a standalone index and removes [txFrom, txTo), resuming from stat.
func (iit *InvertedIndexRoTx) pruneChunks(ctx context.Context, c kv.RwCursorDupSort, txFrom, txTo uint64, logEvery *time.Ticker, stat *prune.Stat) (err error) {
	var k []byte
	switch stat.ValueProgress {
	case prune.InProgress:
		k, _, err = c.Seek(stat.LastPrunedValue)
	case prune.First:
		k, _, err = c.First()
	default:
		return nil
	}
	if err != nil {
		return fmt.Errorf("iterate over %s index values: %w", iit.ii.FilenameBase, err)
	}
	d := newIIChunkDeleter(c)
	defer func() {
		stat.PruneCountValues += d.count
		if d.count > 0 {
			stat.MinTxNum, stat.MaxTxNum = min(stat.MinTxNum, d.min), max(stat.MaxTxNum, d.max)
		}
	}()
	for k != nil {
		if ctx.Err() != nil {
			stat.LastPrunedValue, stat.ValueProgress = bytes.Clone(k), prune.InProgress
			return nil
		}
		stat.ScanCountKeys++
		if k, err = d.pruneKey(k, txFrom, txTo); err != nil {
			return fmt.Errorf("prune %s index values: %w", iit.ii.FilenameBase, err)
		}
		if stat.ScanCountKeys%1024 == 0 {
			select {
			case <-logEvery.C:
				iit.ii.logger.Info("[snapshots] prune index", "name", iit.ii.FilenameBase, "scanned keys", stat.ScanCountKeys, "pruned values", stat.PruneCountValues+d.count)
			default:
			}
		}
	}
	stat.LastPrunedValue, stat.ValueProgress = nil, prune.Done
	return nil
}

// iiChunkStream iterates the txNums of one key: asc [from, to), desc (to, from]; -1 is unbounded.
type iiChunkStream struct {
	c     kv.CursorDupSort
	key   []byte
	to    int
	asc   bool
	limit int

	chunk   iiChunk
	i       int
	hasNext bool
	nextN   uint64
	err     error
}

func newIIChunkStream(tx kv.Tx, table string, key []byte, from, to int, asc order.By, limit int) (*iiChunkStream, error) {
	c, err := tx.CursorDupSort(table) //nolint:gocritic // closed by iiChunkStream.Close
	if err != nil {
		return nil, err
	}
	s := &iiChunkStream{c: c, key: key, to: to, asc: bool(asc), limit: limit}
	start := uint64(math.MaxUint64)
	if from >= 0 {
		start = uint64(from)
	} else if asc {
		start = 0
	}
	s.seek(start)
	return s, nil
}

// seek positions on the first txNum not before t.
func (s *iiChunkStream) seek(t uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], t)
	v, err := s.c.SeekBothRange(s.key, b[:])
	if err != nil {
		s.err = err
		return
	}
	if v == nil && !s.asc { // t is above every chunk: start from the last one
		k, _, err := s.c.SeekExact(s.key)
		if err != nil || k == nil {
			s.err, s.hasNext = err, false
			return
		}
		if v, err = s.c.LastDup(); err != nil {
			s.err = err
			return
		}
	}
	if !s.load(v) {
		return
	}
	if s.asc {
		s.i = s.chunk.search(t)
	} else {
		s.i = s.lastAtMost(t)
	}
	s.advance()
}

// lastAtMost returns the index of the last txNum of the chunk <= t, or -1.
func (s *iiChunkStream) lastAtMost(t uint64) int {
	i := s.chunk.search(t)
	if i < s.chunk.len() && s.chunk.at(i) == t {
		i++
	}
	return i - 1
}

func (s *iiChunkStream) load(v []byte) bool {
	if v == nil {
		s.hasNext = false
		return false
	}
	var err error
	if s.chunk, err = parseIIChunk(v); err != nil {
		s.err = err
		return false
	}
	return true
}

func (s *iiChunkStream) advance() {
	if s.asc {
		for s.i >= s.chunk.len() {
			_, v, err := s.c.NextDup()
			if err != nil {
				s.err = err
				return
			}
			if !s.load(v) {
				return
			}
			s.i = 0
		}
		n := s.chunk.at(s.i)
		s.i++
		s.hasNext = s.to < 0 || n < uint64(s.to)
		s.nextN = n
		return
	}
	for s.i < 0 {
		_, v, err := s.c.PrevDup()
		if err != nil {
			s.err = err
			return
		}
		if !s.load(v) {
			return
		}
		s.i = s.chunk.len() - 1
	}
	n := s.chunk.at(s.i)
	s.i--
	s.hasNext = s.to < 0 || n > uint64(s.to)
	s.nextN = n
}

func (s *iiChunkStream) HasNext() bool {
	if s.err != nil {
		return true
	}
	return s.limit != 0 && s.hasNext
}

func (s *iiChunkStream) Next() (uint64, error) {
	if s.err != nil {
		return 0, s.err
	}
	s.limit--
	n := s.nextN
	s.advance()
	return n, nil
}

func (s *iiChunkStream) CanSeek() bool { return s.limit < 0 }

func (s *iiChunkStream) Seek(t uint64) error {
	if s.err != nil || !s.hasNext || s.asc && t <= s.nextN || !s.asc && t >= s.nextN {
		return s.err
	}
	if s.asc && t <= s.chunk.at(s.chunk.len()-1) {
		s.i = s.chunk.search(t)
		s.advance()
		return s.err
	}
	if !s.asc && t >= s.chunk.min {
		s.i = s.lastAtMost(t)
		s.advance()
		return s.err
	}
	s.seek(t)
	return s.err
}

func (s *iiChunkStream) Close() {
	if s.c != nil {
		s.c.Close()
		s.c = nil
	}
}
