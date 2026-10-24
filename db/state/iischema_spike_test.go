package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/recsplit/multiencseq"
	"github.com/erigontech/erigon/db/seg"
)

type spikePair struct {
	tx uint64
	k  uint32
}

type spikeData struct {
	keys  [][]byte
	pairs []spikePair // sorted by tx, k
	freq  []int
}

func spikeLoad(t *testing.T, path string, stepSize uint64) *spikeData {
	base := filepath.Base(path)
	parts := strings.Split(strings.TrimSuffix(base, ".ef"), ".")
	fromStep, err := strconv.ParseUint(strings.Split(parts[len(parts)-1], "-")[0], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	baseTxNum := fromStep * stepSize
	d, err := seg.NewDecompressor(path)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	g := seg.NewReader(d.MakeGetter(), seg.CompressNone)
	sd := &spikeData{}
	var seq multiencseq.SequenceReader
	for g.HasNext() {
		k, _ := g.Next(nil)
		v, _ := g.Next(nil)
		seq.Reset(baseTxNum, v)
		ki := uint32(len(sd.keys))
		sd.keys = append(sd.keys, bytes.Clone(k))
		it := seq.Iterator(0)
		n := 0
		for it.HasNext() {
			tx, _ := it.Next()
			sd.pairs = append(sd.pairs, spikePair{tx, ki})
			n++
		}
		sd.freq = append(sd.freq, n)
	}
	slices.SortFunc(sd.pairs, func(a, b spikePair) int {
		if a.tx != b.tx {
			if a.tx < b.tx {
				return -1
			}
			return 1
		}
		return int(a.k) - int(b.k)
	})
	return sd
}

// spikeSchema is one candidate layout of the II values table.
type spikeSchema interface {
	name() string
	flags() kv.TableFlags
	// flush gets one ETL-ordered batch: key groups sorted by key, txNums ascending.
	flush(tx kv.RwTx, keys [][]byte, vals [][]uint64) error
	rangeCount(tx kv.Tx, key []byte, from, to uint64, asc bool, limit int) (int, error)
	prune(tx kv.RwTx, txTo uint64) error
	unwind(tx kv.RwTx, keys [][]byte, txFrom uint64) error
}

const spikeTbl = "IIVals"

// ---- arm: current DupSort key -> txNum ----

type dupSchema struct{}

func (dupSchema) name() string         { return "dup" }
func (dupSchema) flags() kv.TableFlags { return kv.DupSort }
func (dupSchema) flush(tx kv.RwTx, keys [][]byte, vals [][]uint64) error {
	c, err := tx.RwCursorDupSort(spikeTbl)
	if err != nil {
		return err
	}
	defer c.Close()
	var b [8]byte
	for i, k := range keys {
		for _, v := range vals[i] {
			binary.BigEndian.PutUint64(b[:], v)
			if err := c.Put(k, b[:]); err != nil {
				return err
			}
		}
	}
	return nil
}
func (dupSchema) rangeCount(tx kv.Tx, key []byte, from, to uint64, asc bool, limit int) (int, error) {
	var f, tt [8]byte
	binary.BigEndian.PutUint64(f[:], from)
	binary.BigEndian.PutUint64(tt[:], to)
	it, err := tx.RangeDupSort(spikeTbl, key, f[:], tt[:], order.By(asc), limit)
	if err != nil {
		return 0, err
	}
	defer it.Close()
	n := 0
	for it.HasNext() {
		if _, _, err := it.Next(); err != nil {
			return 0, err
		}
		n++
	}
	return n, nil
}
func (dupSchema) prune(tx kv.RwTx, txTo uint64) error {
	c, err := tx.RwCursorDupSort(spikeTbl)
	if err != nil {
		return err
	}
	defer c.Close()
	for k, v, err := c.First(); k != nil; k, v, err = c.NextNoDup() {
		if err != nil {
			return err
		}
		if binary.BigEndian.Uint64(v) >= txTo {
			continue
		}
		last, err := c.LastDup()
		if err != nil {
			return err
		}
		if binary.BigEndian.Uint64(last) < txTo {
			if err := c.DeleteCurrentDuplicates(); err != nil {
				return err
			}
			continue
		}
		key := bytes.Clone(k)
		for {
			if v, err = c.SeekBothRange(key, make([]byte, 8)); err != nil {
				return err
			}
			if v == nil || binary.BigEndian.Uint64(v) >= txTo {
				break
			}
			if err := c.DeleteCurrent(); err != nil {
				return err
			}
		}
	}
	return nil
}
func (dupSchema) unwind(tx kv.RwTx, keys [][]byte, txFrom uint64) error {
	c, err := tx.RwCursorDupSort(spikeTbl)
	if err != nil {
		return err
	}
	defer c.Close()
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], txFrom)
	for _, k := range keys {
		v, err := c.SeekBothRange(k, b[:])
		if err != nil {
			return err
		}
		for v != nil {
			if err := c.DeleteCurrent(); err != nil {
				return err
			}
			if v, err = c.SeekBothRange(k, b[:]); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---- arm: chunks key||BE8(maxTxNum) -> encoded txNums, open chunk key||FF*8 ----

type chunkCodec interface {
	name() string
	encode(dst []byte, vals []uint64) []byte
	decode(dst []uint64, b []byte) []uint64
}

type chunkSchema struct {
	codec chunkCodec
	limit int // max encoded value size
	vbuf  []uint64
	ebuf  []byte
	kbuf  []byte
}

var openSuffix = [8]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

func (s *chunkSchema) name() string         { return fmt.Sprintf("chunk-%s-%d", s.codec.name(), s.limit) }
func (s *chunkSchema) flags() kv.TableFlags { return kv.Default }

func (s *chunkSchema) chunkKey(key []byte, max uint64, open bool) []byte {
	s.kbuf = append(s.kbuf[:0], key...)
	if open {
		return append(s.kbuf, openSuffix[:]...)
	}
	return binary.BigEndian.AppendUint64(s.kbuf, max)
}

// writeChunks splits sorted vals into closed chunks of at most limit bytes and one open chunk.
func (s *chunkSchema) writeChunks(c kv.RwCursor, key []byte, vals []uint64) error {
	for len(vals) > 0 {
		s.ebuf = s.codec.encode(s.ebuf[:0], vals)
		if len(s.ebuf) <= s.limit {
			return c.Put(s.chunkKey(key, 0, true), s.ebuf)
		}
		lo, hi := 1, len(vals) // largest prefix that fits
		for lo < hi {
			mid := (lo + hi + 1) / 2
			if len(s.codec.encode(s.ebuf[:0], vals[:mid])) <= s.limit {
				lo = mid
			} else {
				hi = mid - 1
			}
		}
		s.ebuf = s.codec.encode(s.ebuf[:0], vals[:lo])
		if err := c.Put(s.chunkKey(key, vals[lo-1], false), s.ebuf); err != nil {
			return err
		}
		vals = vals[lo:]
	}
	return nil
}

func (s *chunkSchema) flush(tx kv.RwTx, keys [][]byte, vals [][]uint64) error {
	c, err := tx.RwCursor(spikeTbl)
	if err != nil {
		return err
	}
	defer c.Close()
	for i, k := range keys {
		ck := s.chunkKey(k, 0, true)
		_, v, err := c.SeekExact(ck)
		if err != nil {
			return err
		}
		merged := vals[i]
		if v != nil {
			s.vbuf = s.codec.decode(s.vbuf[:0], v)
			if s.vbuf[len(s.vbuf)-1] < merged[0] {
				merged = append(s.vbuf, merged...)
			} else {
				merged = mergeSortedUniq(s.vbuf, merged)
			}
			s.vbuf = merged
		}
		if err := s.writeChunks(c, k, merged); err != nil {
			return err
		}
	}
	return nil
}

func mergeSortedUniq(a, b []uint64) []uint64 {
	out := make([]uint64, 0, len(a)+len(b))
	out = append(append(out, a...), b...)
	slices.Sort(out)
	return slices.Compact(out)
}

func (s *chunkSchema) rangeCount(tx kv.Tx, key []byte, from, to uint64, asc bool, limit int) (int, error) {
	c, err := tx.Cursor(spikeTbl)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	sk := s.chunkKey(key, from, false)
	k, v, err := c.Seek(sk)
	if err != nil {
		return 0, err
	}
	n := 0
	if asc { // [from, to)
		for ; k != nil && len(k) == len(key)+8 && bytes.HasPrefix(k, key); k, v, err = c.Next() {
			if err != nil {
				return 0, err
			}
			s.vbuf = s.codec.decode(s.vbuf[:0], v)
			i, _ := slices.BinarySearch(s.vbuf, from)
			for _, x := range s.vbuf[i:] {
				if x >= to || n == limit {
					return n, nil
				}
				n++
			}
		}
		return n, nil
	}
	// desc: (to, from]
	if k == nil {
		k, v, err = c.Last()
	} else if len(k) != len(key)+8 || !bytes.HasPrefix(k, key) {
		k, v, err = c.Prev()
		if err != nil {
			return 0, err
		}
	}
	for ; k != nil && len(k) == len(key)+8 && bytes.HasPrefix(k, key); k, v, err = c.Prev() {
		if err != nil {
			return 0, err
		}
		s.vbuf = s.codec.decode(s.vbuf[:0], v)
		i, found := slices.BinarySearch(s.vbuf, from)
		if found {
			i++
		}
		for j := i - 1; j >= 0; j-- {
			if s.vbuf[j] <= to || n == limit {
				return n, nil
			}
			n++
		}
	}
	return n, nil
}

func (s *chunkSchema) prune(tx kv.RwTx, txTo uint64) error {
	c, err := tx.RwCursor(spikeTbl)
	if err != nil {
		return err
	}
	defer c.Close()
	var next []byte
	for k, v, err := c.First(); k != nil; {
		if err != nil {
			return err
		}
		k = bytes.Clone(k)
		key := k[:len(k)-8]
		open := bytes.Equal(k[len(k)-8:], openSuffix[:])
		if !open && binary.BigEndian.Uint64(k[len(k)-8:]) < txTo {
			if err := c.DeleteCurrent(); err != nil {
				return err
			}
			k, v, err = c.Seek(k)
			continue
		}
		s.vbuf = s.codec.decode(s.vbuf[:0], v)
		if s.vbuf[0] < txTo {
			i, _ := slices.BinarySearch(s.vbuf, txTo)
			if i == len(s.vbuf) {
				if err := c.DeleteCurrent(); err != nil {
					return err
				}
			} else {
				s.ebuf = s.codec.encode(s.ebuf[:0], s.vbuf[i:])
				if err := c.Put(k, s.ebuf); err != nil {
					return err
				}
			}
		}
		// later chunks of this key hold only txNums >= txTo
		next = append(append(next[:0], key...), openSuffix[:]...)
		if k, v, err = c.Seek(next); err != nil {
			return err
		}
		if k != nil && bytes.Equal(k, next) {
			k, v, err = c.Next()
		}
	}
	return nil
}

func (s *chunkSchema) unwind(tx kv.RwTx, keys [][]byte, txFrom uint64) error {
	c, err := tx.RwCursor(spikeTbl)
	if err != nil {
		return err
	}
	defer c.Close()
	for _, key := range keys {
		k, v, err := c.Seek(s.chunkKey(key, txFrom, false))
		if err != nil {
			return err
		}
		var keep []uint64
		first := true
		for ; k != nil && len(k) == len(key)+8 && bytes.HasPrefix(k, key); k, v, err = c.Seek(k) {
			if err != nil {
				return err
			}
			k = bytes.Clone(k)
			if first {
				s.vbuf = s.codec.decode(s.vbuf[:0], v)
				i, _ := slices.BinarySearch(s.vbuf, txFrom)
				keep = append(keep[:0], s.vbuf[:i]...)
				first = false
			}
			if err := c.DeleteCurrent(); err != nil {
				return err
			}
		}
		if len(keep) > 0 {
			s.ebuf = s.codec.encode(s.ebuf[:0], keep)
			if err := c.Put(s.chunkKey(key, 0, true), s.ebuf); err != nil {
				return err
			}
		}
	}
	return nil
}

type roaringCodec struct{}

func (roaringCodec) name() string { return "roaring64" }
func (roaringCodec) encode(dst []byte, vals []uint64) []byte {
	bm := roaring64.New()
	bm.AddMany(vals)
	bm.RunOptimize()
	b, _ := bm.ToBytes()
	return append(dst, b...)
}
func (roaringCodec) decode(dst []uint64, b []byte) []uint64 {
	bm := roaring64.New()
	if _, err := bm.FromUnsafeBytes(b); err != nil {
		panic(err)
	}
	it := bm.Iterator()
	for it.HasNext() {
		dst = append(dst, it.Next())
	}
	return dst
}

type varintCodec struct{}

func (varintCodec) name() string { return "varint" }
func (varintCodec) encode(dst []byte, vals []uint64) []byte {
	prev := uint64(0)
	for _, v := range vals {
		dst = binary.AppendUvarint(dst, v-prev)
		prev = v
	}
	return dst
}
func (varintCodec) decode(dst []uint64, b []byte) []uint64 {
	prev := uint64(0)
	for len(b) > 0 {
		d, n := binary.Uvarint(b)
		b = b[n:]
		prev += d
		dst = append(dst, prev)
	}
	return dst
}

// ---- arm: DupSort key -> dup{BE8(max)||varint}, open chunk dup FF*8||varint ----

type dupChunkSchema struct {
	limit int
	vbuf  []uint64
	ebuf  []byte
}

func (s *dupChunkSchema) name() string         { return fmt.Sprintf("dupchunk-varint-%d", s.limit) }
func (s *dupChunkSchema) flags() kv.TableFlags { return kv.DupSort }

func (s *dupChunkSchema) val(max uint64, open bool, vals []uint64) []byte {
	if open {
		s.ebuf = append(s.ebuf[:0], openSuffix[:]...)
	} else {
		s.ebuf = binary.BigEndian.AppendUint64(s.ebuf[:0], max)
	}
	return varintCodec{}.encode(s.ebuf, vals)
}

func (s *dupChunkSchema) write(c kv.RwCursorDupSort, key []byte, vals []uint64, replaceOpen bool) error {
	for len(vals) > 0 {
		if v := s.val(0, true, vals); len(v)-8 <= s.limit {
			if replaceOpen {
				return c.PutCurrent(key, v)
			}
			return c.Put(key, v)
		}
		lo, hi := 1, len(vals)
		for lo < hi {
			mid := (lo + hi + 1) / 2
			if len(varintCodec{}.encode(s.ebuf[:0], vals[:mid])) <= s.limit {
				lo = mid
			} else {
				hi = mid - 1
			}
		}
		if replaceOpen { // the open chunk is split: its sort position changes
			if err := c.DeleteCurrent(); err != nil {
				return err
			}
			replaceOpen = false
		}
		if err := c.Put(key, s.val(vals[lo-1], false, vals[:lo])); err != nil {
			return err
		}
		vals = vals[lo:]
	}
	return nil
}

func (s *dupChunkSchema) flush(tx kv.RwTx, keys [][]byte, vals [][]uint64) error {
	c, err := tx.RwCursorDupSort(spikeTbl)
	if err != nil {
		return err
	}
	defer c.Close()
	var sk [8]byte
	for i, k := range keys {
		binary.BigEndian.PutUint64(sk[:], vals[i][0])
		v, err := c.SeekBothRange(k, sk[:])
		if err != nil {
			return err
		}
		if v == nil {
			if err := s.write(c, k, vals[i], false); err != nil {
				return err
			}
			continue
		}
		s.vbuf = varintCodec{}.decode(s.vbuf[:0], v[8:])
		if bytes.Equal(v[:8], openSuffix[:]) && s.vbuf[len(s.vbuf)-1] < vals[i][0] { // common case: append to the open chunk
			s.vbuf = append(s.vbuf, vals[i]...)
			if err := s.write(c, k, s.vbuf, true); err != nil {
				return err
			}
			continue
		}
		old := slices.Clone(s.vbuf)
		if err := c.DeleteCurrent(); err != nil {
			return err
		}
		for {
			v, err := c.SeekBothRange(k, sk[:])
			if err != nil {
				return err
			}
			if v == nil {
				break
			}
			old = varintCodec{}.decode(old, v[8:])
			if err := c.DeleteCurrent(); err != nil {
				return err
			}
		}
		if err := s.write(c, k, mergeSortedUniq(old, vals[i]), false); err != nil {
			return err
		}
	}
	return nil
}

func (s *dupChunkSchema) rangeCount(tx kv.Tx, key []byte, from, to uint64, asc bool, limit int) (int, error) {
	c, err := tx.CursorDupSort(spikeTbl)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	var sk [8]byte
	binary.BigEndian.PutUint64(sk[:], from)
	v, err := c.SeekBothRange(key, sk[:])
	if err != nil {
		return 0, err
	}
	n := 0
	if asc {
		for ; v != nil; _, v, err = c.NextDup() {
			if err != nil {
				return 0, err
			}
			s.vbuf = varintCodec{}.decode(s.vbuf[:0], v[8:])
			i, _ := slices.BinarySearch(s.vbuf, from)
			for _, x := range s.vbuf[i:] {
				if x >= to || n == limit {
					return n, nil
				}
				n++
			}
		}
		return n, nil
	}
	if v == nil {
		k, _, err := c.SeekExact(key)
		if err != nil || k == nil {
			return 0, err
		}
		if v, err = c.LastDup(); err != nil {
			return 0, err
		}
	}
	for ; v != nil; _, v, err = c.PrevDup() {
		if err != nil {
			return 0, err
		}
		s.vbuf = varintCodec{}.decode(s.vbuf[:0], v[8:])
		i, found := slices.BinarySearch(s.vbuf, from)
		if found {
			i++
		}
		for j := i - 1; j >= 0; j-- {
			if s.vbuf[j] <= to || n == limit {
				return n, nil
			}
			n++
		}
	}
	return n, nil
}

func (s *dupChunkSchema) prune(tx kv.RwTx, txTo uint64) error {
	c, err := tx.RwCursorDupSort(spikeTbl)
	if err != nil {
		return err
	}
	defer c.Close()
	var zero [8]byte
	for k, _, err := c.First(); k != nil; {
		if err != nil {
			return err
		}
		key := bytes.Clone(k)
		for {
			v, err := c.SeekBothRange(key, zero[:])
			if err != nil {
				return err
			}
			if v == nil {
				break
			}
			if !bytes.Equal(v[:8], openSuffix[:]) && binary.BigEndian.Uint64(v[:8]) < txTo {
				if err := c.DeleteCurrent(); err != nil {
					return err
				}
				continue
			}
			s.vbuf = varintCodec{}.decode(s.vbuf[:0], v[8:])
			i, _ := slices.BinarySearch(s.vbuf, txTo)
			if i == len(s.vbuf) {
				if err := c.DeleteCurrent(); err != nil {
					return err
				}
				continue
			}
			if i > 0 {
				prefix := bytes.Clone(v[:8])
				s.ebuf = varintCodec{}.encode(append(s.ebuf[:0], prefix...), s.vbuf[i:])
				if err := c.PutCurrent(key, s.ebuf); err != nil {
					return err
				}
			}
			break
		}
		if k, _, err = c.Seek(key); err != nil {
			return err
		}
		if k != nil && bytes.Equal(k, key) {
			k, _, err = c.NextNoDup()
		}
	}
	return nil
}

func (s *dupChunkSchema) unwind(tx kv.RwTx, keys [][]byte, txFrom uint64) error {
	c, err := tx.RwCursorDupSort(spikeTbl)
	if err != nil {
		return err
	}
	defer c.Close()
	var sk [8]byte
	binary.BigEndian.PutUint64(sk[:], txFrom)
	for _, key := range keys {
		var keep []uint64
		first := true
		for {
			v, err := c.SeekBothRange(key, sk[:])
			if err != nil {
				return err
			}
			if v == nil {
				break
			}
			if first {
				s.vbuf = varintCodec{}.decode(s.vbuf[:0], v[8:])
				i, _ := slices.BinarySearch(s.vbuf, txFrom)
				keep = append(keep[:0], s.vbuf[:i]...)
				first = false
			}
			if err := c.DeleteCurrent(); err != nil {
				return err
			}
		}
		if len(keep) > 0 {
			if err := c.Put(key, s.val(0, true, keep)); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---- arm: DupSort key -> dup{BE8(max)||uvarint gaps below max, descending}; 8-byte dup = one txNum ----

type dupMaxSchema struct {
	limit int
	vbuf  []uint64
	ebuf  []byte
}

func (s *dupMaxSchema) name() string         { return fmt.Sprintf("dupmax-varint-%d", s.limit) }
func (s *dupMaxSchema) flags() kv.TableFlags { return kv.DupSort }

func dupMaxEncode(dst []byte, vals []uint64) []byte {
	max := vals[len(vals)-1]
	dst = binary.BigEndian.AppendUint64(dst, max)
	prev := max
	for i := len(vals) - 2; i >= 0; i-- {
		dst = binary.AppendUvarint(dst, prev-vals[i])
		prev = vals[i]
	}
	return dst
}

func dupMaxDecode(dst []uint64, v []byte) []uint64 {
	start := len(dst)
	prev := binary.BigEndian.Uint64(v)
	dst = append(dst, prev)
	for b := v[8:]; len(b) > 0; {
		d, n := binary.Uvarint(b)
		b = b[n:]
		prev -= d
		dst = append(dst, prev)
	}
	slices.Reverse(dst[start:])
	return dst
}

// fit returns the longest prefix of vals that encodes into the limit.
func (s *dupMaxSchema) fit(vals []uint64) int {
	size := 0
	for i := 1; i < len(vals); i++ {
		var tmp [binary.MaxVarintLen64]byte
		size += binary.PutUvarint(tmp[:], vals[i]-vals[i-1])
		if size > s.limit {
			return i
		}
	}
	return len(vals)
}

// write stores vals as chunks; when replace, the cursor is on the dup that vals[0:] extends.
func (s *dupMaxSchema) write(c kv.RwCursorDupSort, key []byte, vals []uint64, replace bool) error {
	for len(vals) > 0 {
		n := s.fit(vals)
		s.ebuf = dupMaxEncode(s.ebuf[:0], vals[:n])
		if replace {
			if err := c.PutCurrent(key, s.ebuf); err != nil {
				return err
			}
			replace = false
		} else if err := c.Put(key, s.ebuf); err != nil {
			return err
		}
		vals = vals[n:]
	}
	return nil
}

func (s *dupMaxSchema) flush(tx kv.RwTx, keys [][]byte, vals [][]uint64) error {
	c, err := tx.RwCursorDupSort(spikeTbl)
	if err != nil {
		return err
	}
	defer c.Close()
	var sk [8]byte
	for i, k := range keys {
		binary.BigEndian.PutUint64(sk[:], vals[i][0])
		v, err := c.SeekBothRange(k, sk[:])
		if err != nil {
			return err
		}
		if v == nil { // all new txNums are above the last chunk: extend it
			kk, _, err := c.SeekExact(k)
			if err != nil {
				return err
			}
			if kk == nil {
				if err := s.write(c, k, vals[i], false); err != nil {
					return err
				}
				continue
			}
			last, err := c.LastDup()
			if err != nil {
				return err
			}
			s.vbuf = append(dupMaxDecode(s.vbuf[:0], last), vals[i]...)
			if err := s.write(c, k, s.vbuf, true); err != nil {
				return err
			}
			continue
		}
		old := dupMaxDecode(nil, v)
		if err := c.DeleteCurrent(); err != nil {
			return err
		}
		for {
			v, err := c.SeekBothRange(k, sk[:])
			if err != nil {
				return err
			}
			if v == nil {
				break
			}
			old = dupMaxDecode(old, v)
			if err := c.DeleteCurrent(); err != nil {
				return err
			}
		}
		if err := s.write(c, k, mergeSortedUniq(old, vals[i]), false); err != nil {
			return err
		}
	}
	return nil
}

func (s *dupMaxSchema) rangeCount(tx kv.Tx, key []byte, from, to uint64, asc bool, limit int) (int, error) {
	c, err := tx.CursorDupSort(spikeTbl)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	var sk [8]byte
	binary.BigEndian.PutUint64(sk[:], from)
	v, err := c.SeekBothRange(key, sk[:])
	if err != nil {
		return 0, err
	}
	n := 0
	if asc {
		for ; v != nil; _, v, err = c.NextDup() {
			if err != nil {
				return 0, err
			}
			s.vbuf = dupMaxDecode(s.vbuf[:0], v)
			i, _ := slices.BinarySearch(s.vbuf, from)
			for _, x := range s.vbuf[i:] {
				if x >= to || n == limit {
					return n, nil
				}
				n++
			}
		}
		return n, nil
	}
	if v == nil {
		k, _, err := c.SeekExact(key)
		if err != nil || k == nil {
			return 0, err
		}
		if v, err = c.LastDup(); err != nil {
			return 0, err
		}
	}
	for ; v != nil; _, v, err = c.PrevDup() {
		if err != nil {
			return 0, err
		}
		s.vbuf = dupMaxDecode(s.vbuf[:0], v)
		i, found := slices.BinarySearch(s.vbuf, from)
		if found {
			i++
		}
		for j := i - 1; j >= 0; j-- {
			if s.vbuf[j] <= to || n == limit {
				return n, nil
			}
			n++
		}
	}
	return n, nil
}

func (s *dupMaxSchema) prune(tx kv.RwTx, txTo uint64) error {
	c, err := tx.RwCursorDupSort(spikeTbl)
	if err != nil {
		return err
	}
	defer c.Close()
	var zero [8]byte
	for k, _, err := c.First(); k != nil; {
		if err != nil {
			return err
		}
		key := bytes.Clone(k)
		for {
			v, err := c.SeekBothRange(key, zero[:])
			if err != nil {
				return err
			}
			if v == nil {
				break
			}
			if binary.BigEndian.Uint64(v[:8]) < txTo {
				if err := c.DeleteCurrent(); err != nil {
					return err
				}
				continue
			}
			s.vbuf = dupMaxDecode(s.vbuf[:0], v)
			if i, _ := slices.BinarySearch(s.vbuf, txTo); i > 0 {
				if err := c.PutCurrent(key, dupMaxEncode(s.ebuf[:0], s.vbuf[i:])); err != nil {
					return err
				}
			}
			break
		}
		if k, _, err = c.Seek(key); err != nil {
			return err
		}
		if k != nil && bytes.Equal(k, key) {
			k, _, err = c.NextNoDup()
		}
	}
	return nil
}

func (s *dupMaxSchema) unwind(tx kv.RwTx, keys [][]byte, txFrom uint64) error {
	c, err := tx.RwCursorDupSort(spikeTbl)
	if err != nil {
		return err
	}
	defer c.Close()
	var sk [8]byte
	binary.BigEndian.PutUint64(sk[:], txFrom)
	for _, key := range keys {
		var keep []uint64
		first := true
		for {
			v, err := c.SeekBothRange(key, sk[:])
			if err != nil {
				return err
			}
			if v == nil {
				break
			}
			if first {
				s.vbuf = dupMaxDecode(s.vbuf[:0], v)
				i, _ := slices.BinarySearch(s.vbuf, txFrom)
				keep = append(keep[:0], s.vbuf[:i]...)
				first = false
			}
			if err := c.DeleteCurrent(); err != nil {
				return err
			}
		}
		if len(keep) > 0 {
			if err := c.Put(key, dupMaxEncode(s.ebuf[:0], keep)); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---- arm: production code ----

type prodSchema struct{}

func (prodSchema) name() string         { return "prod" }
func (prodSchema) flags() kv.TableFlags { return kv.DupSort }
func (prodSchema) flush(tx kv.RwTx, keys [][]byte, vals [][]uint64) error {
	c, err := tx.RwCursorDupSort(spikeTbl)
	if err != nil {
		return err
	}
	defer c.Close()
	w := &iiChunkWriter{c: c}
	var b [8]byte
	for i, k := range keys {
		for _, v := range vals[i] {
			binary.BigEndian.PutUint64(b[:], v)
			if err := w.add(k, b[:]); err != nil {
				return err
			}
		}
	}
	return w.flush()
}
func (prodSchema) rangeCount(tx kv.Tx, key []byte, from, to uint64, asc bool, limit int) (int, error) {
	s, err := newIIChunkStream(tx, spikeTbl, key, int(min(from, math.MaxInt64)), int(min(to, math.MaxInt64)), order.By(asc), limit)
	if err != nil {
		return 0, err
	}
	defer s.Close()
	n := 0
	for s.HasNext() {
		if _, err := s.Next(); err != nil {
			return 0, err
		}
		n++
	}
	return n, nil
}
func (prodSchema) prune(tx kv.RwTx, txTo uint64) error {
	c, err := tx.RwCursorDupSort(spikeTbl)
	if err != nil {
		return err
	}
	defer c.Close()
	iit := &InvertedIndexRoTx{ii: &InvertedIndex{logger: log.New()}}
	stat := &prune.Stat{ValueProgress: prune.First}
	return iit.pruneChunks(context.Background(), c, 0, txTo, time.NewTicker(time.Hour), stat)
}
func (prodSchema) unwind(tx kv.RwTx, keys [][]byte, txFrom uint64) error {
	c, err := tx.RwCursorDupSort(spikeTbl)
	if err != nil {
		return err
	}
	defer c.Close()
	d := newIIChunkDeleter(c)
	for _, k := range keys {
		if err := d.deleteRange(k, txFrom, math.MaxUint64); err != nil {
			return err
		}
	}
	return nil
}

// ---- driver ----

func spikeBatches(sd *spikeData, pairs []spikePair) ([][]byte, [][]uint64) {
	byKey := map[uint32][]uint64{}
	for _, p := range pairs {
		byKey[p.k] = append(byKey[p.k], p.tx)
	}
	ks := make([]uint32, 0, len(byKey))
	for k := range byKey {
		ks = append(ks, k)
	}
	slices.SortFunc(ks, func(a, b uint32) int { return bytes.Compare(sd.keys[a], sd.keys[b]) })
	keys := make([][]byte, len(ks))
	vals := make([][]uint64, len(ks))
	for i, k := range ks {
		keys[i], vals[i] = sd.keys[k], byKey[k]
	}
	return keys, vals
}

func TestIISchemaSpike(t *testing.T) {
	path := os.Getenv("II_SPIKE_EF")
	if path == "" {
		t.Log("set II_SPIKE_EF to a .ef file")
		return
	}
	const stepSize = 1_562_500
	sd := spikeLoad(t, path, stepSize)
	first, last := sd.pairs[0].tx, sd.pairs[len(sd.pairs)-1].tx+1
	t.Logf("%s: keys=%d pairs=%d tx=[%d,%d)", filepath.Base(path), len(sd.keys), len(sd.pairs), first, last)

	bulkBatch := uint64(200_000)
	tipBatch := uint64(250)
	tipTxs := uint64(dbgEnvU("II_SPIKE_TIP_TXS", 100_000))
	tipFrom := last - tipTxs

	// query sets: hot keys by frequency, random keys uniformly
	rnd := rand.New(rand.NewPCG(1, 2))
	var hotQ, coldQ [][]byte
	for range 2000 {
		hotQ = append(hotQ, sd.keys[sd.pairs[rnd.IntN(len(sd.pairs))].k])
		coldQ = append(coldQ, sd.keys[rnd.IntN(len(sd.keys))])
	}
	unwindFrom := last - 4*tipBatch
	unwindKeys := map[uint32]struct{}{}
	for i := len(sd.pairs) - 1; i >= 0 && sd.pairs[i].tx >= unwindFrom; i-- {
		unwindKeys[sd.pairs[i].k] = struct{}{}
	}
	var uk [][]byte
	for k := range unwindKeys {
		uk = append(uk, sd.keys[k])
	}
	slices.SortFunc(uk, bytes.Compare)

	arms := []func() spikeSchema{
		func() spikeSchema { return dupSchema{} },
		func() spikeSchema { return &chunkSchema{codec: roaringCodec{}, limit: 1900} },
		func() spikeSchema { return &chunkSchema{codec: varintCodec{}, limit: 1900} },
		func() spikeSchema { return &chunkSchema{codec: varintCodec{}, limit: 512} },
		func() spikeSchema { return &chunkSchema{codec: varintCodec{}, limit: 256} },
		func() spikeSchema { return &dupChunkSchema{limit: 512} },
		func() spikeSchema { return &dupMaxSchema{limit: 512} },
		func() spikeSchema { return prodSchema{} },
		func() spikeSchema { return &chunkSchema{codec: varintCodec{}, limit: 1024} },
	}
	if f := os.Getenv("II_SPIKE_ARMS"); f != "" {
		var sel []func() spikeSchema
		for _, part := range strings.Split(f, ",") {
			for _, a := range arms {
				if a().name() == part {
					sel = append(sel, a)
				}
			}
		}
		arms = sel
	}
	fmt.Printf("%-22s %8s %8s %9s %9s %9s %9s %9s %9s %9s %8s %8s\n", "arm", "bulk_s", "tip_ms", "tip_dirty", "size_MB",
		"hotTip_us", "hotAll_us", "hotDesc_us", "cold_us", "coldTip_us", "prune_s", "unw_ms")
	for _, mk := range arms {
		s := mk()
		dir := t.TempDir()
		db := mdbx.New(dbcfg.ChainDB, log.New()).InMem(dir).MapSize(64 * datasize.GB).DirtySpace(uint64(256 * datasize.MB)).
			WithTableCfg(func(kv.TableCfg) kv.TableCfg { return kv.TableCfg{spikeTbl: kv.TableCfgItem{Flags: s.flags()}} }).MustOpen()
		ctx := t.Context()

		// bulk: [first, tipFrom) in 200k-txNum flushes
		start := time.Now()
		i := 0
		for i < len(sd.pairs) && sd.pairs[i].tx < tipFrom {
			end := min(sd.pairs[i].tx-sd.pairs[i].tx%bulkBatch+bulkBatch, tipFrom)
			j := i
			for j < len(sd.pairs) && sd.pairs[j].tx < end {
				j++
			}
			keys, vals := spikeBatches(sd, sd.pairs[i:j])
			tx, err := db.BeginRw(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.flush(tx, keys, vals); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			i = j
		}
		bulk := time.Since(start)

		// tip: one flush+commit per 250 txNums
		var tipDur time.Duration
		var dirty uint64
		for i < len(sd.pairs) {
			end := sd.pairs[i].tx - sd.pairs[i].tx%tipBatch + tipBatch
			j := i
			for j < len(sd.pairs) && sd.pairs[j].tx < end {
				j++
			}
			keys, vals := spikeBatches(sd, sd.pairs[i:j])
			st := time.Now()
			tx, err := db.BeginRw(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.flush(tx, keys, vals); err != nil {
				t.Fatal(err)
			}
			if sp, ok := tx.(interface {
				SpaceDirty() (uint64, uint64, error)
			}); ok {
				d, _, _ := sp.SpaceDirty()
				dirty += d
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			tipDur += time.Since(st)
			i = j
		}
		nTip := (last - tipFrom + tipBatch - 1) / tipBatch

		ro, err := db.BeginRo(ctx)
		if err != nil {
			t.Fatal(err)
		}
		size, _ := ro.BucketSize(spikeTbl)
		q := func(keys [][]byte, from, to uint64, asc bool, limit int) (time.Duration, int) {
			st := time.Now()
			total := 0
			for _, k := range keys {
				n, err := s.rangeCount(ro, k, from, to, asc, limit)
				if err != nil {
					t.Fatal(err)
				}
				total += n
			}
			return time.Since(st) / time.Duration(len(keys)), total
		}
		recent := last - 25_000
		hotTip, n1 := q(hotQ, recent, last, true, -1)
		hotAll, n2 := q(hotQ, first, last, true, -1)
		hotDesc, n3 := q(hotQ, last, first, false, 10)
		cold, n4 := q(coldQ, first, last, true, -1)
		coldTip, n5 := q(coldQ, recent, last, true, -1)
		ro.Rollback()

		// unwind last 4 blocks, then prune the first step
		tx, err := db.BeginRw(ctx)
		if err != nil {
			t.Fatal(err)
		}
		st := time.Now()
		if err := s.unwind(tx, uk, unwindFrom); err != nil {
			t.Fatal(err)
		}
		unw := time.Since(st)
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		tx, err = db.BeginRw(ctx)
		if err != nil {
			t.Fatal(err)
		}
		pruneTo := first + (last-first)/2
		st = time.Now()
		if err := s.prune(tx, pruneTo); err != nil {
			t.Fatal(err)
		}
		pr := time.Since(st)
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		// correctness: every arm must return the same counts
		ro, _ = db.BeginRo(ctx)
		_, after := 0, 0
		for _, k := range hotQ[:200] {
			n, err := s.rangeCount(ro, k, 0, ^uint64(0), true, -1)
			if err != nil {
				t.Fatal(err)
			}
			after += n
		}
		ro.Rollback()
		db.Close()
		hot200 := map[string]int{}
		for _, k := range hotQ[:200] {
			hot200[string(k)]++
		}
		want := 0
		for _, p := range sd.pairs {
			if p.tx >= pruneTo && p.tx < unwindFrom {
				want += hot200[string(sd.keys[p.k])]
			}
		}
		if want != after {
			t.Fatalf("%s: after prune+unwind got %d want %d", s.name(), after, want)
		}
		fmt.Printf("%-22s %8.2f %8.3f %9s %9.1f %9.1f %9.1f %9.1f %9.1f %9.1f %8.2f %8.1f  chk=%d/%d/%d/%d/%d/%d\n", s.name(),
			bulk.Seconds(), float64(tipDur.Microseconds())/1000/float64(nTip), datasize.ByteSize(dirty/nTip).HR(),
			float64(size)/1e6, us(hotTip), us(hotAll), us(hotDesc), us(cold), us(coldTip), pr.Seconds(), float64(unw.Microseconds())/1000,
			n1, n2, n3, n4, n5, after)
	}
}

func us(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1000 }

func dbgEnvU(name string, def uint64) uint64 {
	if v := os.Getenv(name); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err == nil {
			return n
		}
	}
	return def
}
