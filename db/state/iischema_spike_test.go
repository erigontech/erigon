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
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/recsplit/multiencseq"
	"github.com/erigontech/erigon/db/seg"
	mdbxgo "github.com/erigontech/mdbx-go/mdbx"
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
	u16   bool // dup = BE8(max) || LE uint16 offsets from min, ascending, min implicit
	// width selects the offsets layout when set: 2 or 4 = fixed LE offsets from min;
	// -1 = per-chunk width 1/2/4 stored as a trailing byte (none for a one-txNum chunk).
	width      int
	widths     []int // width -1: candidate widths, default 1/2/4
	rangeSplit bool  // uvarint deltas, but split a chunk when max-min > 65535 (isolates the u16 split cost)
	vbuf       []uint64
	ebuf       []byte
}

func (s *dupMaxSchema) w() int {
	if s.u16 {
		return 2
	}
	return s.width
}

func (s *dupMaxSchema) name() string {
	switch {
	case s.u16:
		return "u16off"
	case s.width == -1 && s.widths != nil:
		return fmt.Sprintf("woff%v-%d", s.widths, s.limit)
	case s.width == -1:
		return fmt.Sprintf("woff-%d", s.limit)
	case s.width == -2:
		return fmt.Sprintf("h16-%d", s.limit)
	case s.width > 0:
		return fmt.Sprintf("u%doff-%d", 8*s.width, s.limit)
	case s.rangeSplit:
		return fmt.Sprintf("varint-rsplit-%d", s.limit)
	}
	return fmt.Sprintf("dupmax-varint-%d", s.limit)
}

func putOff(dst []byte, w int, v uint64) []byte {
	switch w {
	case 1:
		return append(dst, byte(v))
	case 2:
		return binary.LittleEndian.AppendUint16(dst, uint16(v))
	case 3:
		return append(dst, byte(v), byte(v>>8), byte(v>>16))
	}
	return binary.LittleEndian.AppendUint32(dst, uint32(v))
}

func getOff(b []byte, w int) uint64 {
	switch w {
	case 1:
		return uint64(b[0])
	case 2:
		return uint64(binary.LittleEndian.Uint16(b))
	case 3:
		return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16
	}
	return uint64(binary.LittleEndian.Uint32(b))
}

// chunkWidth is the width an adaptive chunk of vals uses.
func (s *dupMaxSchema) cands() []int {
	if s.widths != nil {
		return s.widths
	}
	return []int{1, 2, 4}
}

func (s *dupMaxSchema) chunkWidth(vals []uint64) int {
	r := vals[len(vals)-1] - vals[0]
	for _, w := range s.cands() {
		if r <= uint64(1)<<(8*w)-1 {
			return w
		}
	}
	panic("chunk range too wide")
}

func (s *dupMaxSchema) enc(dst []byte, vals []uint64) []byte {
	w := s.w()
	if w == 0 {
		return dupMaxEncode(dst, vals)
	}
	if w == -2 { // u16 offsets (even payload) when the range fits, else uvarint deltas padded to an odd payload
		if len(vals) == 1 || vals[len(vals)-1]-vals[0] <= math.MaxUint16 {
			w = 2
		} else {
			start := len(dst)
			dst = dupMaxEncode(dst, vals)
			if (len(dst)-start-8)%2 == 0 {
				dst = append(dst, 0)
			}
			return dst
		}
	}
	dst = binary.BigEndian.AppendUint64(dst, vals[len(vals)-1])
	if w == -1 {
		if len(vals) == 1 {
			return dst
		}
		w = s.chunkWidth(vals)
	}
	for _, v := range vals[1:] {
		dst = putOff(dst, w, v-vals[0])
	}
	if s.w() == -1 {
		dst = append(dst, byte(w))
	}
	return dst
}

func (s *dupMaxSchema) dec(dst []uint64, v []byte) []uint64 {
	w := s.w()
	if w == 0 {
		return dupMaxDecode(dst, v)
	}
	if w == -2 {
		if (len(v)-8)%2 == 1 {
			if v[len(v)-1] == 0 {
				v = v[:len(v)-1]
			}
			return dupMaxDecode(dst, v)
		}
		w = 2
	}
	max, offs := binary.BigEndian.Uint64(v), v[8:]
	if w == -1 && len(offs) > 0 {
		w, offs = int(offs[len(offs)-1]), offs[:len(offs)-1]
	}
	min := max
	if len(offs) > 0 {
		min -= getOff(offs[len(offs)-w:], w)
	}
	dst = append(dst, min)
	for ; len(offs) > 0; offs = offs[w:] {
		dst = append(dst, min+getOff(offs, w))
	}
	return dst
}
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
	switch w := s.w(); {
	case w == -1:
		return s.fitAdaptive(vals)
	case w == -2:
		return s.fitHybrid(vals)
	case w > 0:
		maxRange := uint64(1)<<(8*w) - 1
		n := 1
		for n < len(vals) && w*n <= s.limit && vals[n]-vals[0] <= maxRange {
			n++
		}
		return n
	}
	size := 0
	for i := 1; i < len(vals); i++ {
		if s.rangeSplit && vals[i]-vals[0] > math.MaxUint16 {
			return i
		}
		var tmp [binary.MaxVarintLen64]byte
		size += binary.PutUvarint(tmp[:], vals[i]-vals[i-1])
		if size > s.limit {
			return i
		}
	}
	return len(vals)
}

// fitHybrid takes a u16 chunk when the next gap fits, else a uvarint chunk over the run of gaps > 65535.
func (s *dupMaxSchema) fitHybrid(vals []uint64) int {
	n := 1
	if len(vals) > 1 && vals[1]-vals[0] <= math.MaxUint16 {
		for n < len(vals) && 2*n <= s.limit && vals[n]-vals[0] <= math.MaxUint16 {
			n++
		}
		return n
	}
	size := 0
	for n < len(vals) {
		if n+1 < len(vals) && vals[n+1]-vals[n] <= math.MaxUint16 && n > 1 {
			break // vals[n] starts the next dense run
		}
		var tmp [binary.MaxVarintLen64]byte
		size += binary.PutUvarint(tmp[:], vals[n]-vals[n-1])
		if size+1 > s.limit {
			break
		}
		n++
	}
	return n
}

// fitAdaptive picks, per chunk, the width 1/2/4 with the fewest bytes per txNum.
func (s *dupMaxSchema) fitAdaptive(vals []uint64) int {
	bestN, bestCost := 1, 9.0
	for _, w := range s.cands() {
		maxRange := uint64(1)<<(8*w) - 1
		n := 1
		for n < len(vals) && w*n <= s.limit && vals[n]-vals[0] <= maxRange {
			n++
		}
		cost := float64(8+1+w*(n-1)) / float64(n)
		if n == 1 {
			cost = 8
		}
		if cost < bestCost {
			bestN, bestCost = n, cost
		}
	}
	return bestN
}

// write stores vals as chunks; when replace, the cursor is on the dup that vals[0:] extends.
func (s *dupMaxSchema) write(c kv.RwCursorDupSort, key []byte, vals []uint64, replace bool) error {
	for len(vals) > 0 {
		n := s.fit(vals)
		s.ebuf = s.enc(s.ebuf[:0], vals[:n])
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
			s.vbuf = append(s.dec(s.vbuf[:0], last), vals[i]...)
			if err := s.write(c, k, s.vbuf, true); err != nil {
				return err
			}
			continue
		}
		old := s.dec(nil, v)
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
			old = s.dec(old, v)
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
			s.vbuf = s.dec(s.vbuf[:0], v)
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
		s.vbuf = s.dec(s.vbuf[:0], v)
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
			s.vbuf = s.dec(s.vbuf[:0], v)
			if i, _ := slices.BinarySearch(s.vbuf, txTo); i > 0 {
				if err := c.PutCurrent(key, s.enc(s.ebuf[:0], s.vbuf[i:])); err != nil {
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
				s.vbuf = s.dec(s.vbuf[:0], v)
				i, _ := slices.BinarySearch(s.vbuf, txFrom)
				keep = append(keep[:0], s.vbuf[:i]...)
				first = false
			}
			if err := c.DeleteCurrent(); err != nil {
				return err
			}
		}
		if len(keep) > 0 {
			if err := c.Put(key, s.enc(s.ebuf[:0], keep)); err != nil {
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
	pc := iiChunkPruneCursor{RwCursorDupSort: c, d: newIIChunkDeleter(c)}
	for _, k := range keys { // per-txNum deletes, as prune.HashSeekingPrune does
		s, err := newIIChunkStream(tx, spikeTbl, k, int(txFrom), -1, order.Asc, -1)
		if err != nil {
			return err
		}
		txNums, err := stream.ToArrayU64(s)
		s.Close()
		if err != nil {
			return err
		}
		for _, n := range txNums {
			if err := pc.DeleteExact(k, binary.BigEndian.AppendUint64(nil, n)); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---- arm: production writer with the prefetcher, as loadIndex runs it ----

var spikeDB kv.RoDB

type prodPfSchema struct{ prodSchema }

func (prodPfSchema) name() string { return "prodpf" }
func (prodPfSchema) flush(tx kv.RwTx, keys [][]byte, vals [][]uint64) error {
	c, err := tx.RwCursorDupSort(spikeTbl)
	if err != nil {
		return err
	}
	defer c.Close()
	ctx := context.Background()
	p := newInvertedIndexPrefetcher(spikeDB, spikeTbl, 6)
	if err := p.open(ctx); err != nil {
		return err
	}
	defer p.close()
	w := &iiChunkWriter{c: c}
	pairs := make([][2][]byte, 0, 65536)
	write := func() error {
		if err := p.prefetch(ctx, pairs); err != nil {
			return err
		}
		for _, pair := range pairs {
			if err := w.add(pair[0], pair[1]); err != nil {
				return err
			}
		}
		pairs = pairs[:0]
		return nil
	}
	for i, k := range keys {
		for _, v := range vals[i] {
			pairs = append(pairs, [2][]byte{k, binary.BigEndian.AppendUint64(nil, v)})
			if len(pairs) == 65536 {
				if err := write(); err != nil {
					return err
				}
			}
		}
	}
	if err := write(); err != nil {
		return err
	}
	return w.flush()
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
	prevTx := make([]uint64, len(sd.keys))
	var gaps, bigGaps int
	for _, p := range sd.pairs {
		if prevTx[p.k] != 0 {
			gaps++
			if p.tx-prevTx[p.k] > math.MaxUint16 {
				bigGaps++
			}
		}
		prevTx[p.k] = p.tx
	}
	fmt.Printf("gaps=%d gaps>=65536=%d (%.3f%%)\n", gaps, bigGaps, 100*float64(bigGaps)/float64(gaps))

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
		func() spikeSchema { return &dupMaxSchema{limit: 512, u16: true} },
		func() spikeSchema { return &dupMaxSchema{limit: 512, rangeSplit: true} },
		func() spikeSchema { return &dupMaxSchema{limit: 512, width: 4} },
		func() spikeSchema { return &dupMaxSchema{limit: 512, width: -1} },
		func() spikeSchema { return &dupMaxSchema{limit: 512, width: -2} },
		func() spikeSchema { return &dupMaxSchema{limit: 512, width: 3} },
		func() spikeSchema { return &dupMaxSchema{limit: 512, width: -1, widths: []int{2, 3}} },
		func() spikeSchema { return &dupMaxSchema{limit: 512, width: -1, widths: []int{1, 2, 3}} },
		func() spikeSchema { return prodSchema{} },
		func() spikeSchema { return prodPfSchema{} },
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
			WithTableCfg(func(kv.TableCfg) kv.TableCfg {
				return kv.TableCfg{spikeTbl: kv.TableCfgItem{Flags: s.flags()}, spikeTbl + "C": kv.TableCfgItem{Flags: s.flags()}}
			}).MustOpen()
		spikeDB = db
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
		dups, _ := ro.Count(spikeTbl)
		if os.Getenv("II_SPIKE_BREAKDOWN") != "" {
			spikeBreakdown(t, ro, s.name())
			spikeCompactSize(t, db, s.name())
		}
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
		fmt.Printf("%-22s %8.2f %8.3f %9s %9.1f %9.1f %9.1f %9.1f %9.1f %9.1f %8.2f %8.1f  dups=%d chk=%d/%d/%d/%d/%d/%d\n", s.name(),
			bulk.Seconds(), float64(tipDur.Microseconds())/1000/float64(nTip), datasize.ByteSize(dirty/nTip).HR(),
			float64(size)/1e6, us(hotTip), us(hotAll), us(hotDesc), us(cold), us(coldTip), pr.Seconds(), float64(unw.Microseconds())/1000,
			dups, n1, n2, n3, n4, n5, after)
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

func TestIISchemaSpikeKeysTable(t *testing.T) {
	path := os.Getenv("II_SPIKE_EF")
	if path == "" {
		t.Log("set II_SPIKE_EF to a .ef file")
		return
	}
	sd := spikeLoad(t, path, 1_562_500)
	db := mdbx.New(dbcfg.ChainDB, log.New()).InMem(t.TempDir()).MapSize(64 * datasize.GB).DirtySpace(uint64(256 * datasize.MB)).
		WithTableCfg(func(kv.TableCfg) kv.TableCfg { return kv.TableCfg{spikeTbl: kv.TableCfgItem{Flags: kv.DupSort}} }).MustOpen()
	defer db.Close()
	var b [8]byte
	for i := 0; i < len(sd.pairs); {
		tx, err := db.BeginRw(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		c, err := tx.RwCursorDupSort(spikeTbl)
		if err != nil {
			t.Fatal(err)
		}
		for j := 0; j < 200_000 && i < len(sd.pairs); i, j = i+1, j+1 {
			binary.BigEndian.PutUint64(b[:], sd.pairs[i].tx)
			if err := c.Put(b[:], sd.keys[sd.pairs[i].k]); err != nil {
				t.Fatal(err)
			}
		}
		c.Close()
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	tx, _ := db.BeginRo(t.Context())
	defer tx.Rollback()
	size, _ := tx.BucketSize(spikeTbl)
	txNums := map[uint64]struct{}{}
	for _, p := range sd.pairs {
		txNums[p.tx] = struct{}{}
	}
	fmt.Printf("%s keys table: %.1f MB, pairs=%d txNums=%d\n", filepath.Base(path), float64(size)/1e6, len(sd.pairs), len(txNums))
}

// spikeBreakdown prints where the bytes of the values table go.
func spikeBreakdown(t *testing.T, ro kv.Tx, name string) {
	st, err := ro.(interface {
		BucketStat(string) (*mdbxgo.Stat, error)
	}).BucketStat(spikeTbl)
	if err != nil {
		t.Fatal(err)
	}
	var keys, dups, keyBytes, valBytes, singleKeys, singleDups uint64
	var perKey [5]uint64 // keys by total value bytes: <=64, <=512, <=2K, <=8K, more
	var perKeyDups [5]uint64
	var prevKey []byte
	var cur, curDups uint64
	flushKey := func() {
		if prevKey == nil {
			return
		}
		i := 4
		switch {
		case cur <= 64:
			i = 0
		case cur <= 512:
			i = 1
		case cur <= 2048:
			i = 2
		case cur <= 8192:
			i = 3
		}
		perKey[i]++
		perKeyDups[i] += curDups
		if curDups == 1 {
			singleKeys++
		}
	}
	if err := ro.ForEach(spikeTbl, nil, func(k, v []byte) error {
		if !bytes.Equal(k, prevKey) {
			flushKey()
			prevKey = bytes.Clone(k)
			keys++
			keyBytes += uint64(len(k))
			cur, curDups = 0, 0
		}
		dups++
		valBytes += uint64(len(v))
		cur += uint64(len(v))
		curDups++
		if len(v) == 8 {
			singleDups++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	flushKey()
	pages := st.LeafPages + st.BranchPages + st.OverflowPages
	fmt.Printf("  %s: keys=%d dups=%d singleKeys=%d 8Bdups=%d keyMB=%.1f valMB=%.1f (prefix %.1f + payload %.1f) pages leaf=%d branch=%d ovf=%d depth=%d tableMB=%.1f fill=%.0f%%\n",
		name, keys, dups, singleKeys, singleDups, float64(keyBytes)/1e6, float64(valBytes)/1e6, float64(8*dups)/1e6, float64(valBytes-8*dups)/1e6,
		st.LeafPages, st.BranchPages, st.OverflowPages, st.Depth, float64(pages*4096)/1e6, 100*float64(keyBytes+valBytes)/float64(pages*4096))
	fmt.Printf("  %s: keys by value bytes <=64/<=512/<=2K/<=8K/more: %v dups %v\n", name, perKey, perKeyDups)
}

// spikeCompactSize copies the values table in order with Append/AppendDup and prints its size: the fill-factor floor.
func spikeCompactSize(t *testing.T, db kv.RwDB, name string) {
	tx, err := db.BeginRw(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	c, err := tx.RwCursorDupSort(spikeTbl + "C")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var prev []byte
	if err := tx.ForEach(spikeTbl, nil, func(k, v []byte) error {
		if bytes.Equal(k, prev) {
			return c.AppendDup(k, v)
		}
		prev = bytes.Clone(k)
		return c.Append(k, v)
	}); err != nil {
		t.Fatal(err)
	}
	size, _ := tx.BucketSize(spikeTbl + "C")
	fmt.Printf("  %s: compact copy MB=%.1f\n", name, float64(size)/1e6)
}
