// Copyright 2024 The Erigon Authors
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

package btindex

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"
	"unsafe"

	"github.com/c2h5oh/datasize"
	"github.com/edsrzf/mmap-go"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/murmur3"
	"github.com/erigontech/erigon/db/datastruct/existence"
	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
)

const BtreeLogPrefix = "btree"

// DefaultBtreeM - amount of keys on leaf of BTree
// It will do log2(M) co-located-reads from data file - for binary-search inside leaf
var DefaultBtreeM = uint64(dbg.EnvInt("BT_M", 256))

const DefaultBtreeStartSkip = uint64(4) // defines smallest shard available for scan instead of binsearch

const (
	// btFirstByteLegacy (0x00) is the released legacy layout's first byte (the EF count's MSB).
	btFirstByteLegacy = byte(0x00)
	// btFirstByteUseFooter is the (non-zero) first byte of footer-based files; it lets the reader fall
	// back to the legacy layout from the first byte. The format version lives in the footer, not here.
	btFirstByteUseFooter = byte(0x01)
)

// BtInterp enables interpolation search in the leaf window, falling back to binary after BtInterpBudget probes.
var BtInterp = dbg.EnvBool("BT_INTERP", true)
var BtInterpBudget = uint64(dbg.EnvInt("BT_INTERP_BUDGET", 8))

var ErrBtIndexLookupBounds = errors.New("BtIndex: lookup di bounds error")

type Cursor struct {
	ef         *eliasfano32.EliasFano
	returnInto *sync.Pool
	getter     *seg.Reader
	key        []byte
	value      []byte
	d          uint64
}

func (c *Cursor) Close() {
	if c == nil {
		return
	}
	c.key = c.key[:0]
	c.value = c.value[:0]
	c.d = 0
	c.getter = nil
	if c.returnInto != nil {
		c.returnInto.Put(c)
	}
}

// getter should be alive all the time of cursor usage
// Key and value is valid until cursor.Next is called
func (c *Cursor) Key() []byte {
	return c.key
}

func (c *Cursor) Di() uint64 {
	return c.d
}

func (c *Cursor) Value() []byte {
	return c.value
}

func (c *Cursor) nextNoRead() bool {
	if c.d+1 >= c.ef.Count() {
		return false
	}

	c.d++

	offset := c.ef.Get(c.d)
	c.getter.Reset(offset)

	return true
}

func (c *Cursor) Next() bool { // could return error instead
	if !c.next() {
		// c.Close()
		return false
	}

	if err := c.readKV(); err != nil {
		fmt.Printf("nextKV error %v\n", err)
		return false
	}
	return true
}

// next returns if another key/value pair is available int that index.
// moves pointer d to next element if successful
func (c *Cursor) next() bool {
	if c.d+1 == c.ef.Count() {
		return false
	}
	c.d++
	return true
}

func (c *Cursor) resetNoRead(di uint64, g *seg.Reader) error {
	if c.d >= c.ef.Count() {
		return fmt.Errorf("%w %d/%d", ErrBtIndexLookupBounds, c.d, c.ef.Count())
	}

	c.d = di
	c.getter = g

	offset := c.ef.Get(c.d)
	c.getter.Reset(offset)

	return nil
}

func (c *Cursor) Reset(di uint64, g *seg.Reader) error {
	c.d = di
	c.getter = g
	return c.readKV()
}

func (c *Cursor) readKV() error {
	if c.d >= c.ef.Count() {
		return fmt.Errorf("%w %d/%d", ErrBtIndexLookupBounds, c.d, c.ef.Count())
	}
	if c.getter == nil {
		return errors.New("getter is nil")
	}

	offset := c.ef.Get(c.d)
	c.getter.Reset(offset)
	if !c.getter.HasNext() {
		return fmt.Errorf("pair %d/%d key not found, file: %s/%s", c.d, c.ef.Count(), c.getter.FileName(), c.getter.FileName())
	}
	c.key, _ = c.getter.Next(nil)
	if !c.getter.HasNext() {
		return fmt.Errorf("pair %d/%d val not found, file: %s/%s", c.d, c.ef.Count(), c.getter.FileName(), c.getter.FileName())
	}
	c.value, _ = c.getter.Next(nil) // if value is not compressed, we getting ptr to slice from mmap, may need to copy
	return nil
}

type BtIndexWriter struct {
	indexF    *os.File
	writer    *countingWriter
	ef        *eliasfano32.EliasFano
	efBuilder *eliasfano32.OffHeapBuilder

	args BtIndexWriterArgs

	indexFileName string

	nodeHeaderBuf [2]byte

	built   bool
	lvl     log.Lvl
	logger  log.Logger
	noFsync bool // fsync is enabled by default, but tests can manually disable
}

type BtIndexWriterArgs struct {
	IndexFile string
	TmpDir    string
	M         uint64
	KeyCount  uint64
	MaxOffset uint64 // must be >= the largest offset passed to AddKey; an over-estimate (e.g. data file size) is fine
	Lvl       log.Lvl
}

// NewBtIndexWriter creates a new BtIndexWriter instance with given number of keys
// Typical bucket size is 100 - 2048, larger bucket sizes result in smaller representations of hash functions, at a cost of slower access
// salt parameters is used to randomise the hash function construction, to ensure that different Erigon instances (nodes)
// are likely to use different hash function, to collision attacks are unlikely to slow down any meaningful number of nodes at the same time
func NewBtIndexWriter(args BtIndexWriterArgs, logger log.Logger) (_ *BtIndexWriter, err error) {
	if args.Lvl == 0 {
		args.Lvl = log.LvlTrace
	}

	btw := &BtIndexWriter{lvl: args.Lvl, logger: logger, args: args}
	defer func() {
		if err != nil {
			btw.Close()
		}
	}()

	_, fname := filepath.Split(btw.args.IndexFile)
	btw.indexFileName = fname

	if btw.indexF, err = dir.CreateTemp(args.IndexFile); err != nil {
		return nil, fmt.Errorf("create temp index file for %s: %w", args.IndexFile, err)
	}
	btw.writer = &countingWriter{w: getBufioWriter(btw.indexF)}

	if args.KeyCount > 0 {
		if args.M == 0 {
			return nil, fmt.Errorf("[index] %s: M must be > 0", btw.indexFileName)
		}
		if btw.efBuilder, err = eliasfano32.NewEliasFanoOffHeap(args.KeyCount, args.MaxOffset, args.TmpDir); err != nil {
			return nil, fmt.Errorf("[index] create offheap ef: %w", err)
		}
		btw.ef = btw.efBuilder.EliasFano

		if _, err = btw.writer.Write([]byte{btFirstByteUseFooter}); err != nil {
			return nil, fmt.Errorf("[index] write format byte: %w", err)
		}
	}

	return btw, nil
}

func (btw *BtIndexWriter) AddKey(key []byte, offset uint64) error {
	if btw.built {
		return errors.New("cannot add keys after perfect hash function had been built")
	}
	if btw.ef == nil {
		return fmt.Errorf("[index] %s: AddKey called with KeyCount==0", btw.indexFileName)
	}
	di := btw.ef.AddedCount()
	if di >= btw.args.KeyCount {
		return fmt.Errorf("[index] %s: AddKey beyond KeyCount=%d", btw.indexFileName, btw.args.KeyCount)
	}
	if offset > btw.args.MaxOffset { // EF is pre-sized for [0, MaxOffset]; a larger offset would write out of bounds
		return fmt.Errorf("[index] %s: offset %d exceeds MaxOffset %d", btw.indexFileName, offset, btw.args.MaxOffset)
	}
	btw.ef.AddOffset(offset)

	// every M-th key (di==0 included) is kept as a B-tree node
	if di%btw.args.M != 0 {
		return nil
	}
	if err := (Node{key: key}).Encode(btw.writer, btw.nodeHeaderBuf[:]); err != nil {
		return err
	}
	return nil
}

// Build has to be called after all the keys have been added, and it initiates the process
// of building the perfect hash function and writing index into a file
func (btw *BtIndexWriter) Build() error {
	if btw.built {
		return errors.New("already built")
	}
	defer btw.closeTemps()
	if btw.args.KeyCount > 0 && btw.ef.AddedCount() != btw.args.KeyCount {
		return fmt.Errorf("[index] %s: KeyCount=%d but %d keys added", btw.indexFileName, btw.args.KeyCount, btw.ef.AddedCount())
	}

	log.Log(btw.args.Lvl, "[index] calculating", "file", btw.indexFileName)

	var err error
	if btw.args.KeyCount > 0 {
		// nodes were streamed straight into the index file during AddKey
		if err = btw.writer.padTo(btEFAlign); err != nil {
			return fmt.Errorf("[index] pad before ef: %w", err)
		}
		efOffset := btw.writer.written

		btw.ef.Build()
		if err = btw.ef.Write(btw.writer); err != nil {
			return fmt.Errorf("[index] write ef: %w", err)
		}
		if err = btw.writer.padTo(btFooterAlign); err != nil {
			return fmt.Errorf("[index] pad before footer: %w", err)
		}

		footer := Footer{Meta: Metadata{KeysCount: btw.args.KeyCount, M: btw.args.M, EfOffset: efOffset}, FormatVersion: btVersion}
		if err = footer.Encode(btw.writer); err != nil {
			return fmt.Errorf("[index] write footer: %w", err)
		}
	}

	btw.logger.Log(btw.args.Lvl, "[index] write", "file", btw.indexFileName)
	btw.built = true

	if err = btw.writer.Flush(); err != nil {
		return err
	}
	if err = btw.fsync(); err != nil {
		return err
	}
	tmpName := btw.indexF.Name()
	if err = btw.indexF.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmpName, btw.args.IndexFile); err != nil {
		return err
	}
	btw.indexF = nil
	return nil
}

func (btw *BtIndexWriter) DisableFsync() { btw.noFsync = true }

// fsync - other processes/goroutines must see only "fully-complete" (valid) files. No partial-writes.
// To achieve it: write to .tmp file then `rename` when file is ready.
// Machine may power-off right after `rename` - it means `fsync` must be before `rename`
func (btw *BtIndexWriter) fsync() error {
	if btw.noFsync {
		return nil
	}
	if err := btw.indexF.Sync(); err != nil {
		btw.logger.Warn("couldn't fsync", "err", err, "file", btw.indexF.Name())
		return err
	}
	return nil
}

func (btw *BtIndexWriter) closeTemps() {
	if btw.efBuilder != nil {
		btw.efBuilder.Close()
		btw.efBuilder = nil
		btw.ef = nil
	}
}

func (btw *BtIndexWriter) Close() {
	if btw.writer != nil {
		putBufioWriter(btw.writer.w)
		btw.writer = nil
	}
	if btw.indexF != nil { // non-nil means Build didn't rename it: drop the partial .tmp
		name := btw.indexF.Name()
		btw.indexF.Close()
		_ = dir.RemoveFile(name)
		btw.indexF = nil
	}
	btw.closeTemps()
}

type BtIndex struct {
	m        mmap.MMap
	data     []byte
	ef       *eliasfano32.EliasFano
	file     *os.File
	bplus    *BpsTree
	size     int64
	modTime  time.Time
	filePath string
	indexM   uint64
	pool     sync.Pool
}

// Decompressor should be managed by caller (could be closed after index is built). When index is built, external getter should be passed to seekInFiles function
func CreateBtreeIndexWithDecompressor(indexPath string, existenceFilterPath string, M uint64, decompressor *seg.Reader, seed uint32, ps *background.ProgressSet, tmpdir string, logger log.Logger, noFsync bool, accessors statecfg.Accessors) (*BtIndex, error) {
	err := BuildBtreeIndexWithDecompressor(indexPath, existenceFilterPath, decompressor, ps, tmpdir, seed, logger, noFsync, accessors)
	if err != nil {
		return nil, err
	}
	return OpenBtreeIndexWithDecompressor(indexPath, M, decompressor)
}

// OpenBtreeIndexAndDataFile opens btree index file and data file and returns it along with BtIndex instance
// Mostly useful for testing
func OpenBtreeIndexAndDataFile(indexPath, dataPath string, M uint64, compressed seg.FileCompression, trace bool) (_ *seg.Decompressor, _ *BtIndex, err error) {
	d, err := seg.NewDecompressor(dataPath)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if err != nil {
			d.Close()
		}
	}()
	kv := seg.NewReader(d.MakeGetter(), compressed)
	bt, err := OpenBtreeIndexWithDecompressor(indexPath, M, kv)
	if err != nil {
		return nil, nil, err
	}
	return d, bt, nil
}

func BuildBtreeIndexWithDecompressor(indexPath string, existenceFilterPath string, kv *seg.Reader, ps *background.ProgressSet, tmpdir string, salt uint32, logger log.Logger, noFsync bool, accessors statecfg.Accessors) error {
	_, indexFileName := filepath.Split(indexPath)
	p := ps.AddNew(indexFileName, uint64(kv.Count()/2))
	defer ps.Delete(p)

	defer kv.MadvNormal().DisableReadAhead()

	var existenceFilter *existence.Filter
	if accessors.Has(statecfg.AccessorExistence) {
		var err error
		existenceFilter, err = existence.NewFilter(uint64(kv.Count()/2), existenceFilterPath)
		if err != nil {
			return err
		}
		if noFsync {
			existenceFilter.DisableFsync()
		}
	}

	args := BtIndexWriterArgs{
		IndexFile: indexPath,
		TmpDir:    tmpdir,
		M:         DefaultBtreeM,
		KeyCount:  uint64(kv.Count() / 2),
		MaxOffset: uint64(kv.Size()),
	}

	iw, err := NewBtIndexWriter(args, logger)
	if err != nil {
		return err
	}
	defer iw.Close()

	kv.Reset(0)

	key := make([]byte, 0, 64)
	var pos uint64

	for kv.HasNext() {
		key, _ = kv.Next(key[:0])
		if err = iw.AddKey(key, pos); err != nil {
			return err
		}
		hi, _ := murmur3.Sum128WithSeed(key, salt)
		if existenceFilter != nil {
			if err := existenceFilter.AddHash(hi); err != nil {
				return err
			}
		}
		pos, _ = kv.Skip()

		p.Processed.Add(1)
	}
	//logger.Warn("empty keys", "key lengths", ks, "total emptys", emptys, "total", kv.Count()/2)
	if err := iw.Build(); err != nil {
		return err
	}

	if existenceFilter != nil {
		if err := existenceFilter.Build(); err != nil {
			return err
		}
	}
	return nil
}

func OpenBtreeIndexWithDecompressor(indexPath string, M uint64, kvGetter *seg.Reader) (bt *BtIndex, err error) {
	idx := &BtIndex{
		filePath: indexPath,
		indexM:   M,
	}

	var validationPassed bool
	defer func() {
		// recover from panic if one occurred. Set err to nil if no panic
		if r := recover(); r != nil {
			// do r with only the stack trace
			err = fmt.Errorf("incomplete or not-fully downloaded file %s", indexPath)
		}
		if err != nil || !validationPassed {
			idx.Close()
			idx = nil
		}
	}()

	s, err := os.Stat(indexPath)
	if err != nil {
		return nil, err
	}
	idx.size = s.Size()
	idx.modTime = s.ModTime()

	idx.file, err = os.Open(indexPath)
	if err != nil {
		return nil, err
	}
	if idx.size == 0 {
		return idx, nil
	}

	idx.m, err = mmap.MapRegion(idx.file, int(idx.size), mmap.RDONLY, 0, 0)
	if err != nil {
		return nil, err
	}
	idx.data = idx.m[:idx.size]

	var nodeOfftEF *eliasfano32.EliasFano
	var keysBlob []byte
	var nodeStride uint64
	switch idx.data[0] {
	case btFirstByteLegacy: // legacy [EF][nodesCount][di-nodes]
		var pos int
		idx.ef, pos = eliasfano32.ReadEliasFano(idx.data)
		if len(idx.data[pos:]) > 0 {
			keysBlob = idx.data[pos:]
			if nodeOfftEF, nodeStride, _, err = decodeListNodesV0(keysBlob); err != nil {
				return nil, err
			}
			if nodeStride == 0 { // <2 nodes: only di=0 exists, stride is irrelevant
				nodeStride = M
			}
		}
	case btFirstByteUseFooter: // footer-based layout: [leadingByte][nodes][EF][footer][anchor]
		var footer Footer
		var footerStart int
		if footer, footerStart, err = ReadFooter(idx.data); err != nil {
			if errors.Is(err, errNotFooterFormat) {
				return nil, fmt.Errorf("btindex: %s: truncated or corrupt footer (missing trailing magic)", indexPath)
			}
			return nil, fmt.Errorf("btindex: %s: %w", indexPath, err)
		}
		if footer.FormatVersion != btVersion {
			return nil, fmt.Errorf("btindex: %s: unsupported format version %d (want %d): upgrade Erigon", indexPath, footer.FormatVersion, btVersion)
		}
		M = footer.Meta.M
		if M == 0 || footer.Meta.EfOffset >= uint64(footerStart) {
			return nil, fmt.Errorf("btindex: corrupt footer in %s (M=%d ef_offset=%d body=%d)", indexPath, M, footer.Meta.EfOffset, footerStart)
		}
		// ceil(K/M) overflow-safe: (K+M-1)/M overflows when K≈MaxUint64; use (K-1)/M+1 for K>0.
		var nodesCount uint64
		if footer.Meta.KeysCount > 0 {
			nodesCount = (footer.Meta.KeysCount-1)/M + 1
		}
		keysBlob = idx.data[1:]
		nodeStride = M
		var nodesEnd int
		if nodeOfftEF, nodesEnd, err = decodeNodes(keysBlob, nodesCount); err != nil {
			return nil, err
		}
		if footer.Meta.EfOffset != uint64(alignUp(1+nodesEnd, btEFAlign)) { // cross-check ef_offset against the decoded nodes
			return nil, fmt.Errorf("btindex: corrupt footer in %s: ef_offset=%d but nodes end at %d", indexPath, footer.Meta.EfOffset, alignUp(1+nodesEnd, btEFAlign))
		}
		idx.ef, _ = eliasfano32.ReadEliasFano(idx.data[footer.Meta.EfOffset:])
		if idx.ef.Count() != footer.Meta.KeysCount {
			return nil, fmt.Errorf("btindex: corrupt file %s: ef has %d keys, footer says %d", indexPath, idx.ef.Count(), footer.Meta.KeysCount)
		}
	default:
		return nil, fmt.Errorf("btindex: %s: unknown format byte %#x", indexPath, idx.data[0])
	}

	idx.indexM = M
	idx.pool.New = func() any {
		return &Cursor{ef: idx.ef, returnInto: &idx.pool}
	}

	defer kvGetter.MadvNormal().DisableReadAhead()

	if nodeOfftEF == nil {
		idx.bplus = NewBpsTree(kvGetter, idx.ef, M, idx.dataLookup)
	} else {
		idx.bplus = NewBpsTreeWithNodes(kvGetter, idx.ef, M, idx.dataLookup, keysBlob, nodeOfftEF, nodeStride)
	}
	idx.bplus.cursorGetter = idx.newCursor

	validationPassed = true
	dbg.ArmGCLeakCheck(idx.FileName(), idx)
	return idx, nil
}

// dataLookup fetches key and value from data file by di (data index)
// di starts from 0 so di is never >= keyCount
func (b *BtIndex) dataLookup(di uint64, g *seg.Reader) (k, v []byte, offset uint64, err error) {
	if di >= b.ef.Count() {
		return nil, nil, 0, fmt.Errorf("%w: keyCount=%d, but key %d requested. file: %s", ErrBtIndexLookupBounds, b.ef.Count(), di, b.FileName())
	}

	offset = b.ef.Get(di)
	g.Reset(offset)
	if !g.HasNext() {
		return nil, nil, 0, fmt.Errorf("pair %d/%d key not found, file: %s/%s", di, b.ef.Count(), b.FileName(), g.FileName())
	}

	k, _ = g.Next(nil)
	if !g.HasNext() {
		return nil, nil, 0, fmt.Errorf("pair %d/%d value not found, file: %s/%s", di, b.ef.Count(), b.FileName(), g.FileName())
	}
	v, _ = g.Next(nil)
	return k, v, offset, nil
}

// getter should be alive all the time of cursor usage
// Key and value is valid until cursor.Next is called
func (b *BtIndex) newCursor(k, v []byte, d uint64, g *seg.Reader) *Cursor {
	c := b.pool.Get().(*Cursor)
	c.ef = b.ef
	c.returnInto = &b.pool

	c.d, c.getter = d, g
	c.key = append(c.key[:0], k...)
	c.value = append(c.value[:0], v...)
	return c
}

func (b *BtIndex) DataHandle() unsafe.Pointer {
	return unsafe.Pointer(&b.data[0])
}

func (b *BtIndex) Size() int64 { return b.size }

func (b *BtIndex) ModTime() time.Time { return b.modTime }

func (b *BtIndex) FilePath() string { return b.filePath }

func (b *BtIndex) FileName() string { return path.Base(b.filePath) }

func (b *BtIndex) M() uint64 { return b.indexM }

func (b *BtIndex) Empty() bool { return b == nil || b.ef == nil || b.ef.Count() == 0 }

func (b *BtIndex) KeyCount() uint64 {
	if b.Empty() {
		return 0
	}
	return b.ef.Count()
}

func (b *BtIndex) Close() {
	if b == nil {
		return
	}
	dbg.DisarmGCLeakCheck(b)
	if b.m != nil {
		if err := b.m.Unmap(); err != nil {
			log.Log(dbg.FileCloseLogLevel, "unmap", "err", err, "file", b.FileName(), "stack", dbg.Stack())
		}
		b.m = nil
	}
	if b.file != nil {
		if err := b.file.Close(); err != nil {
			log.Log(dbg.FileCloseLogLevel, "close", "err", err, "file", b.FileName(), "stack", dbg.Stack())
		}
		b.file = nil
	}
	if b.bplus != nil {
		b.bplus.Close()
		b.bplus = nil
	}
}

// Get - exact match of key. `k == nil` - means not found
func (b *BtIndex) Get(lookup []byte, gr *seg.Reader) (k, v []byte, offsetInFile uint64, found bool, err error) {
	// TODO: optimize by "push-down" - instead of using seek+compare, alloc can have method Get which will return nil if key doesn't exists
	// alternativaly: can allocate cursor on-stack
	// 	it := Iter{} // allocation on stack
	//  it.Initialize(file)

	if b.Empty() {
		return k, v, 0, false, nil
	}

	// defer func() {
	// 	fmt.Printf("[Bindex][%s] Get (%t) '%x' -> '%x' di=%d err %v\n", b.FileName(), found, lookup, v, index, err)
	// }()
	if b.bplus == nil {
		panic(fmt.Errorf("Get: `b.bplus` is nil: %s", gr.FileName()))
	}
	// weak assumption that k will be ignored and used lookup instead.
	// since fetching k and v from data file is required to use Getter.
	// Why to do Getter.Reset twice when we can get kv right there.
	v, found, offsetInFile, err = b.bplus.Get(gr, lookup)
	if err != nil {
		if errors.Is(err, ErrBtIndexLookupBounds) {
			return k, v, offsetInFile, false, nil
		}
		return lookup, v, offsetInFile, false, err
	}
	return lookup, v, offsetInFile, found, nil
}

// Seek moves cursor to position where key >= x.
// Then if x == nil - first key returned
//
//	if x is larger than any other key in index, nil cursor is returned.
//
// Caller should close cursor after use.
func (b *BtIndex) Seek(g *seg.Reader, x []byte) (*Cursor, error) {
	if b.Empty() {
		return nil, nil
	}
	c, err := b.bplus.Seek(g, x)
	if err != nil || c == nil {
		if errors.Is(err, ErrBtIndexLookupBounds) {
			return nil, nil
		}
		return nil, err
	}
	return c, nil
}

// OrdinalLookup returns cursor for key at position i
func (b *BtIndex) OrdinalLookup(getter *seg.Reader, i uint64) *Cursor {
	k, v, _, err := b.dataLookup(i, getter)
	if err != nil {
		return nil
	}
	return b.newCursor(k, v, i, getter)
}

func (b *BtIndex) Offsets() *eliasfano32.EliasFano { return b.bplus.Offsets() }
func (b *BtIndex) Distances() (map[int]int, error) { return b.bplus.Distances() }

// Erigon doesn't create tons of bufio readers/writers, but it has tons of
// parallel small unit-tests which each create many small files and bufio
// readers/writers — pooling avoids the allocation pressure in that scenario.
var bufioWriterPool = sync.Pool{New: func() any { return bufio.NewWriterSize(nil, int(512*datasize.KB)) }}

func getBufioWriter(w io.Writer) *bufio.Writer {
	bw := bufioWriterPool.Get().(*bufio.Writer)
	bw.Reset(w)
	return bw
}

// Reset(nil) before Put is required: without it the pool entry retains a
// reference to the underlying io.Writer/io.Reader, keeping it alive until the
// next GC cycle or until the entry is reused — whichever comes first.
func putBufioWriter(w *bufio.Writer) { w.Reset(nil); bufioWriterPool.Put(w) }
