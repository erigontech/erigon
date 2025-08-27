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

package state

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/c2h5oh/datasize"
	"github.com/edsrzf/mmap-go"
	"github.com/spaolacci/murmur3"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datastruct/existence"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
)

const BtreeLogPrefix = "btree"

// DefaultBtreeM - amount of keys on leaf of BTree
// It will do log2(M) co-located-reads from data file - for binary-search inside leaf
var DefaultBtreeM = uint64(dbg.EnvInt("BT_M", 256))

const DefaultBtreeStartSkip = uint64(4) // defines smallest shard available for scan instead of binsearch

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
	maxOffset  uint64
	prevOffset uint64
	minDelta   uint64
	indexW     *bufio.Writer
	indexF     *os.File
	ef         *eliasfano32.EliasFano
	collector  *etl.Collector

	args BtIndexWriterArgs

	indexFileName string
	tmpFilePath   string

	numBuf      [8]byte
	keysWritten uint64

	built   bool
	lvl     log.Lvl
	logger  log.Logger
	noFsync bool // fsync is enabled by default, but tests can manually disable
}

type BtIndexWriterArgs struct {
	IndexFile   string // File name where the index and the minimal perfect hash function will be written to
	TmpDir      string
	M           uint64
	KeyCount    int
	EtlBufLimit datasize.ByteSize
	Lvl         log.Lvl
}

// NewBtIndexWriter creates a new BtIndexWriter instance with given number of keys
// Typical bucket size is 100 - 2048, larger bucket sizes result in smaller representations of hash functions, at a cost of slower access
// salt parameters is used to randomise the hash function construction, to ensure that different Erigon instances (nodes)
// are likely to use different hash function, to collision attacks are unlikely to slow down any meaningful number of nodes at the same time
func NewBtIndexWriter(args BtIndexWriterArgs, logger log.Logger) (*BtIndexWriter, error) {
	if args.EtlBufLimit == 0 {
		args.EtlBufLimit = etl.BufferOptimalSize / 2
	}
	if args.Lvl == 0 {
		args.Lvl = log.LvlTrace
	}

	btw := &BtIndexWriter{lvl: args.Lvl, logger: logger, args: args,
		tmpFilePath: args.IndexFile + ".tmp"}

	_, fname := filepath.Split(btw.args.IndexFile)
	btw.indexFileName = fname

	btw.collector = etl.NewCollectorWithAllocator(BtreeLogPrefix+" "+fname, btw.args.TmpDir, etl.LargeSortableBuffers, logger)
	btw.collector.SortAndFlushInBackground(true)
	btw.collector.LogLvl(btw.args.Lvl)

	return btw, nil
}

func (btw *BtIndexWriter) AddKey(key []byte, offset uint64, keep bool) error {
	if btw.built {
		return errors.New("cannot add keys after perfect hash function had been built")
	}

	binary.BigEndian.PutUint64(btw.numBuf[:], offset)
	if offset > btw.maxOffset {
		btw.maxOffset = offset
	}

	keepKey := keep
	if btw.keysWritten > 0 {
		delta := offset - btw.prevOffset
		if btw.keysWritten == 1 || delta < btw.minDelta {
			btw.minDelta = delta
		}
		keepKey = btw.keysWritten%btw.args.M == 0
	}

	var k []byte
	if keepKey {
		k = key
	}

	if err := btw.collector.Collect(btw.numBuf[:], k); err != nil {
		return err
	}
	btw.keysWritten++
	btw.prevOffset = offset
	return nil
}

// Build has to be called after all the keys have been added, and it initiates the process
// of building the perfect hash function and writing index into a file
func (btw *BtIndexWriter) Build() error {
	if btw.built {
		return errors.New("already built")
	}
	var err error
	if btw.indexF, err = os.Create(btw.tmpFilePath); err != nil {
		return fmt.Errorf("create index file %s: %w", btw.args.IndexFile, err)
	}
	defer btw.indexF.Close()
	btw.indexW = bufio.NewWriterSize(btw.indexF, etl.BufIOSize)

	defer btw.collector.Close()
	log.Log(btw.args.Lvl, "[index] calculating", "file", btw.indexFileName)

	if btw.keysWritten > 0 {
		btw.ef = eliasfano32.NewEliasFano(btw.keysWritten, btw.maxOffset)

		nodes := make([]Node, 0, btw.keysWritten/btw.args.M)
		var ki uint64
		if err = btw.collector.Load(nil, "", func(offt, k []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
			btw.ef.AddOffset(binary.BigEndian.Uint64(offt))

			if len(k) > 0 { // for every M-th key, keep the key
				nodes = append(nodes, Node{key: common.Copy(k), di: ki})
			}
			ki++ // we need to keep key ordinal so count every key
			return nil
		}, etl.TransformArgs{}); err != nil {
			return err
		}
		btw.ef.Build()

		if err := btw.ef.Write(btw.indexW); err != nil {
			return fmt.Errorf("[index] write ef: %w", err)
		}
		if err = encodeListNodes(nodes, btw.indexW); err != nil {
			return fmt.Errorf("[index] write nodes: %w", err)
		}
	}

	btw.logger.Log(btw.args.Lvl, "[index] write", "file", btw.indexFileName)
	btw.built = true

	if err = btw.indexW.Flush(); err != nil {
		return err
	}
	if err = btw.fsync(); err != nil {
		return err
	}
	if err = btw.indexF.Close(); err != nil {
		return err
	}
	if err = os.Rename(btw.tmpFilePath, btw.args.IndexFile); err != nil {
		return err
	}
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
		btw.logger.Warn("couldn't fsync", "err", err, "file", btw.tmpFilePath)
		return err
	}
	return nil
}

func (btw *BtIndexWriter) Close() {
	if btw.indexF != nil {
		btw.indexF.Close()
	}
	if btw.collector != nil {
		btw.collector.Close()
	}
	//if btw.offsetCollector != nil {
	//	btw.offsetCollector.Close()
	//}
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
	pool     sync.Pool
}

// Decompressor should be managed by caller (could be closed after index is built). When index is built, external getter should be passed to seekInFiles function
func CreateBtreeIndexWithDecompressor(indexPath string, M uint64, decompressor *seg.Reader, seed uint32, ps *background.ProgressSet, tmpdir string, logger log.Logger, noFsync bool, accessors statecfg.Accessors) (*BtIndex, error) {
	err := BuildBtreeIndexWithDecompressor(indexPath, decompressor, ps, tmpdir, seed, logger, noFsync, accessors)
	if err != nil {
		return nil, err
	}
	return OpenBtreeIndexWithDecompressor(indexPath, M, decompressor)
}

// OpenBtreeIndexAndDataFile opens btree index file and data file and returns it along with BtIndex instance
// Mostly useful for testing
func OpenBtreeIndexAndDataFile(indexPath, dataPath string, M uint64, compressed seg.FileCompression, trace bool) (*seg.Decompressor, *BtIndex, error) {
	d, err := seg.NewDecompressor(dataPath)
	if err != nil {
		return nil, nil, err
	}
	kv := seg.NewReader(d.MakeGetter(), compressed)
	bt, err := OpenBtreeIndexWithDecompressor(indexPath, M, kv)
	if err != nil {
		d.Close()
		return nil, nil, err
	}
	return d, bt, nil
}

func BuildBtreeIndexWithDecompressor(indexPath string, kv *seg.Reader, ps *background.ProgressSet, tmpdir string, salt uint32, logger log.Logger, noFsync bool, accessors statecfg.Accessors) error {
	_, indexFileName := filepath.Split(indexPath)
	p := ps.AddNew(indexFileName, uint64(kv.Count()/2))
	defer ps.Delete(p)

	defer kv.MadvNormal().DisableReadAhead()
	existenceFilterPath := strings.TrimSuffix(indexPath, ".bt") + ".kvei"

	var existenceFilter *existence.Filter
	if accessors.Has(statecfg.AccessorExistence) {
		var err error
		useFuse := false
		existenceFilter, err = existence.NewFilter(uint64(kv.Count()/2), existenceFilterPath, useFuse)
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
	}

	iw, err := NewBtIndexWriter(args, logger)
	if err != nil {
		return err
	}
	defer iw.Close()

	kv.Reset(0)

	key := make([]byte, 0, 64)
	var pos uint64

	var b0 [256]bool
	for kv.HasNext() {
		key, _ = kv.Next(key[:0])
		keep := false
		if !b0[key[0]] {
			b0[key[0]] = true
			keep = true
		}
		err = iw.AddKey(key, pos, keep)
		if err != nil {
			return err
		}
		hi, _ := murmur3.Sum128WithSeed(key, salt)
		if existenceFilter != nil {
			existenceFilter.AddHash(hi)
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

// For now, M is not stored inside index file.
func OpenBtreeIndexWithDecompressor(indexPath string, M uint64, kvGetter *seg.Reader) (bt *BtIndex, err error) {
	idx := &BtIndex{
		filePath: indexPath,
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

	var pos int
	if len(idx.data[pos:]) == 0 {
		return idx, nil
	}

	idx.ef, pos = eliasfano32.ReadEliasFano(idx.data[pos:])
	idx.pool = sync.Pool{}
	idx.pool.New = func() any {
		return &Cursor{ef: idx.ef, returnInto: &idx.pool}
	}

	defer kvGetter.MadvNormal().DisableReadAhead()

	if len(idx.data[pos:]) == 0 {
		idx.bplus = NewBpsTree(kvGetter, idx.ef, M, idx.dataLookup, idx.keyCmp)
		idx.bplus.cursorGetter = idx.newCursor
		// fallback for files without nodes encoded
	} else {
		nodes, err := decodeListNodes(idx.data[pos:])
		if err != nil {
			return nil, err
		}
		idx.bplus = NewBpsTreeWithNodes(kvGetter, idx.ef, M, idx.dataLookup, idx.keyCmp, nodes)
		idx.bplus.cursorGetter = idx.newCursor
	}

	validationPassed = true
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

// comparing `k` with item of index `di`. using buffer `kBuf` to avoid allocations
func (b *BtIndex) keyCmp(k []byte, di uint64, g *seg.Reader, resBuf []byte) (int, []byte, error) {
	if di >= b.ef.Count() {
		return 0, nil, fmt.Errorf("%w: keyCount=%d, but key %d requested. file: %s", ErrBtIndexLookupBounds, b.ef.Count(), di+1, b.FileName())
	}

	offset := b.ef.Get(di)
	g.Reset(offset)
	if !g.HasNext() {
		return 0, nil, fmt.Errorf("key at %d/%d not found, file: %s", di, b.ef.Count(), b.FileName())
	}

	resBuf, _ = g.Next(resBuf)

	//TODO: use `b.getter.Match` after https://github.com/erigontech/erigon/issues/7855
	return bytes.Compare(resBuf, k), resBuf, nil
	//return b.getter.Match(k), result, nil
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
