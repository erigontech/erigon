// Copyright 2022 The Erigon Authors
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

package seg

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common/assert"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/mmap"
)

type word []byte // plain text word associated with code from dictionary

type codeword struct {
	pattern word          // Pattern corresponding to entries
	ptr     *patternTable // pointer to deeper level tables
	code    uint16        // code associated with that word
	len     byte          // Number of bits in the codes
}

type patternTable struct {
	patterns []*codeword
	bitLen   int // Number of bits to lookup in the table
}

func (pt *patternTable) insertWord(cw *codeword) {
	codeStep := uint16(1) << uint16(cw.len)
	codeFrom, codeTo := cw.code, cw.code+codeStep
	if pt.bitLen != int(cw.len) && cw.len > 0 {
		codeTo = codeFrom | (uint16(1) << pt.bitLen)
	}

	for c := codeFrom; c < codeTo; c += codeStep {
		pt.patterns[c] = cw
	}
}

type posTable struct {
	pos    []uint64
	lens   []byte
	ptrs   []*posTable
	bitLen int
}

// patternArena pre-allocates all storage for a decompressor's Huffman pattern table,
// consolidating O(N) individual heap allocations into 3 large slabs.
// This reduces GC pressure when many decompressors are open simultaneously.
type patternArena struct {
	codewords []codeword
	tables    []patternTable
	slots     []*codeword // backing store for all patternTable.patterns slices
	cwIdx     int
	tableIdx  int
	slotIdx   int
}

func (a *patternArena) allocCW(code uint16, pattern word, codeLen byte, ptr *patternTable) *codeword {
	cw := &a.codewords[a.cwIdx]
	a.cwIdx++
	cw.code, cw.pattern, cw.len, cw.ptr = code, pattern, codeLen, ptr
	return cw
}

func (a *patternArena) allocTable(bitLen int) *patternTable {
	sz := 1 << bitLen
	t := &a.tables[a.tableIdx]
	a.tableIdx++
	t.bitLen = bitLen
	t.patterns = a.slots[a.slotIdx : a.slotIdx+sz]
	a.slotIdx += sz
	return t
}

// posArena pre-allocates all storage for a decompressor's Huffman position table,
// consolidating O(N) individual heap allocations into 4 large slabs.
type posArena struct {
	tables   []posTable
	posArr   []uint64
	lensArr  []byte
	ptrsArr  []*posTable
	tableIdx int
	slotIdx  int
}

func (a *posArena) allocTable(bitLen int) *posTable {
	sz := 1 << bitLen
	t := &a.tables[a.tableIdx]
	a.tableIdx++
	t.bitLen = bitLen
	t.pos = a.posArr[a.slotIdx : a.slotIdx+sz]
	t.lens = a.lensArr[a.slotIdx : a.slotIdx+sz]
	t.ptrs = a.ptrsArr[a.slotIdx : a.slotIdx+sz]
	a.slotIdx += sz
	return t
}

// countHuffmanArena mirrors the recursive build logic to count, without allocating,
// the exact number of sub-tables and total slots needed for either a pattern or
// position Huffman table. Both tables share the same tree structure.
// Returns (patternsConsumed, extraSlots, numSubTables).
// For pattern tables: numCW = len(patterns) + numSubTables (terminals + routing nodes).
func countHuffmanArena(depths []uint64, bits int, depth, maxDepth uint64) (consumed, extraSlots, numSubTables int) {
	if len(depths) == 0 {
		return
	}
	if depth == depths[0] {
		return 1, 0, 0
	}
	if bits == 9 {
		c, s, nt := countHuffmanArena(depths, 0, depth, maxDepth)
		return c, s + (1 << min(int(maxDepth), 9)), nt + 1
	}
	if maxDepth == 0 {
		return
	}
	c0, s0, nt0 := countHuffmanArena(depths, bits+1, depth+1, maxDepth-1)
	c1, s1, nt1 := countHuffmanArena(depths[c0:], bits+1, depth+1, maxDepth-1)
	return c0 + c1, s0 + s1, nt0 + nt1
}

type ErrCompressedFileCorrupted struct {
	FileName string
	Reason   string
}

func (e ErrCompressedFileCorrupted) Error() string {
	return fmt.Sprintf("compressed file %q dictionary is corrupted: %s", e.FileName, e.Reason)
}

func (e ErrCompressedFileCorrupted) Is(err error) bool {
	var e1 *ErrCompressedFileCorrupted
	return errors.As(err, &e1)
}

// Decompressor provides access to the superstrings in a file produced by a compressor
type Decompressor struct {
	f                   *os.File
	mmapHandle2         *[mmap.MaxMapSize]byte // mmap handle for windows (this is used to close mmap)
	dict                *patternTable
	posDict             *posTable
	patArena            *patternArena // arena keeping all pattern table allocations alive
	posArena            *posArena     // arena keeping all position table allocations alive
	mmapHandle1         []byte        // mmap handle for unix (this is used to close mmap)
	data                []byte        // slice of correct size for the decompressor to work with
	wordsStart          uint64        // Offset of whether the superstrings actually start
	size                int64
	modTime             time.Time
	wordsCount          uint64
	emptyWordsCount     uint64
	hasMetadata         bool
	metadata            []byte
	version             uint8
	featureFlagBitmask  FeatureFlagBitmask
	compPageValuesCount uint8

	serializedDictSize uint64
	lenDictSize        uint64 // huffman encoded lengths
	dictWords          int
	dictLens           int

	filePath, fileName string

	readAheadRefcnt atomic.Int32 // ref-counter: allow enable/disable read-ahead from goroutines. only when refcnt=0 - disable read-ahead once
}

const (
	// Maximal Huffman tree depth
	// Note: mainnet has patternMaxDepth 31
	maxAllowedDepth = 50

	compressedMinSize = 32
)

func NewDecompressor(compressedFilePath string) (*Decompressor, error) {
	return NewDecompressorWithMetadata(compressedFilePath, false)
}

func NewDecompressorWithMetadata(compressedFilePath string, hasMetadata bool) (*Decompressor, error) {
	_, fName := filepath.Split(compressedFilePath)
	var err error
	var validationPassed = false
	d := &Decompressor{
		filePath:    compressedFilePath,
		fileName:    fName,
		hasMetadata: hasMetadata,
	}

	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("incomplete file: %s, %+v, trace: %s", compressedFilePath, rec, dbg.Stack())
		}
		if err != nil || !validationPassed {
			d.Close()
			d = nil
		}
	}()

	d.f, err = os.Open(compressedFilePath)
	if err != nil {
		return nil, err
	}

	var stat os.FileInfo
	if stat, err = d.f.Stat(); err != nil {
		return nil, err
	}
	d.size = stat.Size()
	if !hasMetadata && d.size < compressedMinSize {
		return nil, &ErrCompressedFileCorrupted{
			FileName: fName,
			Reason: fmt.Sprintf("invalid file size %s, expected at least %s",
				datasize.ByteSize(d.size).HR(), datasize.ByteSize(compressedMinSize).HR())}
	}

	d.modTime = stat.ModTime()
	if d.mmapHandle1, d.mmapHandle2, err = mmap.Mmap(d.f, int(d.size)); err != nil {
		return nil, err
	}
	// read patterns from file
	d.data = d.mmapHandle1[:d.size]
	defer d.MadvNormal().DisableReadAhead() //speedup opening on slow drives

	d.version = d.data[0]

	if d.version == FileCompressionFormatV1 {
		// 1st byte: version,
		// 2nd byte: defines how exactly the file is compressed
		// 3rd byte (otional): exists if PageLevelCompressionEnabled flag is enabled, and defines number of values on compressed page
		d.featureFlagBitmask = FeatureFlagBitmask(d.data[1])
		d.data = d.data[2:]
	}

	if d.featureFlagBitmask.Has(PageLevelCompressionEnabled) {
		d.compPageValuesCount = d.data[0]
		d.data = d.data[1:]
	}

	if hasMetadata {
		metadataLen := binary.BigEndian.Uint32(d.data[:4])
		d.metadata = d.data[4 : 4+metadataLen]
		d.data = d.data[4+metadataLen:]

		dataSize := len(d.data)
		if dataSize < compressedMinSize {
			return nil, &ErrCompressedFileCorrupted{
				FileName: fName,
				Reason: fmt.Sprintf("invalid file size %s, expected at least %s",
					datasize.ByteSize(dataSize).HR(), datasize.ByteSize(compressedMinSize).HR())}
		}
		// not editing d.size because of checkFileLenChanges check
	}

	d.wordsCount = binary.BigEndian.Uint64(d.data[:8])
	d.emptyWordsCount = binary.BigEndian.Uint64(d.data[8:16])

	pos := uint64(24)
	dictSize := binary.BigEndian.Uint64(d.data[16:pos])
	d.serializedDictSize = dictSize

	if pos+dictSize > uint64(len(d.data)) {
		return nil, &ErrCompressedFileCorrupted{
			FileName: fName,
			Reason: fmt.Sprintf("invalid patterns dictSize=%s while file size is just %s",
				datasize.ByteSize(dictSize).HR(), datasize.ByteSize(len(d.data)).HR())}
	}

	// todo awskii: want to move dictionary reading to separate function?
	data := d.data[pos : pos+dictSize]

	var depths []uint64
	var patterns [][]byte
	var dictPos uint64
	var patternMaxDepth uint64

	for dictPos < dictSize {
		depth, ns := binary.Uvarint(data[dictPos:])
		if depth > maxAllowedDepth {
			return nil, &ErrCompressedFileCorrupted{
				FileName: fName,
				Reason:   fmt.Sprintf("depth=%d > patternMaxDepth=%d ", depth, maxAllowedDepth)}
		}
		depths = append(depths, depth)
		if depth > patternMaxDepth {
			patternMaxDepth = depth
		}
		dictPos += uint64(ns)
		l, n := binary.Uvarint(data[dictPos:])
		dictPos += uint64(n)
		patterns = append(patterns, data[dictPos:dictPos+l])
		//fmt.Printf("depth = %d, pattern = [%x]\n", depth, data[dictPos:dictPos+l])
		dictPos += l
	}
	d.dictWords = len(patterns)

	if dictSize > 0 {
		var bitLen int
		if patternMaxDepth > 9 {
			bitLen = 9
		} else {
			bitLen = int(patternMaxDepth)
		}
		// Pre-count exact arena sizes, then build with a single set of large allocations
		// instead of O(N) individual ones — reduces GC pressure when many files are open.
		_, extraSlots, numSubTables := countHuffmanArena(depths, 0, 0, patternMaxDepth)
		d.patArena = &patternArena{
			codewords: make([]codeword, len(patterns)+numSubTables), // terminals + routing nodes
			tables:    make([]patternTable, 1+numSubTables),
			slots:     make([]*codeword, (1<<bitLen)+extraSlots),
		}
		d.dict = d.patArena.allocTable(bitLen)
		if _, err = buildCondensedPatternTable(d.dict, depths, patterns, 0, 0, 0, patternMaxDepth, d.patArena); err != nil {
			return nil, &ErrCompressedFileCorrupted{FileName: fName, Reason: err.Error()}
		}
	}

	if assert.Enable && pos != 24 {
		panic("pos != 24")
	}
	pos += dictSize // offset patterns
	// read positions
	dictSize = binary.BigEndian.Uint64(d.data[pos : pos+8])
	d.lenDictSize = dictSize
	pos += 8

	if pos+dictSize > uint64(d.size) {
		return nil, &ErrCompressedFileCorrupted{
			FileName: fName,
			Reason: fmt.Sprintf("invalid dictSize=%s overflows file size of %s",
				datasize.ByteSize(dictSize).HR(), datasize.ByteSize(d.size).HR())}
	}

	data = d.data[pos : pos+dictSize]

	var posDepths []uint64
	var poss []uint64
	var posMaxDepth uint64

	dictPos = 0
	for dictPos < dictSize {
		depth, ns := binary.Uvarint(data[dictPos:])
		if depth > maxAllowedDepth {
			return nil, &ErrCompressedFileCorrupted{FileName: fName, Reason: fmt.Sprintf("posMaxDepth=%d", depth)}
		}
		posDepths = append(posDepths, depth)
		if depth > posMaxDepth {
			posMaxDepth = depth
		}
		dictPos += uint64(ns)
		dp, n := binary.Uvarint(data[dictPos:])
		dictPos += uint64(n)
		poss = append(poss, dp)
	}
	d.dictLens = len(poss)

	if dictSize > 0 {
		var bitLen int
		if posMaxDepth > 9 {
			bitLen = 9
		} else {
			bitLen = int(posMaxDepth)
		}
		// Pre-count exact arena sizes, then build with a single set of large allocations.
		_, extraSlots, numSubTables := countHuffmanArena(posDepths, 0, 0, posMaxDepth)
		totalSlots := (1 << bitLen) + extraSlots
		d.posArena = &posArena{
			tables:  make([]posTable, 1+numSubTables),
			posArr:  make([]uint64, totalSlots),
			lensArr: make([]byte, totalSlots),
			ptrsArr: make([]*posTable, totalSlots),
		}
		d.posDict = d.posArena.allocTable(bitLen)
		if _, err = buildPosTable(posDepths, poss, d.posDict, 0, 0, 0, posMaxDepth, d.posArena); err != nil {
			return nil, &ErrCompressedFileCorrupted{FileName: fName, Reason: err.Error()}
		}
	}
	d.wordsStart = pos + dictSize

	if d.Count() == 0 && dictSize == 0 && d.size > d.calcCompressedMinSize() {
		return nil, &ErrCompressedFileCorrupted{
			FileName: fName, Reason: fmt.Sprintf("size %v but no words in it", datasize.ByteSize(d.size).HR())}
	}

	validationPassed = true
	return d, nil
}

func buildCondensedPatternTable(table *patternTable, depths []uint64, patterns [][]byte, code uint16, bits int, depth uint64, maxDepth uint64, arena *patternArena) (int, error) {
	if maxDepth > maxAllowedDepth {
		return 0, fmt.Errorf("buildCondensedPatternTable: maxDepth=%d is too deep", maxDepth)
	}

	if len(depths) == 0 {
		return 0, nil
	}
	if depth == depths[0] {
		//fmt.Printf("depth=%d, maxDepth=%d, code=[%b], codeLen=%d, pattern=[%x]\n", depth, maxDepth, code, bits, pattern)
		cw := arena.allocCW(code, word(patterns[0]), byte(bits), nil)
		table.insertWord(cw)
		return 1, nil
	}
	if bits == 9 {
		var bitLen int
		if maxDepth > 9 {
			bitLen = 9
		} else {
			bitLen = int(maxDepth)
		}
		subTable := arena.allocTable(bitLen)
		cw := arena.allocCW(code, nil, 0, subTable)
		table.insertWord(cw)
		return buildCondensedPatternTable(subTable, depths, patterns, 0, 0, depth, maxDepth, arena)
	}
	if maxDepth == 0 {
		return 0, errors.New("buildCondensedPatternTable: maxDepth reached zero")
	}
	b0, err := buildCondensedPatternTable(table, depths, patterns, code, bits+1, depth+1, maxDepth-1, arena)
	if err != nil {
		return 0, err
	}
	b1, err := buildCondensedPatternTable(table, depths[b0:], patterns[b0:], (uint16(1)<<bits)|code, bits+1, depth+1, maxDepth-1, arena)
	return b0 + b1, err
}

func buildPosTable(depths []uint64, poss []uint64, table *posTable, code uint16, bits int, depth uint64, maxDepth uint64, arena *posArena) (int, error) {
	if maxDepth > maxAllowedDepth {
		return 0, fmt.Errorf("buildPosTable: maxDepth=%d is too deep", maxDepth)
	}
	if len(depths) == 0 {
		return 0, nil
	}
	if depth == depths[0] {
		p := poss[0]
		//fmt.Printf("depth=%d, maxDepth=%d, code=[%b], codeLen=%d, pos=%d\n", depth, maxDepth, code, bits, p)
		if table.bitLen == bits {
			table.pos[code] = p
			table.lens[code] = byte(bits)
			table.ptrs[code] = nil
		} else {
			codeStep := uint16(1) << bits
			codeFrom := code
			codeTo := code | (uint16(1) << table.bitLen)
			for c := codeFrom; c < codeTo; c += codeStep {
				table.pos[c] = p
				table.lens[c] = byte(bits)
				table.ptrs[c] = nil
			}
		}
		return 1, nil
	}
	if bits == 9 {
		var bitLen int
		if maxDepth > 9 {
			bitLen = 9
		} else {
			bitLen = int(maxDepth)
		}
		newTable := arena.allocTable(bitLen)
		table.pos[code] = 0
		table.lens[code] = byte(0)
		table.ptrs[code] = newTable
		return buildPosTable(depths, poss, newTable, 0, 0, depth, maxDepth, arena)
	}
	if maxDepth == 0 {
		return 0, errors.New("buildPosTable: maxDepth reached zero")
	}
	b0, err := buildPosTable(depths, poss, table, code, bits+1, depth+1, maxDepth-1, arena)
	if err != nil {
		return 0, err
	}
	b1, err := buildPosTable(depths[b0:], poss[b0:], table, (uint16(1)<<bits)|code, bits+1, depth+1, maxDepth-1, arena)
	return b0 + b1, err
}

func (d *Decompressor) DataHandle() unsafe.Pointer {
	return unsafe.Pointer(&d.data[0])
}
func (d *Decompressor) SerializedDictSize() uint64      { return d.serializedDictSize }
func (d *Decompressor) SerializedLenSize() uint64       { return d.lenDictSize }
func (d *Decompressor) DictWords() int                  { return d.dictWords }
func (d *Decompressor) DictLens() int                   { return d.dictLens }
func (d *Decompressor) CompressedPageValuesCount() int  { return int(d.compPageValuesCount) }
func (d *Decompressor) CompressionFormatVersion() uint8 { return d.version }

func (d *Decompressor) Size() int64 {
	return d.size
}

func (d *Decompressor) ModTime() time.Time {
	return d.modTime
}

func (d *Decompressor) IsOpen() bool {
	return d != nil && d.f != nil
}

func (d *Decompressor) checkFileLenChange() {
	if d.f == nil {
		return
	}
	st, err := d.f.Stat()
	if err != nil {
		log.Log(dbg.FileCloseLogLevel, "close", "err", err, "file", d.FileName())
		return
	}
	if d.size != st.Size() {
		err := fmt.Errorf("file len changed: from %d to %d, %s", d.size, st.Size(), d.FileName())
		log.Warn(err.Error())
		panic(err)
	}
}

func (d *Decompressor) Close() {
	if d == nil || d.f == nil {
		return
	}
	d.checkFileLenChange()
	if err := mmap.Munmap(d.mmapHandle1, d.mmapHandle2); err != nil {
		log.Log(dbg.FileCloseLogLevel, "unmap", "err", err, "file", d.FileName(), "stack", dbg.Stack())
	}
	if err := d.f.Close(); err != nil {
		log.Log(dbg.FileCloseLogLevel, "close", "err", err, "file", d.FileName(), "stack", dbg.Stack())
	}

	d.f = nil
	d.data = nil
	d.posDict = nil
	d.dict = nil
	d.patArena = nil
	d.posArena = nil
}

func (d *Decompressor) FilePath() string { return d.filePath }
func (d *Decompressor) FileName() string { return d.fileName }
func (d *Decompressor) GetMetadata() []byte {
	if !d.hasMetadata {
		panic("no metadata stored")
	}
	return d.metadata
}

// WithReadAhead - Expect read in sequential order. (Hence, pages in the given range can be aggressively read ahead, and may be freed soon after they are accessed.)
func (d *Decompressor) WithReadAhead(f func() error) error {
	if d == nil || d.mmapHandle1 == nil {
		return nil
	}
	defer d.MadvSequential().DisableReadAhead()
	return f()
}

// DisableReadAhead - usage: `defer d.EnableReadAhead().DisableReadAhead()`. Please don't use this funcs without `defer` to avoid leak.
func (d *Decompressor) DisableReadAhead() {
	if d == nil || d.mmapHandle1 == nil {
		return
	}
	leftReaders := d.readAheadRefcnt.Add(-1)
	if leftReaders < 0 {
		log.Warn("read-ahead negative counter", "file", d.FileName())
		return
	}

	if !dbg.SnapshotMadvRnd { // all files
		_ = mmap.MadviseNormal(d.mmapHandle1)
		return
	}

	_ = mmap.MadviseRandom(d.mmapHandle1)
}

func (d *Decompressor) MadvSequential() *Decompressor {
	if d == nil || d.mmapHandle1 == nil {
		return d
	}
	d.readAheadRefcnt.Add(1)
	_ = mmap.MadviseSequential(d.mmapHandle1)
	return d
}
func (d *Decompressor) MadvNormal() MadvDisabler {
	if d == nil || d.mmapHandle1 == nil {
		return d
	}
	d.readAheadRefcnt.Add(1)
	_ = mmap.MadviseNormal(d.mmapHandle1)
	return d
}
func (d *Decompressor) MadvWillNeed() *Decompressor {
	if d == nil || d.mmapHandle1 == nil {
		return d
	}
	d.readAheadRefcnt.Add(1)
	_ = mmap.MadviseWillNeed(d.mmapHandle1)
	return d
}

// SequentialView provides a separate mmap of the same file with MADV_SEQUENTIAL.
// Use this for sequential operations (merges, full scans) that run concurrently
// with random readers — it avoids changing the madvise hint on the shared mmap.
//
// Design decisions and kernel-level rationale:
//
//  1. Separate VMA, independent madvise flags.
//     madvise(2) operates per-VMA (Virtual Memory Area). A second mmap() of the
//     same fd creates a new VMA with its own flags — the original VMA's MADV_RANDOM
//     is untouched. This is documented in the Linux madvise(2) man page:
//     https://man7.org/linux/man-pages/man2/madvise.2.html
//     "The advice applies to the region starting at addr and extending for len bytes."
//     See also VMA-level advice in the kernel source:
//     https://www.kernel.org/doc/html/latest/admin-guide/mm/concepts.html#virtual-memory-areas
//
//  2. Shared page cache — no double memory cost.
//     Both mappings (MADV_RANDOM and MADV_SEQUENTIAL) are backed by the same page
//     cache pages (same inode). A second mmap does NOT duplicate physical memory.
//     The page cache is indexed by (inode, offset), so identical pages are shared
//     regardless of how many VMAs map them:
//     https://www.kernel.org/doc/html/latest/admin-guide/mm/concepts.html#page-cache
//
//  3. MADV_SEQUENTIAL triggers readahead and "deactivate behind".
//     The kernel performs aggressive readahead on sequential VMAs and moves accessed
//     pages to the inactive LRU list ("deactivate behind"). However, pages that are
//     also hot in the MADV_RANDOM VMA have the "referenced" bit set and get promoted
//     back to the active list by the second-chance / two-list LRU algorithm:
//     https://www.kernel.org/doc/html/latest/admin-guide/mm/multigen_lru.html
//     https://www.kernel.org/doc/html/latest/mm/page_reclaim.html
//
//  4. Why not just call MadvSequential() on the shared mmap?
//     Calling madvise(MADV_SEQUENTIAL) on the shared mmap changes the VMA flags for
//     ALL concurrent readers of that file. Random RPC lookups would suddenly get
//     sequential readahead (wasted I/O on unneeded pages) and "deactivate behind"
//     (evicting hot pages from the page cache). This is the problem we are solving.
type SequentialView struct {
	d           *Decompressor
	mmapHandle1 []byte
	mmapHandle2 *[mmap.MaxMapSize]byte
	data        []byte // words data region from the sequential mmap
}

// OpenSequentialView creates a separate mmap of the same file with MADV_SEQUENTIAL.
// The caller must call Close when done.
func (d *Decompressor) OpenSequentialView() (*SequentialView, error) {
	if d == nil || d.f == nil {
		return nil, nil
	}
	h1, h2, err := mmap.Mmap(d.f, int(d.size))
	if err != nil {
		return nil, err
	}
	_ = mmap.MadviseNormal(h1)
	// d.data is a sub-slice of d.mmapHandle1 starting after file headers
	// (version, feature flags, metadata). wordsStart is relative to d.data,
	// so the file offset is: headerSize + wordsStart.
	headerSize := d.size - int64(len(d.data))
	wordsFileOffset := headerSize + int64(d.wordsStart)
	return &SequentialView{
		d: d, mmapHandle1: h1, mmapHandle2: h2,
		data: h1[wordsFileOffset:d.size],
	}, nil
}

func (v *SequentialView) MakeGetter() *Getter {
	g := &Getter{
		d:           v.d,
		posDict:     v.d.posDict,
		data:        v.data,
		dataLen:     uint64(len(v.data)),
		patternDict: v.d.dict,
		fName:       v.d.FileName(),
	}
	if v.d.posDict != nil {
		g.posMask = uint16(1)<<v.d.posDict.bitLen - 1
	}
	return g
}

func (v *SequentialView) Close() {
	if v == nil || v.mmapHandle1 == nil {
		return
	}
	_ = mmap.Munmap(v.mmapHandle1, v.mmapHandle2)
	v.mmapHandle1 = nil
	v.data = nil
}

// Getter represent "reader" or "iterator" that can move across the data of the decompressor
// The full state of the getter can be captured by saving dataP, and dataBit
type Getter struct {
	dataP   uint64    // current byte offset in data
	dataLen uint64    // len(data), precomputed
	dataBit int       // bit offset within current byte (0-7)
	posMask uint16    // cached posDict.mask, avoids pointer chain
	posDict *posTable // Huffman table for positions
	data    []byte    // compressed bitstream (ptr at 48, len at 56 = CL0)
	//less hot fields
	patternDict *patternTable
	d           *Decompressor
	fName       string
	trace       bool
}

func (g *Getter) MadvNormal() MadvDisabler {
	g.d.MadvNormal()
	return g
}
func (g *Getter) DisableReadAhead()   { g.d.DisableReadAhead() }
func (g *Getter) Trace(t bool)        { g.trace = t }
func (g *Getter) Count() int          { return g.d.Count() }
func (g *Getter) FileName() string    { return g.fName }
func (g *Getter) GetMetadata() []byte { return g.d.GetMetadata() }

// nextPosClean aligns to the next byte boundary then reads the next position.
func (g *Getter) nextPosClean() uint64 {
	if g.dataBit > 0 {
		g.dataP++
		g.dataBit = 0
	}
	return g.nextPos()
}

// nextPos reads the next position from the Huffman-coded bitstream.
func (g *Getter) nextPos() uint64 {
	if g.posDict.bitLen == 0 {
		return g.posDict.pos[0]
	}
	table := g.posDict
	data := g.data
	dataP := g.dataP
	dataBit := g.dataBit
	dataLen := uint64(len(data))
	mask := g.posMask
	for {
		// Read up to 16 bits starting at dataP, shifted by dataBit
		code := uint16(data[dataP]) >> dataBit
		if 8-dataBit < table.bitLen && dataP+1 < dataLen {
			code |= uint16(data[dataP+1]) << (8 - dataBit)
		}
		code &= mask
		l := int(table.lens[code])
		if l == 0 {
			table = table.ptrs[code]
			dataBit += 9
			dataP += uint64(dataBit >> 3)
			dataBit &= 7
			mask = uint16(1)<<table.bitLen - 1
		} else {
			dataBit += l
			dataP += uint64(dataBit >> 3)
			g.dataP = dataP
			g.dataBit = dataBit & 7
			return table.pos[code]
		}
	}
}

func (g *Getter) nextPattern() []byte {
	table := g.patternDict
	if table.bitLen == 0 {
		return table.patterns[0].pattern
	}

	data := g.data
	dataP := g.dataP
	dataBit := g.dataBit

	for {
		code := uint16(data[dataP]) >> dataBit
		if 8-dataBit < table.bitLen && dataP+1 < g.dataLen {
			code |= uint16(data[dataP+1]) << (8 - dataBit)
		}
		code &= (uint16(1) << table.bitLen) - 1

		cw := table.patterns[code]
		if cw.len == 0 {
			table = cw.ptr
			dataBit += 9
		} else {
			dataBit += int(cw.len)
		}
		dataP += uint64(dataBit >> 3)
		dataBit &= 7
		if cw.len != 0 {
			g.dataP = dataP
			g.dataBit = dataBit
			return cw.pattern
		}
	}
}

func (g *Getter) Size() int {
	return len(g.data)
}

func (d *Decompressor) Count() int           { return int(d.wordsCount) }
func (d *Decompressor) EmptyWordsCount() int { return int(d.emptyWordsCount) }

// MakeGetter creates an object that can be used to access superstrings in the decompressor's file
// Getter is not thread-safe, but there can be multiple getters used simultaneously and concurrently
// for the same decompressor
func (d *Decompressor) MakeGetter() *Getter {
	data := d.data[d.wordsStart:]
	g := &Getter{
		d:           d,
		posDict:     d.posDict,
		data:        data,
		dataLen:     uint64(len(data)),
		patternDict: d.dict,
		fName:       d.FileName(),
	}
	if d.posDict != nil {
		g.posMask = uint16(1)<<g.posDict.bitLen - 1
	}
	return g
}

func (g *Getter) DataLen() int {
	return len(g.data)
}

func (g *Getter) Reset(offset uint64) {
	g.dataP = offset
	g.dataBit = 0
}

func (g *Getter) HasNext() bool {
	return g.dataP < g.dataLen
}

// Next extracts a compressed word from current offset in the file
// and appends it to the given buf, returning the result of appending
// After extracting next word, it moves to the beginning of the next one
func (g *Getter) Next(buf []byte) ([]byte, uint64) {
	savePos := g.dataP
	wordLen := g.nextPosClean()
	wordLen-- // because when create huffman tree we do ++ , because 0 is terminator
	if wordLen == 0 {
		if g.dataBit > 0 {
			g.dataP++
			g.dataBit = 0
		}
		if buf == nil { // wordLen == 0, means we have valid record of 0 size. nil - is the marker of "something not found"
			buf = []byte{}
		}
		return buf, g.dataP
	}

	bufOffset := len(buf)
	if len(buf)+int(wordLen) > cap(buf) {
		newBuf := make([]byte, len(buf)+int(wordLen))
		copy(newBuf, buf)
		buf = newBuf
	} else {
		// Expand buffer
		if len(buf)+int(wordLen) < 0 {
			log.Error("can't expand buffer", "filename", g.fName, "pos", savePos, "buf len", len(buf))
			return nil, 0
		}
		buf = buf[:len(buf)+int(wordLen)]
	}

	// Loop below fills in the patterns
	// Tracking position in buf where to insert part of the word
	bufPos := bufOffset
	for pos := g.nextPos(); pos != 0; pos = g.nextPos() {
		bufPos += int(pos) - 1 // Positions where to insert patterns are encoded relative to one another
		pt := g.nextPattern()
		copy(buf[bufPos:], pt)
	}
	if g.dataBit > 0 {
		g.dataP++
		g.dataBit = 0
	}
	postLoopPos := g.dataP
	g.dataP = savePos
	g.dataBit = 0
	g.nextPosClean() // Reset the state of huffman reader

	// Restore to the beginning of buf
	bufPos = bufOffset
	lastUncovered := bufOffset

	// Loop below fills the data which is not in the patterns
	for pos := g.nextPos(); pos != 0; pos = g.nextPos() {
		bufPos += int(pos) - 1 // Positions where to insert patterns are encoded relative to one another
		if bufPos > lastUncovered {
			dif := uint64(bufPos - lastUncovered)
			copy(buf[lastUncovered:bufPos], g.data[postLoopPos:postLoopPos+dif])
			postLoopPos += dif
		}
		lastUncovered = bufPos + len(g.nextPattern())
	}
	if bufOffset+int(wordLen) > lastUncovered {
		dif := uint64(bufOffset + int(wordLen) - lastUncovered)
		copy(buf[lastUncovered:lastUncovered+int(dif)], g.data[postLoopPos:postLoopPos+dif])
		postLoopPos += dif
	}
	g.dataP = postLoopPos
	g.dataBit = 0
	return buf, postLoopPos
}

func (g *Getter) NextUncompressed() ([]byte, uint64) {
	wordLen := g.nextPosClean()
	wordLen-- // because when create huffman tree we do ++ , because 0 is terminator
	if wordLen == 0 {
		if g.dataBit > 0 {
			g.dataP++
			g.dataBit = 0
		}
		return g.data[g.dataP:g.dataP], g.dataP
	}
	g.nextPos()
	if g.dataBit > 0 {
		g.dataP++
		g.dataBit = 0
	}
	pos := g.dataP
	g.dataP += wordLen
	return g.data[pos:g.dataP], g.dataP
}

// Skip moves offset to the next word and returns the new offset and the length of the word.
func (g *Getter) Skip() (uint64, int) {
	l := g.nextPosClean()
	l-- // because when create huffman tree we do ++ , because 0 is terminator
	if l == 0 {
		if g.dataBit > 0 {
			g.dataP++
			g.dataBit = 0
		}
		return g.dataP, 0
	}
	wordLen := int(l)

	var add uint64
	var bufPos int
	var lastUncovered int
	for pos := g.nextPos(); pos != 0; pos = g.nextPos() {
		bufPos += int(pos) - 1
		if wordLen < bufPos {
			panic(fmt.Sprintf("likely .idx is invalid: %s", g.fName))
		}
		if bufPos > lastUncovered {
			add += uint64(bufPos - lastUncovered)
		}
		lastUncovered = bufPos + len(g.nextPattern())
	}
	if g.dataBit > 0 {
		g.dataP++
		g.dataBit = 0
	}
	if int(l) > lastUncovered {
		add += l - uint64(lastUncovered)
	}
	// Uncovered characters
	g.dataP += add
	return g.dataP, wordLen
}

func (g *Getter) SkipUncompressed() (uint64, int) {
	wordLen := g.nextPosClean()
	wordLen-- // because when create huffman tree we do ++ , because 0 is terminator
	if wordLen == 0 {
		if g.dataBit > 0 {
			g.dataP++
			g.dataBit = 0
		}
		return g.dataP, 0
	}
	g.nextPos()
	if g.dataBit > 0 {
		g.dataP++
		g.dataBit = 0
	}
	g.dataP += wordLen
	return g.dataP, int(wordLen)
}

// MatchPrefix only checks if the word at the current offset has a buf prefix. Does not move offset to the next word.
func (g *Getter) MatchPrefix(prefix []byte) bool {
	savePos := g.dataP

	wordLen := g.nextPosClean()
	wordLen-- // because when create huffman tree we do ++ , because 0 is terminator
	prefixLen := len(prefix)
	if wordLen == 0 || int(wordLen) < prefixLen {
		g.dataP, g.dataBit = savePos, 0
		return prefixLen == int(wordLen)
	}

	var bufPos int
	// In the first pass, we only check patterns
	// Only run this loop as far as the prefix goes, there is no need to check further
	for pos := g.nextPos(); pos != 0; pos = g.nextPos() {
		bufPos += int(pos) - 1
		pattern := g.nextPattern()
		var comparisonLen int
		if prefixLen < bufPos+len(pattern) {
			comparisonLen = prefixLen - bufPos
		} else {
			comparisonLen = len(pattern)
		}
		if bufPos < prefixLen {
			if !bytes.Equal(prefix[bufPos:bufPos+comparisonLen], pattern[:comparisonLen]) {
				g.dataP, g.dataBit = savePos, 0
				return false
			}
		}
	}

	if g.dataBit > 0 {
		g.dataP++
		g.dataBit = 0
	}
	postLoopPos := g.dataP
	g.dataP, g.dataBit = savePos, 0
	g.nextPosClean() // Reset the state of huffman decoder
	// Second pass - we check spaces not covered by the patterns
	var lastUncovered int
	bufPos = 0
	for pos := g.nextPos(); pos != 0 && lastUncovered < prefixLen; pos = g.nextPos() {
		bufPos += int(pos) - 1
		if bufPos > lastUncovered {
			dif := uint64(bufPos - lastUncovered)
			var comparisonLen int
			if prefixLen < lastUncovered+int(dif) {
				comparisonLen = prefixLen - lastUncovered
			} else {
				comparisonLen = int(dif)
			}
			if !bytes.Equal(prefix[lastUncovered:lastUncovered+comparisonLen], g.data[postLoopPos:postLoopPos+uint64(comparisonLen)]) {
				g.dataP, g.dataBit = savePos, 0
				return false
			}
			postLoopPos += dif
		}
		lastUncovered = bufPos + len(g.nextPattern())
	}
	if prefixLen > lastUncovered && int(wordLen) > lastUncovered {
		dif := wordLen - uint64(lastUncovered)
		var comparisonLen int
		if prefixLen < int(wordLen) {
			comparisonLen = prefixLen - lastUncovered
		} else {
			comparisonLen = int(dif)
		}
		if !bytes.Equal(prefix[lastUncovered:lastUncovered+comparisonLen], g.data[postLoopPos:postLoopPos+uint64(comparisonLen)]) {
			g.dataP, g.dataBit = savePos, 0
			return false
		}
	}
	g.dataP, g.dataBit = savePos, 0
	return true
}

// MatchCmp lexicographically compares given buf with the word at the current offset in the file.
// returns 0 if buf == word, -1 if buf < word, 1 if buf > word
func (g *Getter) MatchCmp(buf []byte) int {
	savePos := g.dataP
	wordLen := g.nextPosClean()
	wordLen-- // because when create huffman tree we do ++ , because 0 is terminator
	lenBuf := len(buf)
	if wordLen == 0 && lenBuf != 0 {
		g.dataP, g.dataBit = savePos, 0
		return 1
	}
	if wordLen == 0 && lenBuf == 0 {
		if g.dataBit > 0 {
			g.dataP++
			g.dataBit = 0
		}
		return 0
	}

	decoded := make([]byte, wordLen)
	var bufPos int
	// In the first pass, we only check patterns
	for pos := g.nextPos(); pos != 0; pos = g.nextPos() {
		bufPos += int(pos) - 1
		pattern := g.nextPattern()
		copy(decoded[bufPos:], pattern)
	}
	if g.dataBit > 0 {
		g.dataP++
		g.dataBit = 0
	}
	postLoopPos := g.dataP
	g.dataP, g.dataBit = savePos, 0
	g.nextPosClean() // Reset the state of huffman decoder
	// Second pass - we check spaces not covered by the patterns
	var lastUncovered int
	bufPos = 0
	for pos := g.nextPos(); pos != 0; pos = g.nextPos() {
		bufPos += int(pos) - 1
		// fmt.Printf("BUF POS: %d, POS: %d, lastUncovered: %d\n", bufPos, pos, lastUncovered)
		if bufPos > lastUncovered {
			dif := uint64(bufPos - lastUncovered)
			copy(decoded[lastUncovered:bufPos], g.data[postLoopPos:postLoopPos+dif])
			postLoopPos += dif
		}
		lastUncovered = bufPos + len(g.nextPattern())
	}

	if int(wordLen) > lastUncovered {
		dif := wordLen - uint64(lastUncovered)
		copy(decoded[lastUncovered:wordLen], g.data[postLoopPos:postLoopPos+dif])
		postLoopPos += dif
	}
	cmp := bytes.Compare(buf, decoded)
	if cmp == 0 {
		g.dataP, g.dataBit = postLoopPos, 0
	} else {
		g.dataP, g.dataBit = savePos, 0
	}
	return cmp
}

func (g *Getter) MatchPrefixUncompressed(prefix []byte) bool {
	savePos := g.dataP
	defer func() {
		g.dataP, g.dataBit = savePos, 0
	}()

	wordLen := g.nextPosClean()
	wordLen-- // because when create huffman tree we do ++ , because 0 is terminator
	prefixLen := len(prefix)
	if wordLen == 0 && prefixLen != 0 {
		return true
	}
	if prefixLen == 0 {
		return false
	}

	g.nextPosClean()

	return bytes.HasPrefix(g.data[g.dataP:g.dataP+wordLen], prefix)
}

func (g *Getter) MatchCmpUncompressed(buf []byte) int {
	savePos := g.dataP
	defer func() {
		g.dataP, g.dataBit = savePos, 0
	}()

	wordLen := g.nextPosClean()
	wordLen-- // because when create huffman tree we do ++ , because 0 is terminator
	bufLen := len(buf)
	if wordLen == 0 && bufLen != 0 {
		return 1
	}
	if bufLen == 0 {
		return -1
	}

	g.nextPosClean()

	return bytes.Compare(buf, g.data[g.dataP:g.dataP+wordLen])
}

// BinarySearch - !expecting sorted file - does Seek `g` to key which >= `fromPrefix` by using BinarySearch - means unoptimal and touching many places in file
// use `.Next` to read found
// at `ok = false` leaving `g` in unpredictible state
func (g *Getter) BinarySearch(seek []byte, count int, getOffset func(i uint64) (offset uint64)) (foundOffset uint64, ok bool) {
	var key []byte
	foundItem := sort.Search(count, func(i int) bool {
		offset := getOffset(uint64(i))
		g.Reset(offset)
		if g.HasNext() {
			key, _ = g.Next(key[:0])
			return bytes.Compare(key, seek) >= 0
		}
		return false
	})
	if foundItem == count { // `Search` returns `n` if not found
		return 0, false
	}
	foundOffset = getOffset(uint64(foundItem))
	g.Reset(foundOffset)
	if !g.HasNext() {
		return 0, false
	}
	return foundOffset, true
}

func (d *Decompressor) calcCompressedMinSize() int64 {
	if d.version == FileCompressionFormatV0 {
		return compressedMinSize
	}

	if d.featureFlagBitmask.Has(PageLevelCompressionEnabled) {
		return compressedMinSize + 3 // 2 bytes always are used for bitmask and version + 1 optional for page level compression if enabled
	}

	return compressedMinSize + 2
}
