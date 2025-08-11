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
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon-lib/common/assert"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/mmap"
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

func newPatternTable(bitLen int) *patternTable {
	pt := &patternTable{
		bitLen: bitLen,
	}
	if bitLen <= condensePatternTableBitThreshold {
		pt.patterns = make([]*codeword, 1<<pt.bitLen)
	}
	return pt
}

func (pt *patternTable) insertWord(cw *codeword) {
	if pt.bitLen <= condensePatternTableBitThreshold {
		codeStep := uint16(1) << uint16(cw.len)
		codeFrom, codeTo := cw.code, cw.code+codeStep
		if pt.bitLen != int(cw.len) && cw.len > 0 {
			codeTo = codeFrom | (uint16(1) << pt.bitLen)
		}

		for c := codeFrom; c < codeTo; c += codeStep {
			pt.patterns[c] = cw
		}
		return
	}

	pt.patterns = append(pt.patterns, cw)
}

func (pt *patternTable) condensedTableSearch(code uint16) *codeword {
	if pt.bitLen <= condensePatternTableBitThreshold {
		return pt.patterns[code]
	}
	for _, cur := range pt.patterns {
		if cur.code == code {
			return cur
		}
		d := code - cur.code
		if d&1 != 0 {
			continue
		}
		if checkDistance(int(cur.len), int(d)) {
			return cur
		}
	}
	return nil
}

type posTable struct {
	pos    []uint64
	lens   []byte
	ptrs   []*posTable
	bitLen int
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
	f               *os.File
	mmapHandle2     *[mmap.MaxMapSize]byte // mmap handle for windows (this is used to close mmap)
	dict            *patternTable
	posDict         *posTable
	mmapHandle1     []byte // mmap handle for unix (this is used to close mmap)
	data            []byte // slice of correct size for the decompressor to work with
	wordsStart      uint64 // Offset of whether the superstrings actually start
	size            int64
	modTime         time.Time
	wordsCount      uint64
	emptyWordsCount uint64

	serializedDictSize uint64
	dictWords          int

	filePath, fileName string

	readAheadRefcnt atomic.Int32 // ref-counter: allow enable/disable read-ahead from goroutines. only when refcnt=0 - disable read-ahead once
}

const (
	// Maximal Huffman tree depth
	// Note: mainnet has patternMaxDepth 31
	maxAllowedDepth = 50

	compressedMinSize = 32
)

// Tables with bitlen greater than threshold will be condensed.
// Condensing reduces size of decompression table but leads to slower reads.
// To disable condesning at all set to 9 (we don't use tables larger than 2^9)
// To enable condensing for tables of size larger 64 = 6
// for all tables                                    = 0
// There is no sense to condense tables of size [1 - 64] in terms of performance
//
// Should be set before calling NewDecompression.
var condensePatternTableBitThreshold = 9

func init() {
	v, _ := os.LookupEnv("DECOMPRESS_CONDENSITY")
	if v != "" {
		i, err := strconv.Atoi(v)
		if err != nil {
			panic(err)
		}
		if i < 3 || i > 9 {
			panic("DECOMPRESS_CONDENSITY: only numbers in range 3-9 are acceptable ")
		}
		condensePatternTableBitThreshold = i
		fmt.Printf("set DECOMPRESS_CONDENSITY to %d\n", i)
	}
}

func SetDecompressionTableCondensity(fromBitSize int) {
	condensePatternTableBitThreshold = fromBitSize
}

func NewDecompressor(compressedFilePath string) (*Decompressor, error) {
	_, fName := filepath.Split(compressedFilePath)
	var err error
	var validationPassed = false
	d := &Decompressor{
		filePath: compressedFilePath,
		fileName: fName,
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
	if d.size < compressedMinSize {
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

	d.wordsCount = binary.BigEndian.Uint64(d.data[:8])
	d.emptyWordsCount = binary.BigEndian.Uint64(d.data[8:16])

	pos := uint64(24)
	dictSize := binary.BigEndian.Uint64(d.data[16:pos])
	d.serializedDictSize = dictSize

	if pos+dictSize > uint64(d.size) {
		return nil, &ErrCompressedFileCorrupted{
			FileName: fName,
			Reason: fmt.Sprintf("invalid patterns dictSize=%s while file size is just %s",
				datasize.ByteSize(dictSize).HR(), datasize.ByteSize(d.size).HR())}
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
		// fmt.Printf("pattern maxDepth=%d\n", tree.maxDepth)
		d.dict = newPatternTable(bitLen)
		if _, err = buildCondensedPatternTable(d.dict, depths, patterns, 0, 0, 0, patternMaxDepth); err != nil {
			return nil, &ErrCompressedFileCorrupted{FileName: fName, Reason: err.Error()}
		}
	}

	if assert.Enable && pos != 24 {
		panic("pos != 24")
	}
	pos += dictSize // offset patterns
	// read positions
	dictSize = binary.BigEndian.Uint64(d.data[pos : pos+8])
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

	if dictSize > 0 {
		var bitLen int
		if posMaxDepth > 9 {
			bitLen = 9
		} else {
			bitLen = int(posMaxDepth)
		}
		//fmt.Printf("pos maxDepth=%d\n", tree.maxDepth)
		tableSize := 1 << bitLen
		d.posDict = &posTable{
			bitLen: bitLen,
			pos:    make([]uint64, tableSize),
			lens:   make([]byte, tableSize),
			ptrs:   make([]*posTable, tableSize),
		}
		if _, err = buildPosTable(posDepths, poss, d.posDict, 0, 0, 0, posMaxDepth); err != nil {
			return nil, &ErrCompressedFileCorrupted{FileName: fName, Reason: err.Error()}
		}
	}
	d.wordsStart = pos + dictSize

	if d.Count() == 0 && dictSize == 0 && d.size > compressedMinSize {
		return nil, &ErrCompressedFileCorrupted{
			FileName: fName, Reason: fmt.Sprintf("size %v but no words in it", datasize.ByteSize(d.size).HR())}
	}
	validationPassed = true
	return d, nil
}

func buildCondensedPatternTable(table *patternTable, depths []uint64, patterns [][]byte, code uint16, bits int, depth uint64, maxDepth uint64) (int, error) {
	if maxDepth > maxAllowedDepth {
		return 0, fmt.Errorf("buildCondensedPatternTable: maxDepth=%d is too deep", maxDepth)
	}

	if len(depths) == 0 {
		return 0, nil
	}
	if depth == depths[0] {
		pattern := word(patterns[0])
		//fmt.Printf("depth=%d, maxDepth=%d, code=[%b], codeLen=%d, pattern=[%x]\n", depth, maxDepth, code, bits, pattern)
		cw := &codeword{code: code, pattern: pattern, len: byte(bits), ptr: nil}
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
		cw := &codeword{code: code, pattern: nil, len: byte(0), ptr: newPatternTable(bitLen)}
		table.insertWord(cw)
		return buildCondensedPatternTable(cw.ptr, depths, patterns, 0, 0, depth, maxDepth)
	}
	if maxDepth == 0 {
		return 0, errors.New("buildCondensedPatternTable: maxDepth reached zero")
	}
	b0, err := buildCondensedPatternTable(table, depths, patterns, code, bits+1, depth+1, maxDepth-1)
	if err != nil {
		return 0, err
	}
	b1, err := buildCondensedPatternTable(table, depths[b0:], patterns[b0:], (uint16(1)<<bits)|code, bits+1, depth+1, maxDepth-1)
	return b0 + b1, err
}

func buildPosTable(depths []uint64, poss []uint64, table *posTable, code uint16, bits int, depth uint64, maxDepth uint64) (int, error) {
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
		tableSize := 1 << bitLen
		newTable := &posTable{
			bitLen: bitLen,
			pos:    make([]uint64, tableSize),
			lens:   make([]byte, tableSize),
			ptrs:   make([]*posTable, tableSize),
		}
		table.pos[code] = 0
		table.lens[code] = byte(0)
		table.ptrs[code] = newTable
		return buildPosTable(depths, poss, newTable, 0, 0, depth, maxDepth)
	}
	if maxDepth == 0 {
		return 0, errors.New("buildPosTable: maxDepth reached zero")
	}
	b0, err := buildPosTable(depths, poss, table, code, bits+1, depth+1, maxDepth-1)
	if err != nil {
		return 0, err
	}
	b1, err := buildPosTable(depths[b0:], poss[b0:], table, (uint16(1)<<bits)|code, bits+1, depth+1, maxDepth-1)
	return b0 + b1, err
}

func (d *Decompressor) DataHandle() unsafe.Pointer {
	return unsafe.Pointer(&d.data[0])
}
func (d *Decompressor) SerializedDictSize() uint64 { return d.serializedDictSize }
func (d *Decompressor) DictWords() int             { return d.dictWords }

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
}

func (d *Decompressor) FilePath() string { return d.filePath }
func (d *Decompressor) FileName() string { return d.fileName }

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

// Getter represent "reader" or "iterator" that can move across the data of the decompressor
// The full state of the getter can be captured by saving dataP, and dataBit
type Getter struct {
	patternDict *patternTable
	posDict     *posTable
	fName       string
	data        []byte
	dataP       uint64
	dataBit     int // Value 0..7 - position of the bit
	trace       bool
	d           *Decompressor
}

func (g *Getter) MadvNormal() MadvDisabler {
	g.d.MadvNormal()
	return g
}
func (g *Getter) DisableReadAhead() { g.d.DisableReadAhead() }
func (g *Getter) Trace(t bool)      { g.trace = t }
func (g *Getter) Count() int        { return g.d.Count() }
func (g *Getter) FileName() string  { return g.fName }

func (g *Getter) nextPos(clean bool) (pos uint64) {
	defer func() {
		if rec := recover(); rec != nil {
			panic(fmt.Sprintf("nextPos fails: file: %s, %s, %s", g.fName, rec, dbg.Stack()))
		}
	}()
	if clean && g.dataBit > 0 {
		g.dataP++
		g.dataBit = 0
	}
	table, dataLen, data := g.posDict, len(g.data), g.data
	if table.bitLen == 0 {
		return table.pos[0]
	}
	for l := byte(0); l == 0; {
		code := uint16(data[g.dataP]) >> g.dataBit
		if 8-g.dataBit < table.bitLen && int(g.dataP)+1 < dataLen {
			code |= uint16(data[g.dataP+1]) << (8 - g.dataBit)
		}
		code &= (uint16(1) << table.bitLen) - 1
		l = table.lens[code]
		if l == 0 {
			table = table.ptrs[code]
			g.dataBit += 9
		} else {
			g.dataBit += int(l)
			pos = table.pos[code]
		}
		g.dataP += uint64(g.dataBit / 8)
		g.dataBit %= 8
	}
	return pos
}

func (g *Getter) nextPattern() []byte {
	table := g.patternDict

	if table.bitLen == 0 {
		return table.patterns[0].pattern
	}

	var l byte
	var pattern []byte
	for l == 0 {
		code := uint16(g.data[g.dataP]) >> g.dataBit
		if 8-g.dataBit < table.bitLen && int(g.dataP)+1 < len(g.data) {
			code |= uint16(g.data[g.dataP+1]) << (8 - g.dataBit)
		}
		code &= (uint16(1) << table.bitLen) - 1

		cw := table.condensedTableSearch(code)
		l = cw.len
		if l == 0 {
			table = cw.ptr
			g.dataBit += 9
		} else {
			g.dataBit += int(l)
			pattern = cw.pattern
		}
		g.dataP += uint64(g.dataBit / 8)
		g.dataBit %= 8
	}
	return pattern
}

var condensedWordDistances = buildCondensedWordDistances()

func checkDistance(power int, d int) bool {
	for _, dist := range condensedWordDistances[power] {
		if dist == d {
			return true
		}
	}
	return false
}

func buildCondensedWordDistances() [][]int {
	dist2 := make([][]int, 10)
	for i := 1; i <= 9; i++ {
		dl := make([]int, 0)
		for j := 1 << i; j < 512; j += 1 << i {
			dl = append(dl, j)
		}
		dist2[i] = dl
	}
	return dist2
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
	return &Getter{
		d:           d,
		posDict:     d.posDict,
		data:        d.data[d.wordsStart:],
		patternDict: d.dict,
		fName:       d.FileName(),
	}
}

func (g *Getter) Reset(offset uint64) {
	g.dataP = offset
	g.dataBit = 0
}

func (g *Getter) HasNext() bool {
	return g.dataP < uint64(len(g.data))
}

// Next extracts a compressed word from current offset in the file
// and appends it to the given buf, returning the result of appending
// After extracting next word, it moves to the beginning of the next one
func (g *Getter) Next(buf []byte) ([]byte, uint64) {
	savePos := g.dataP
	wordLen := g.nextPos(true)
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
		buf = buf[:len(buf)+int(wordLen)]
	}

	// Loop below fills in the patterns
	// Tracking position in buf where to insert part of the word
	bufPos := bufOffset
	for pos := g.nextPos(false /* clean */); pos != 0; pos = g.nextPos(false) {
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
	g.nextPos(true /* clean */) // Reset the state of huffman reader

	// Restore to the beginning of buf
	bufPos = bufOffset
	lastUncovered := bufOffset

	// Loop below fills the data which is not in the patterns
	for pos := g.nextPos(false); pos != 0; pos = g.nextPos(false) {
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
	wordLen := g.nextPos(true)
	wordLen-- // because when create huffman tree we do ++ , because 0 is terminator
	if wordLen == 0 {
		if g.dataBit > 0 {
			g.dataP++
			g.dataBit = 0
		}
		return g.data[g.dataP:g.dataP], g.dataP
	}
	g.nextPos(false)
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
	l := g.nextPos(true)
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
	for pos := g.nextPos(false /* clean */); pos != 0; pos = g.nextPos(false) {
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
	wordLen := g.nextPos(true)
	wordLen-- // because when create huffman tree we do ++ , because 0 is terminator
	if wordLen == 0 {
		if g.dataBit > 0 {
			g.dataP++
			g.dataBit = 0
		}
		return g.dataP, 0
	}
	g.nextPos(false)
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
	defer func() {
		g.dataP, g.dataBit = savePos, 0
	}()

	wordLen := g.nextPos(true /* clean */)
	wordLen-- // because when create huffman tree we do ++ , because 0 is terminator
	prefixLen := len(prefix)
	if wordLen == 0 || int(wordLen) < prefixLen {
		if g.dataBit > 0 {
			g.dataP++
			g.dataBit = 0
		}
		if prefixLen != 0 {
			g.dataP, g.dataBit = savePos, 0
		}
		return prefixLen == int(wordLen)
	}

	var bufPos int
	// In the first pass, we only check patterns
	// Only run this loop as far as the prefix goes, there is no need to check further
	for pos := g.nextPos(false /* clean */); pos != 0; pos = g.nextPos(false) {
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
	g.nextPos(true /* clean */) // Reset the state of huffman decoder
	// Second pass - we check spaces not covered by the patterns
	var lastUncovered int
	bufPos = 0
	for pos := g.nextPos(false /* clean */); pos != 0 && lastUncovered < prefixLen; pos = g.nextPos(false) {
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
			return false
		}
	}
	return true
}

// MatchCmp lexicographically compares given buf with the word at the current offset in the file.
// returns 0 if buf == word, -1 if buf < word, 1 if buf > word
func (g *Getter) MatchCmp(buf []byte) int {
	savePos := g.dataP
	wordLen := g.nextPos(true)
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
	for pos := g.nextPos(false /* clean */); pos != 0; pos = g.nextPos(false) {
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
	g.nextPos(true /* clean */) // Reset the state of huffman decoder
	// Second pass - we check spaces not covered by the patterns
	var lastUncovered int
	bufPos = 0
	for pos := g.nextPos(false /* clean */); pos != 0; pos = g.nextPos(false) {
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

	wordLen := g.nextPos(true /* clean */)
	wordLen-- // because when create huffman tree we do ++ , because 0 is terminator
	prefixLen := len(prefix)
	if wordLen == 0 && prefixLen != 0 {
		return true
	}
	if prefixLen == 0 {
		return false
	}

	g.nextPos(true)

	return bytes.HasPrefix(g.data[g.dataP:g.dataP+wordLen], prefix)
}

func (g *Getter) MatchCmpUncompressed(buf []byte) int {
	savePos := g.dataP
	defer func() {
		g.dataP, g.dataBit = savePos, 0
	}()

	wordLen := g.nextPos(true /* clean */)
	wordLen-- // because when create huffman tree we do ++ , because 0 is terminator
	bufLen := len(buf)
	if wordLen == 0 && bufLen != 0 {
		return 1
	}
	if bufLen == 0 {
		return -1
	}

	g.nextPos(true)

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
