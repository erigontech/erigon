/*
   Copyright 2022 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package compress

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
	"unsafe"

	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/mmap"
	"github.com/ledgerwatch/log/v3"
)

type word []byte // plain text word associated with code from dictionary

type codeword struct {
	pattern *word         // Pattern corresponding to entries
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

	filePath, fileName string
}

// Tables with bitlen greater than threshold will be condensed.
// Condensing reduces size of decompression table but leads to slower reads.
// To disable condesning at all set to 9 (we dont use tables larger than 2^9)
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

func NewDecompressor(compressedFilePath string) (d *Decompressor, err error) {
	_, fName := filepath.Split(compressedFilePath)
	d = &Decompressor{
		filePath: compressedFilePath,
		fileName: fName,
	}
	defer func() {

		if rec := recover(); rec != nil {
			err = fmt.Errorf("decompressing file: %s, %+v, trace: %s", compressedFilePath, rec, dbg.Stack())
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
	if d.size < 32 {
		return nil, fmt.Errorf("compressed file is too short: %d", d.size)
	}
	d.modTime = stat.ModTime()
	if d.mmapHandle1, d.mmapHandle2, err = mmap.Mmap(d.f, int(d.size)); err != nil {
		return nil, err
	}
	// read patterns from file
	d.data = d.mmapHandle1[:d.size]
	defer d.EnableReadAhead().DisableReadAhead() //speedup opening on slow drives

	d.wordsCount = binary.BigEndian.Uint64(d.data[:8])
	d.emptyWordsCount = binary.BigEndian.Uint64(d.data[8:16])
	dictSize := binary.BigEndian.Uint64(d.data[16:24])
	data := d.data[24 : 24+dictSize]

	var depths []uint64
	var patterns [][]byte
	var i uint64
	var patternMaxDepth uint64

	for i < dictSize {
		d, ns := binary.Uvarint(data[i:])
		if d > 64 { // mainnet has maxDepth 31
			return nil, fmt.Errorf("dictionary is invalid: patternMaxDepth=%d", d)
		}
		depths = append(depths, d)
		if d > patternMaxDepth {
			patternMaxDepth = d
		}
		i += uint64(ns)
		l, n := binary.Uvarint(data[i:])
		i += uint64(n)
		patterns = append(patterns, data[i:i+l])
		//fmt.Printf("depth = %d, pattern = [%x]\n", d, data[i:i+l])
		i += l
	}

	if dictSize > 0 {
		var bitLen int
		if patternMaxDepth > 9 {
			bitLen = 9
		} else {
			bitLen = int(patternMaxDepth)
		}
		// fmt.Printf("pattern maxDepth=%d\n", tree.maxDepth)
		d.dict = newPatternTable(bitLen)
		buildCondensedPatternTable(d.dict, depths, patterns, 0, 0, 0, patternMaxDepth)
	}

	// read positions
	pos := 24 + dictSize
	dictSize = binary.BigEndian.Uint64(d.data[pos : pos+8])
	data = d.data[pos+8 : pos+8+dictSize]

	var posDepths []uint64
	var poss []uint64
	var posMaxDepth uint64

	i = 0
	for i < dictSize {
		d, ns := binary.Uvarint(data[i:])
		if d > 2048 {
			return nil, fmt.Errorf("dictionary is invalid: posMaxDepth=%d", d)
		}
		posDepths = append(posDepths, d)
		if d > posMaxDepth {
			posMaxDepth = d
		}
		i += uint64(ns)
		pos, n := binary.Uvarint(data[i:])
		i += uint64(n)
		poss = append(poss, pos)
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
		buildPosTable(posDepths, poss, d.posDict, 0, 0, 0, posMaxDepth)
	}
	d.wordsStart = pos + 8 + dictSize
	return d, nil
}

func buildCondensedPatternTable(table *patternTable, depths []uint64, patterns [][]byte, code uint16, bits int, depth uint64, maxDepth uint64) int {
	if len(depths) == 0 {
		return 0
	}
	if depth == depths[0] {
		pattern := word(patterns[0])
		//fmt.Printf("depth=%d, maxDepth=%d, code=[%b], codeLen=%d, pattern=[%x]\n", depth, maxDepth, code, bits, pattern)
		cw := &codeword{code: code, pattern: &pattern, len: byte(bits), ptr: nil}
		table.insertWord(cw)
		return 1
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
	b0 := buildCondensedPatternTable(table, depths, patterns, code, bits+1, depth+1, maxDepth-1)
	return b0 + buildCondensedPatternTable(table, depths[b0:], patterns[b0:], (uint16(1)<<bits)|code, bits+1, depth+1, maxDepth-1)
}

func buildPosTable(depths []uint64, poss []uint64, table *posTable, code uint16, bits int, depth uint64, maxDepth uint64) int {
	if len(depths) == 0 {
		return 0
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
		return 1
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
	b0 := buildPosTable(depths, poss, table, code, bits+1, depth+1, maxDepth-1)
	return b0 + buildPosTable(depths[b0:], poss[b0:], table, (uint16(1)<<bits)|code, bits+1, depth+1, maxDepth-1)
}

func (d *Decompressor) DataHandle() unsafe.Pointer {
	return unsafe.Pointer(&d.data[0])
}

func (d *Decompressor) Size() int64 {
	return d.size
}

func (d *Decompressor) ModTime() time.Time {
	return d.modTime
}

func (d *Decompressor) Close() {
	if d.f != nil {
		if err := mmap.Munmap(d.mmapHandle1, d.mmapHandle2); err != nil {
			log.Log(dbg.FileCloseLogLevel, "unmap", "err", err, "file", d.FileName(), "stack", dbg.Stack())
		}
		if err := d.f.Close(); err != nil {
			log.Log(dbg.FileCloseLogLevel, "close", "err", err, "file", d.FileName(), "stack", dbg.Stack())
		}
		d.f = nil
	}
}

func (d *Decompressor) FilePath() string { return d.filePath }
func (d *Decompressor) FileName() string { return d.fileName }

// WithReadAhead - Expect read in sequential order. (Hence, pages in the given range can be aggressively read ahead, and may be freed soon after they are accessed.)
func (d *Decompressor) WithReadAhead(f func() error) error {
	if d == nil || d.mmapHandle1 == nil {
		return nil
	}
	_ = mmap.MadviseSequential(d.mmapHandle1)
	//_ = mmap.MadviseWillNeed(d.mmapHandle1)
	defer mmap.MadviseRandom(d.mmapHandle1)
	return f()
}

// DisableReadAhead - usage: `defer d.EnableReadAhead().DisableReadAhead()`. Please don't use this funcs without `defer` to avoid leak.
func (d *Decompressor) DisableReadAhead() {
	if d == nil || d.mmapHandle1 == nil {
		return
	}
	_ = mmap.MadviseRandom(d.mmapHandle1)
}
func (d *Decompressor) EnableReadAhead() *Decompressor {
	if d == nil || d.mmapHandle1 == nil {
		return d
	}
	_ = mmap.MadviseSequential(d.mmapHandle1)
	return d
}
func (d *Decompressor) EnableMadvNormal() *Decompressor {
	if d == nil || d.mmapHandle1 == nil {
		return d
	}
	_ = mmap.MadviseNormal(d.mmapHandle1)
	return d
}
func (d *Decompressor) EnableWillNeed() *Decompressor {
	if d == nil || d.mmapHandle1 == nil {
		return d
	}
	_ = mmap.MadviseWillNeed(d.mmapHandle1)
	return d
}

// Getter represent "reader" or "interator" that can move accross the data of the decompressor
// The full state of the getter can be captured by saving dataP, and dataBit
type Getter struct {
	patternDict *patternTable
	posDict     *posTable
	fName       string
	data        []byte
	dataP       uint64
	dataBit     int // Value 0..7 - position of the bit
	trace       bool
}

func (g *Getter) Trace(t bool)     { g.trace = t }
func (g *Getter) FileName() string { return g.fName }

func (g *Getter) nextPos(clean bool) (pos uint64) {
	if clean && g.dataBit > 0 {
		g.dataP++
		g.dataBit = 0
	}
	table := g.posDict
	if table.bitLen == 0 {
		return table.pos[0]
	}
	for l := byte(0); l == 0; {
		code := uint16(g.data[g.dataP]) >> g.dataBit
		if 8-g.dataBit < table.bitLen && int(g.dataP)+1 < len(g.data) {
			code |= uint16(g.data[g.dataP+1]) << (8 - g.dataBit)
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
		return *table.patterns[0].pattern
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
			pattern = *cw.pattern
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
		posDict:     d.posDict,
		data:        d.data[d.wordsStart:],
		patternDict: d.dict,
		fName:       d.fileName,
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
	bufPos := len(buf) // Tracking position in buf where to insert part of the word
	lastUncovered := len(buf)
	if len(buf)+int(wordLen) > cap(buf) {
		newBuf := make([]byte, len(buf)+int(wordLen))
		copy(newBuf, buf)
		buf = newBuf
	} else {
		// Expand buffer
		buf = buf[:len(buf)+int(wordLen)]
	}
	// Loop below fills in the patterns
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
	bufPos = lastUncovered      // Restore to the beginning of buf
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
	if int(wordLen) > lastUncovered {
		dif := wordLen - uint64(lastUncovered)
		copy(buf[lastUncovered:wordLen], g.data[postLoopPos:postLoopPos+dif])
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

// Match returns true and next offset if the word at current offset fully matches the buf
// returns false and current offset otherwise.
func (g *Getter) Match(buf []byte) (bool, uint64) {
	savePos := g.dataP
	wordLen := g.nextPos(true)
	wordLen-- // because when create huffman tree we do ++ , because 0 is terminator
	lenBuf := len(buf)
	if wordLen == 0 || int(wordLen) != lenBuf {
		if g.dataBit > 0 {
			g.dataP++
			g.dataBit = 0
		}
		if lenBuf != 0 {
			g.dataP, g.dataBit = savePos, 0
		}
		return lenBuf == int(wordLen), g.dataP
	}

	var bufPos int
	// In the first pass, we only check patterns
	for pos := g.nextPos(false /* clean */); pos != 0; pos = g.nextPos(false) {
		bufPos += int(pos) - 1
		pattern := g.nextPattern()
		if lenBuf < bufPos+len(pattern) || !bytes.Equal(buf[bufPos:bufPos+len(pattern)], pattern) {
			g.dataP, g.dataBit = savePos, 0
			return false, savePos
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
	for pos := g.nextPos(false /* clean */); pos != 0; pos = g.nextPos(false) {
		bufPos += int(pos) - 1
		if bufPos > lastUncovered {
			dif := uint64(bufPos - lastUncovered)
			if lenBuf < bufPos || !bytes.Equal(buf[lastUncovered:bufPos], g.data[postLoopPos:postLoopPos+dif]) {
				g.dataP, g.dataBit = savePos, 0
				return false, savePos
			}
			postLoopPos += dif
		}
		lastUncovered = bufPos + len(g.nextPattern())
	}
	if int(wordLen) > lastUncovered {
		dif := wordLen - uint64(lastUncovered)
		if lenBuf < int(wordLen) || !bytes.Equal(buf[lastUncovered:wordLen], g.data[postLoopPos:postLoopPos+dif]) {
			g.dataP, g.dataBit = savePos, 0
			return false, savePos
		}
		postLoopPos += dif
	}
	if lenBuf != int(wordLen) {
		g.dataP, g.dataBit = savePos, 0
		return false, savePos
	}
	g.dataP, g.dataBit = postLoopPos, 0
	return true, postLoopPos
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

// MatchPrefixCmp lexicographically compares given prefix with the word at the current offset in the file.
// returns 0 if buf == word, -1 if buf < word, 1 if buf > word
func (g *Getter) MatchPrefixCmp(prefix []byte) int {
	savePos := g.dataP
	defer func() {
		g.dataP, g.dataBit = savePos, 0
	}()

	wordLen := g.nextPos(true /* clean */)
	wordLen-- // because when create huffman tree we do ++ , because 0 is terminator
	prefixLen := len(prefix)
	if wordLen == 0 && prefixLen != 0 {
		return 1
	}
	if prefixLen == 0 {
		return 0
	}

	decoded := make([]byte, wordLen)
	var bufPos int
	// In the first pass, we only check patterns
	// Only run this loop as far as the prefix goes, there is no need to check further
	for pos := g.nextPos(false /* clean */); pos != 0; pos = g.nextPos(false) {
		bufPos += int(pos) - 1
		if bufPos > prefixLen {
			break
		}
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
	for pos := g.nextPos(false /* clean */); pos != 0 && lastUncovered < prefixLen; pos = g.nextPos(false) {
		bufPos += int(pos) - 1
		if bufPos > lastUncovered {
			dif := uint64(bufPos - lastUncovered)
			copy(decoded[lastUncovered:bufPos], g.data[postLoopPos:postLoopPos+dif])
			postLoopPos += dif
		}
		lastUncovered = bufPos + len(g.nextPattern())
	}
	if prefixLen > lastUncovered && int(wordLen) > lastUncovered {
		dif := wordLen - uint64(lastUncovered)
		copy(decoded[lastUncovered:wordLen], g.data[postLoopPos:postLoopPos+dif])
		// postLoopPos += dif
	}
	var cmp int
	if prefixLen > int(wordLen) {
		// TODO(racytech): handle this case
		// e.g: prefix = 'aaacb'
		// 		word = 'aaa'
		cmp = bytes.Compare(prefix, decoded)
	} else {
		cmp = bytes.Compare(prefix, decoded[:prefixLen])
	}

	return cmp
}

func (g *Getter) MatchPrefixUncompressed(prefix []byte) int {
	savePos := g.dataP
	defer func() {
		g.dataP, g.dataBit = savePos, 0
	}()

	wordLen := g.nextPos(true /* clean */)
	wordLen-- // because when create huffman tree we do ++ , because 0 is terminator
	prefixLen := len(prefix)
	if wordLen == 0 && prefixLen != 0 {
		return 1
	}
	if prefixLen == 0 {
		return 0
	}

	g.nextPos(true)

	// if prefixLen > int(wordLen) {
	// 	// TODO(racytech): handle this case
	// 	// e.g: prefix = 'aaacb'
	// 	// 		word = 'aaa'
	// }

	return bytes.Compare(prefix, g.data[g.dataP:g.dataP+wordLen])
}

// FastNext extracts a compressed word from current offset in the file
// into the given buf, returning a new byte slice which contains extracted word.
// It is important to allocate enough buf size. Could throw an error if word in file is larger then the buf size.
// After extracting next word, it moves to the beginning of the next one
func (g *Getter) FastNext(buf []byte) ([]byte, uint64) {
	defer func() {
		if rec := recover(); rec != nil {
			panic(fmt.Sprintf("file: %s, %s, %s", g.fName, rec, dbg.Stack()))
		}
	}()

	savePos := g.dataP
	wordLen := g.nextPos(true)
	wordLen-- // because when create huffman tree we do ++ , because 0 is terminator
	// decoded := make([]byte, wordLen)
	if wordLen == 0 {
		if g.dataBit > 0 {
			g.dataP++
			g.dataBit = 0
		}
		return buf[:wordLen], g.dataP
	}
	bufPos := 0 // Tracking position in buf where to insert part of the word
	lastUncovered := 0

	// if int(wordLen) > cap(buf) {
	// 	newBuf := make([]byte, int(wordLen))
	// 	buf = newBuf
	// }
	// Loop below fills in the patterns
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
	bufPos = lastUncovered      // Restore to the beginning of buf
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
	if int(wordLen) > lastUncovered {
		dif := wordLen - uint64(lastUncovered)
		copy(buf[lastUncovered:wordLen], g.data[postLoopPos:postLoopPos+dif])
		postLoopPos += dif
	}
	g.dataP = postLoopPos
	g.dataBit = 0
	return buf[:wordLen], postLoopPos
}
