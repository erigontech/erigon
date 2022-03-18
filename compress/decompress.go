/*
   Copyright 2021 Erigon contributors

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

	"github.com/ledgerwatch/erigon-lib/mmap"
)

type patternTable struct {
	bitLen   int             // Number of bits to lookup in the table
	patterns [][]byte        // Patterns corresponding to entries
	lens     []byte          // Number of bits in the codes
	ptrs     []*patternTable // pointers to deeper level tables
}

type posTable struct {
	bitLen int // Number of bits to lookup in the table
	pos    []uint64
	lens   []byte
	ptrs   []*posTable
}

type huffmanNodePos struct {
	zero     *huffmanNodePos
	one      *huffmanNodePos
	pos      uint64
	maxDepth int
}

type huffmanNodePattern struct {
	zero     *huffmanNodePattern
	one      *huffmanNodePattern
	pattern  []byte
	maxDepth int
}

// Decompressor provides access to the superstrings in a file produced by a compressor
type Decompressor struct {
	compressedFile string
	f              *os.File
	mmapHandle1    []byte                 // mmap handle for unix (this is used to close mmap)
	mmapHandle2    *[mmap.MaxMapSize]byte // mmap handle for windows (this is used to close mmap)
	data           []byte                 // slice of correct size for the decompressor to work with
	dict           *patternTable
	posDict        *posTable
	wordsStart     uint64 // Offset of whether the superstrings actually start
	size           int64

	wordsCount, emptyWordsCount uint64
}

func NewDecompressor(compressedFile string) (*Decompressor, error) {
	d := &Decompressor{
		compressedFile: compressedFile,
	}
	var err error
	d.f, err = os.Open(compressedFile)
	if err != nil {
		return nil, err
	}
	var stat os.FileInfo
	if stat, err = d.f.Stat(); err != nil {
		return nil, err
	}
	d.size = stat.Size()
	if d.size < 40 {
		return nil, fmt.Errorf("compressed file is too short: %d", d.size)
	}
	if d.mmapHandle1, d.mmapHandle2, err = mmap.Mmap(d.f, int(d.size)); err != nil {
		return nil, err
	}
	d.data = d.mmapHandle1[:d.size]
	d.wordsCount = binary.BigEndian.Uint64(d.data[:8])
	d.emptyWordsCount = binary.BigEndian.Uint64(d.data[8:16])
	dictSize := binary.BigEndian.Uint64(d.data[16:24])
	rootOffset := binary.BigEndian.Uint64(d.data[24:32])
	cutoff := binary.BigEndian.Uint64(d.data[32:40])
	data := d.data[40 : 40+dictSize]
	if dictSize > 0 {
		tree := buildHuffmanPattern(data, rootOffset, cutoff)
		var bitLen int
		if tree.maxDepth > 9 {
			bitLen = 9
		} else {
			bitLen = tree.maxDepth
		}
		//fmt.Printf("pattern maxDepth=%d\n", tree.maxDepth)
		tableSize := 1 << bitLen
		d.dict = &patternTable{
			bitLen:   bitLen,
			patterns: make([][]byte, tableSize),
			lens:     make([]byte, tableSize),
			ptrs:     make([]*patternTable, tableSize),
		}
		buildPatternTable(tree, d.dict, 0, 0)
	}
	pos := 40 + dictSize
	dictSize = binary.BigEndian.Uint64(d.data[pos : pos+8])
	rootOffset = binary.BigEndian.Uint64(d.data[pos+8 : pos+16])
	cutoff = binary.BigEndian.Uint64(d.data[pos+16 : pos+24])
	data = d.data[pos+24 : pos+24+dictSize]
	if dictSize > 0 {
		tree := buildHuffmanPos(data, rootOffset, cutoff)
		var bitLen int
		if tree.maxDepth > 9 {
			bitLen = 9
		} else {
			bitLen = tree.maxDepth
		}
		//fmt.Printf("pos maxDepth=%d\n", tree.maxDepth)
		tableSize := 1 << bitLen
		d.posDict = &posTable{
			bitLen: bitLen,
			pos:    make([]uint64, tableSize),
			lens:   make([]byte, tableSize),
			ptrs:   make([]*posTable, tableSize),
		}
		buildPosTable(tree, d.posDict, 0, 0)
	}
	d.wordsStart = pos + 24 + dictSize
	return d, nil
}

func buildHuffmanPos(data []byte, offset uint64, cutoff uint64) *huffmanNodePos {
	if offset < cutoff {
		pos, _ := binary.Uvarint(data[offset:])
		return &huffmanNodePos{pos: pos, maxDepth: 0}
	}
	offsetZero, n := binary.Uvarint(data[offset:])
	offsetOne, _ := binary.Uvarint(data[offset+uint64(n):])
	t0 := buildHuffmanPos(data, offsetZero, cutoff)
	t1 := buildHuffmanPos(data, offsetOne, cutoff)
	var maxDepth int
	if t0.maxDepth > t1.maxDepth {
		maxDepth = t0.maxDepth + 1
	} else {
		maxDepth = t1.maxDepth + 1
	}
	return &huffmanNodePos{zero: t0, one: t1, maxDepth: maxDepth}
}

func buildHuffmanPattern(data []byte, offset uint64, cutoff uint64) *huffmanNodePattern {
	if offset < cutoff {
		l, n := binary.Uvarint(data[offset:])
		return &huffmanNodePattern{pattern: data[offset+uint64(n) : offset+uint64(n)+l], maxDepth: 0}
	}
	offsetZero, n := binary.Uvarint(data[offset:])
	offsetOne, _ := binary.Uvarint(data[offset+uint64(n):])
	t0 := buildHuffmanPattern(data, offsetZero, cutoff)
	t1 := buildHuffmanPattern(data, offsetOne, cutoff)
	var maxDepth int
	if t0.maxDepth > t1.maxDepth {
		maxDepth = t0.maxDepth + 1
	} else {
		maxDepth = t1.maxDepth + 1
	}
	return &huffmanNodePattern{zero: t0, one: t1, maxDepth: maxDepth}
}

func buildPatternTable(tree *huffmanNodePattern, table *patternTable, code uint16, depth int) {
	if tree.zero == nil && tree.one == nil {
		if table.bitLen == depth {
			table.patterns[code] = tree.pattern
			//fmt.Printf(".[%b]%d=>[%s]\n", code, table.bitLen, tree.pattern)
			table.lens[code] = byte(depth)
			table.ptrs[code] = nil
		} else {
			codeStep := uint16(1) << depth
			codeFrom := code
			codeTo := code | (uint16(1) << table.bitLen)
			for c := codeFrom; c < codeTo; c += codeStep {
				table.patterns[c] = tree.pattern
				//fmt.Printf("*[%b]%d=>[%s]\n", c, table.bitLen, tree.pattern)
				table.lens[c] = byte(depth)
				table.ptrs[c] = nil
			}
		}
		return
	}
	if depth == 9 {
		var bitLen int
		if tree.maxDepth > 9 {
			bitLen = 9
		} else {
			bitLen = tree.maxDepth
		}
		tableSize := 1 << bitLen
		newTable := &patternTable{
			bitLen:   bitLen,
			patterns: make([][]byte, tableSize),
			lens:     make([]byte, tableSize),
			ptrs:     make([]*patternTable, tableSize),
		}
		table.patterns[code] = nil
		table.lens[code] = byte(0)
		table.ptrs[code] = newTable
		buildPatternTable(tree, newTable, 0, 0)
		return
	}
	buildPatternTable(tree.zero, table, code, depth+1)
	buildPatternTable(tree.one, table, (uint16(1)<<depth)|code, depth+1)
}

func buildPosTable(tree *huffmanNodePos, table *posTable, code uint16, depth int) {
	if tree.zero == nil && tree.one == nil {
		if table.bitLen == depth {
			table.pos[code] = tree.pos
			//fmt.Printf(".[%b]%d=>%d\n", code, table.bitLen, tree.pos)
			table.lens[code] = byte(depth)
			table.ptrs[code] = nil
		} else {
			codeStep := uint16(1) << depth
			codeFrom := code
			codeTo := code | (uint16(1) << table.bitLen)
			for c := codeFrom; c < codeTo; c += codeStep {
				table.pos[c] = tree.pos
				//fmt.Printf("*[%b]%d=>%d\n", c, table.bitLen, tree.pos)
				table.lens[c] = byte(depth)
				table.ptrs[c] = nil
			}
		}
		return
	}
	if depth == 9 {
		var bitLen int
		if tree.maxDepth > 9 {
			bitLen = 9
		} else {
			bitLen = tree.maxDepth
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
		buildPosTable(tree, newTable, 0, 0)
		return
	}
	buildPosTable(tree.zero, table, code, depth+1)
	buildPosTable(tree.one, table, (uint16(1)<<depth)|code, depth+1)
}

func (d *Decompressor) Size() int64 {
	return d.size
}

func (d *Decompressor) Close() error {
	if err := mmap.Munmap(d.mmapHandle1, d.mmapHandle2); err != nil {
		return err
	}
	if err := d.f.Close(); err != nil {
		return err
	}
	return nil
}

func (d *Decompressor) FilePath() string { return d.compressedFile }

//WithReadAhead - Expect read in sequential order. (Hence, pages in the given range can be aggressively read ahead, and may be freed soon after they are accessed.)
func (d *Decompressor) WithReadAhead(f func() error) error {
	_ = mmap.MadviseSequential(d.mmapHandle1)
	defer mmap.MadviseRandom(d.mmapHandle1)
	return f()
}

// Getter represent "reader" or "interator" that can move accross the data of the decompressor
// The full state of the getter can be captured by saving dataP, b, and mask values.
type Getter struct {
	data        []byte
	dataP       uint64
	dataBit     int // Value 0..7 - position of the bit
	patternDict *patternTable
	posDict     *posTable
	fName       string
}

func (g *Getter) nextPos(clean bool) uint64 {
	if clean {
		if g.dataBit > 0 {
			g.dataP++
			g.dataBit = 0
		}
	}
	table := g.posDict
	if table.bitLen == 0 {
		return table.pos[0]
	}
	var l byte
	var pos uint64
	for l == 0 {
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
		g.dataBit = g.dataBit % 8
	}
	return pos
}

func (g *Getter) nextPattern() []byte {
	table := g.patternDict
	if table.bitLen == 0 {
		return table.patterns[0]
	}
	var l byte
	var pattern []byte
	for l == 0 {
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
			pattern = table.patterns[code]
		}
		g.dataP += uint64(g.dataBit / 8)
		g.dataBit = g.dataBit % 8
	}
	return pattern
}

func (d *Decompressor) Count() int           { return int(d.wordsCount) }
func (d *Decompressor) EmptyWordsCount() int { return int(d.emptyWordsCount) }

// MakeGetter creates an object that can be used to access superstrings in the decompressor's file
// Getter is not thread-safe, but there can be multiple getters used simultaneously and concurrently
// for the same decompressor
func (d *Decompressor) MakeGetter() *Getter {
	return &Getter{patternDict: d.dict, posDict: d.posDict, data: d.data[d.wordsStart:], fName: d.compressedFile}
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
	l := g.nextPos(true)
	l-- // because when create huffman tree we do ++ , because 0 is terminator
	if l == 0 {
		if g.dataBit > 0 {
			g.dataP++
			g.dataBit = 0
		}
		return buf, g.dataP
	}
	bufPos := len(buf) // Tracking position in buf where to insert part of the word
	lastUncovered := len(buf)
	if len(buf)+int(l) > cap(buf) {
		newBuf := make([]byte, len(buf)+int(l))
		copy(newBuf, buf)
		buf = newBuf
	} else {
		// Expand buffer
		buf = buf[:len(buf)+int(l)]
	}
	// Loop below fills in the patterns
	for pos := g.nextPos(false /* clean */); pos != 0; pos = g.nextPos(false) {
		bufPos += int(pos) - 1 // Positions where to insert patterns are encoded relative to one another
		copy(buf[bufPos:], g.nextPattern())
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
	for pos := g.nextPos(false /* clean */); pos != 0; pos = g.nextPos(false) {
		bufPos += int(pos) - 1 // Positions where to insert patterns are encoded relative to one another
		if bufPos > lastUncovered {
			dif := uint64(bufPos - lastUncovered)
			copy(buf[lastUncovered:bufPos], g.data[postLoopPos:postLoopPos+dif])
			postLoopPos += dif
		}
		lastUncovered = bufPos + len(g.nextPattern())
	}
	if int(l) > lastUncovered {
		dif := l - uint64(lastUncovered)
		copy(buf[lastUncovered:l], g.data[postLoopPos:postLoopPos+dif])
		postLoopPos += dif
	}
	g.dataP = postLoopPos
	g.dataBit = 0
	return buf, postLoopPos
}

func (g *Getter) NextUncompressed() ([]byte, uint64) {
	l := g.nextPos(true)
	l-- // because when create huffman tree we do ++ , because 0 is terminator
	if l == 0 {
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
	g.dataP += l
	return g.data[pos:g.dataP], g.dataP
}

// Skip moves offset to the next word and returns the new offset.
func (g *Getter) Skip() uint64 {
	l := g.nextPos(true)
	l-- // because when create huffman tree we do ++ , because 0 is terminator
	if l == 0 {
		if g.dataBit > 0 {
			g.dataP++
			g.dataBit = 0
		}
		return g.dataP
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
	return g.dataP
}

// Match returns true and next offset if the word at current offset fully matches the buf
// returns false and current offset otherwise.
func (g *Getter) Match(buf []byte) (bool, uint64) {
	savePos := g.dataP
	l := g.nextPos(true)
	l-- // because when create huffman tree we do ++ , because 0 is terminator
	lenBuf := len(buf)
	if l == 0 {
		if g.dataBit > 0 {
			g.dataP++
			g.dataBit = 0
		}
		if lenBuf != 0 {
			g.dataP, g.dataBit = savePos, 0
		}
		return lenBuf == 0, g.dataP
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
	if int(l) > lastUncovered {
		dif := l - uint64(lastUncovered)
		if lenBuf < int(l) || !bytes.Equal(buf[lastUncovered:l], g.data[postLoopPos:postLoopPos+dif]) {
			g.dataP, g.dataBit = savePos, 0
			return false, savePos
		}
		postLoopPos += dif
	}
	if lenBuf != int(l) {
		g.dataP, g.dataBit = savePos, 0
		return false, savePos
	}
	g.dataP, g.dataBit = postLoopPos, 0
	return true, postLoopPos
}

// MatchPrefix only checks if the word at the current offset has a buf prefix. Does not move offset to the next word.
func (g *Getter) MatchPrefix(buf []byte) bool {
	savePos := g.dataP
	defer func() {
		g.dataP, g.dataBit = savePos, 0
	}()

	l := g.nextPos(true /* clean */)
	l-- // because when create huffman tree we do ++ , because 0 is terminator
	lenBuf := len(buf)
	if l == 0 {
		if g.dataBit > 0 {
			g.dataP++
			g.dataBit = 0
		}
		if lenBuf != 0 {
			g.dataP, g.dataBit = savePos, 0
		}
		return lenBuf == 0
	}

	var bufPos int
	// In the first pass, we only check patterns
	// Only run this loop as far as the prefix goes, there is no need to check further
	for pos := g.nextPos(false /* clean */); pos != 0 && bufPos < lenBuf; pos = g.nextPos(false) {
		bufPos += int(pos) - 1
		pattern := g.nextPattern()
		var comparisonLen int
		if lenBuf < bufPos+len(pattern) {
			comparisonLen = lenBuf - bufPos
		} else {
			comparisonLen = len(pattern)
		}
		if !bytes.Equal(buf[bufPos:bufPos+comparisonLen], pattern[:comparisonLen]) {
			return false
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
	for pos := g.nextPos(false /* clean */); pos != 0 && lastUncovered < lenBuf; pos = g.nextPos(false) {
		bufPos += int(pos) - 1
		patternLen := len(g.nextPattern())
		if bufPos > lastUncovered {
			dif := uint64(bufPos - lastUncovered)
			var comparisonLen int
			if lenBuf < lastUncovered+int(dif) {
				comparisonLen = lenBuf - lastUncovered
			} else {
				comparisonLen = int(dif)
			}
			if !bytes.Equal(buf[lastUncovered:lastUncovered+comparisonLen], g.data[postLoopPos:postLoopPos+uint64(comparisonLen)]) {
				return false
			}
			postLoopPos += dif
		}
		lastUncovered = bufPos + patternLen
	}
	if lenBuf > lastUncovered && int(l) > lastUncovered {
		dif := l - uint64(lastUncovered)
		var comparisonLen int
		if lenBuf < int(l) {
			comparisonLen = lenBuf - lastUncovered
		} else {
			comparisonLen = int(dif)
		}
		if !bytes.Equal(buf[lastUncovered:lastUncovered+comparisonLen], g.data[postLoopPos:postLoopPos+uint64(comparisonLen)]) {
			return false
		}
	}
	return true
}
