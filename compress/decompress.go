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
	"strings"

	"github.com/ledgerwatch/erigon-lib/mmap"
)

type huffmanNodePos struct {
	zero *huffmanNodePos
	one  *huffmanNodePos
	pos  uint64
}

type huffmanNodePattern struct {
	zero    *huffmanNodePattern
	one     *huffmanNodePattern
	pattern []byte
}

// Decompressor provides access to the superstrings in a file produced by a compressor
type Decompressor struct {
	compressedFile string
	f              *os.File
	mmapHandle1    []byte                 // mmap handle for unix (this is used to close mmap)
	mmapHandle2    *[mmap.MaxMapSize]byte // mmap handle for windows (this is used to close mmap)
	data           []byte                 // slice of correct size for the decompressor to work with
	dict           *huffmanNodePattern
	posDict        *huffmanNodePos
	wordsStart     uint64 // Offset of whether the superstrings actually start
	count          uint64
	size           int64
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
	if d.size < 24 {
		return nil, fmt.Errorf("compressed file is too short")
	}
	if d.mmapHandle1, d.mmapHandle2, err = mmap.Mmap(d.f, int(d.size)); err != nil {
		return nil, err
	}
	d.data = d.mmapHandle1[:d.size]
	d.count = binary.BigEndian.Uint64(d.data[:8])
	dictSize := binary.BigEndian.Uint64(d.data[8:16])
	rootOffset := binary.BigEndian.Uint64(d.data[16:24])
	cutoff := binary.BigEndian.Uint64(d.data[24:32])
	data := d.data[32 : 32+dictSize]
	if dictSize > 0 {
		d.dict = buildHuffmanPattern(data, rootOffset, cutoff)
	}
	pos := 32 + dictSize
	dictSize = binary.BigEndian.Uint64(d.data[pos : pos+8])
	rootOffset = binary.BigEndian.Uint64(d.data[pos+8 : pos+16])
	cutoff = binary.BigEndian.Uint64(d.data[pos+16 : pos+24])
	data = d.data[pos+24 : pos+24+dictSize]
	if dictSize > 0 {
		d.posDict = buildHuffmanPos(data, rootOffset, cutoff)
	}
	d.wordsStart = pos + 24 + dictSize
	return d, nil
}

func buildHuffmanPos(data []byte, offset uint64, cutoff uint64) *huffmanNodePos {
	if offset < cutoff {
		pos, _ := binary.Uvarint(data[offset:])
		return &huffmanNodePos{pos: pos}
	}
	offsetZero, n := binary.Uvarint(data[offset:])
	offsetOne, _ := binary.Uvarint(data[offset+uint64(n):])
	return &huffmanNodePos{zero: buildHuffmanPos(data, offsetZero, cutoff), one: buildHuffmanPos(data, offsetOne, cutoff)}
}

func buildHuffmanPattern(data []byte, offset uint64, cutoff uint64) *huffmanNodePattern {
	if offset < cutoff {
		l, n := binary.Uvarint(data[offset:])
		return &huffmanNodePattern{pattern: data[offset+uint64(n) : offset+uint64(n)+l]}
	}
	offsetZero, n := binary.Uvarint(data[offset:])
	offsetOne, _ := binary.Uvarint(data[offset+uint64(n):])
	return &huffmanNodePattern{zero: buildHuffmanPattern(data, offsetZero, cutoff), one: buildHuffmanPattern(data, offsetOne, cutoff)}
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
	patternDict *huffmanNodePattern
	posDict     *huffmanNodePos
	b           byte
	mask        byte
	uncovered   []int // Buffer for uncovered portions of the word
	word        []byte
	fName       string
}

func (g *Getter) nextPos(clean bool) uint64 {
	if clean {
		g.mask = 0
	}
	node := g.posDict
	if node.zero == nil && node.one == nil {
		return node.pos
	}
	b := g.b
	mask := g.mask
	dataP := g.dataP
	for node.zero != nil || node.one != nil {
		if mask == 0 {
			mask = 1
			b = g.data[dataP]
			dataP++
		}
		if b&mask == 0 {
			node = node.zero
		} else {
			node = node.one
		}
		mask <<= 1
	}
	g.b = b
	g.mask = mask
	g.dataP = dataP
	return node.pos
}

func (g *Getter) nextPattern() []byte {
	node := g.patternDict
	if node.zero == nil && node.one == nil {
		return node.pattern
	}
	b := g.b
	mask := g.mask
	dataP := g.dataP
	for node.zero != nil || node.one != nil {
		if mask == 0 {
			mask = 1
			b = g.data[dataP]
			dataP++
		}
		if b&mask == 0 {
			node = node.zero
		} else {
			node = node.one
		}
		mask <<= 1
	}
	g.b = b
	g.mask = mask
	g.dataP = dataP
	return node.pattern
}

func (d *Decompressor) Count() int { return int(d.count) }

// MakeGetter creates an object that can be used to access superstrings in the decompressor's file
// Getter is not thread-safe, but there can be multiple getters used simultaneously and concurrently
// for the same decompressor
func (d *Decompressor) MakeGetter() *Getter {
	return &Getter{patternDict: d.dict, posDict: d.posDict, data: d.data[d.wordsStart:], uncovered: make([]int, 0, 128), fName: d.compressedFile}
}

func (g *Getter) Reset(offset uint64) {
	g.dataP = offset
	g.mask = 0
	g.b = 0
}

func (g *Getter) HasNext() bool {
	return g.dataP < uint64(len(g.data))
}

// Next extracts a compressed word from current offset in the file
// and appends it to the given buf, returning the result of appending
// After extracting next word, it moves to the beginning of the next one
func (g *Getter) Next(buf []byte) ([]byte, uint64) {
	l := g.nextPos(true)
	l-- // because when create huffman tree we do ++ , because 0 is terminator
	if l == 0 {
		return buf, g.dataP
	}
	if int(l) > len(g.word) {
		g.word = make([]byte, l)
	}
	var pos uint64
	var lastPos int
	var lastUncovered int
	g.uncovered = g.uncovered[:0]
	for pos = g.nextPos(false /* clean */); pos != 0; pos = g.nextPos(false) {
		intPos := lastPos + int(pos) - 1
		lastPos = intPos
		pattern := g.nextPattern()
		if len(g.word) < intPos {
			panic(fmt.Sprintf("likely .idx is invalid: %s", g.fName))
		}
		copy(g.word[intPos:], pattern)
		if intPos > lastUncovered {
			g.uncovered = append(g.uncovered, lastUncovered, intPos)
		}
		lastUncovered = intPos + len(pattern)
	}
	if int(l) > lastUncovered {
		g.uncovered = append(g.uncovered, lastUncovered, int(l))
	}
	// Uncovered characters
	for i := 0; i < len(g.uncovered); i += 2 {
		copy(g.word[g.uncovered[i]:g.uncovered[i+1]], g.data[g.dataP:])
		g.dataP += uint64(g.uncovered[i+1] - g.uncovered[i])
	}
	buf = append(buf, g.word[:l]...)
	return buf, g.dataP
}

// Skip moves offset to the next word and returns the new offset.
func (g *Getter) Skip() uint64 {
	l := g.nextPos(true)
	l-- // because when create huffman tree we do ++ , because 0 is terminator
	if l == 0 {
		return g.dataP
	}
	wordLen := int(l)

	var add uint64
	var pos uint64
	var lastPos int
	var lastUncovered int
	for pos = g.nextPos(false /* clean */); pos != 0; pos = g.nextPos(false) {
		intPos := lastPos + int(pos) - 1
		lastPos = intPos
		if wordLen < intPos {
			panic(fmt.Sprintf("likely .idx is invalid: %s", g.fName))
		}
		if intPos > lastUncovered {
			add += uint64(intPos - lastUncovered)
		}
		pattern := g.nextPattern()
		lastUncovered = intPos + len(pattern)
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
	saveMask := g.mask
	saveB := g.b
	l := g.nextPos(true)
	l-- // because when create huffman tree we do ++ , because 0 is terminator
	lenBuf := len(buf)
	if l == 0 {
		if lenBuf != 0 {
			g.dataP = savePos
		}
		return lenBuf == 0, g.dataP
	}
	res := true

	var pos uint64
	var lastPos int
	var pattern []byte
	preLoopPos := g.dataP
	preLoopMask := g.mask
	preLoopB := g.b
	// In the first pass, we only check patterns
	for pos = g.nextPos(false /* clean */); pos != 0; pos = g.nextPos(false) {
		intPos := lastPos + int(pos) - 1
		lastPos = intPos
		pattern = g.nextPattern()
		if res && (lenBuf < intPos+len(pattern) || !bytes.Equal(buf[intPos:intPos+len(pattern)], pattern)) {
			res = false
		}
	}
	postLoopPos := g.dataP
	postLoopMask := g.mask
	postLoopB := g.b
	g.dataP = preLoopPos
	g.mask = preLoopMask
	g.b = preLoopB
	// Second pass - we check spaces not covered by the patterns
	var lastUncovered int
	lastPos = 0
	for pos = g.nextPos(false /* clean */); pos != 0; pos = g.nextPos(false) {
		intPos := lastPos + int(pos) - 1
		lastPos = intPos
		pattern = g.nextPattern()
		if intPos > lastUncovered {
			dif := uint64(intPos - lastUncovered)
			if res && (lenBuf < intPos || !bytes.Equal(buf[lastUncovered:intPos], g.data[postLoopPos:postLoopPos+dif])) {
				res = false
			}
			postLoopPos += dif
		}
		lastUncovered = intPos + len(pattern)
	}
	if int(l) > lastUncovered {
		dif := l - uint64(lastUncovered)
		if res && (lenBuf < int(l) || !bytes.Equal(buf[lastUncovered:l], g.data[postLoopPos:postLoopPos+dif])) {
			res = false
		}
		postLoopPos += dif
	}
	if res && lenBuf != int(l) {
		res = false
	}
	if res {
		g.dataP = postLoopPos
		g.mask = postLoopMask
		g.b = postLoopB
	} else {
		g.dataP = savePos
		g.mask = saveMask
		g.b = saveB
	}
	return res, g.dataP
}

// MatchPrefix only checks if the word at the current offset has a buf prefix. Does not move offset to the next word.
func (g *Getter) MatchPrefix(buf []byte) bool {
	savePos := g.dataP
	defer func() {
		g.dataP = savePos
	}()
	l := g.nextPos(true)
	l-- // because when create huffman tree we do ++ , because 0 is terminator
	if l == 0 {
		return false
	}
	// count available space for word without actual reallocating memory
	wordLen := len(g.word)
	if int(l) > wordLen {
		wordLen = int(l)
	}

	var pos uint64
	var lastPos int
	var lastUncovered int
	var pattern []byte

	for pos = g.nextPos(false /* clean */); pos != 0; pos = g.nextPos(false) {
		intPos := lastPos + int(pos) - 1
		lastPos = intPos
		if wordLen < intPos {
			panic(fmt.Sprintf("likely .idx is invalid: %s", g.fName))
		}
		pattern = g.nextPattern()
		if strings.HasPrefix(string(pattern), string(buf)) {
			return true
		}

		if intPos > lastUncovered {
			dif := uint64(intPos - lastUncovered)
			if strings.HasPrefix(string(pattern)+string(g.data[g.dataP:g.dataP+dif]), string(buf)) {
				return true
			}
		}

		lastUncovered = intPos + len(pattern)
	}

	if int(l) > lastUncovered {
		dif := l - uint64(lastUncovered)
		if strings.HasPrefix(string(pattern)+string(g.data[g.dataP:g.dataP+dif]), string(buf)) {
			return true
		}
	}

	return false
}
