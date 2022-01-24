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

// Decompressor provides access to the superstrings in a file produced by a compressor
type Decompressor struct {
	compressedFile string
	f              *os.File
	mmapHandle1    []byte                 // mmap handle for unix (this is used to close mmap)
	mmapHandle2    *[mmap.MaxMapSize]byte // mmap handle for windows (this is used to close mmap)
	data           []byte                 // slice of correct size for the decompressor to work with
	dict           Dictionary
	posDict        Dictionary
	wordsStart     uint64 // Offset of whether the superstrings actually start
	count          uint64
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
	size := int(stat.Size())
	if size < 24 {
		return nil, fmt.Errorf("compressed file is too short")
	}
	if d.mmapHandle1, d.mmapHandle2, err = mmap.Mmap(d.f, size); err != nil {
		return nil, err
	}
	d.data = d.mmapHandle1[:size]
	d.count = binary.BigEndian.Uint64(d.data[:8])
	dictSize := binary.BigEndian.Uint64(d.data[8:16])
	d.dict.rootOffset = binary.BigEndian.Uint64(d.data[16:24])
	d.dict.cutoff = binary.BigEndian.Uint64(d.data[24:32])
	d.dict.data = d.data[32 : 32+dictSize]
	pos := 32 + dictSize
	dictSize = binary.BigEndian.Uint64(d.data[pos : pos+8])
	d.posDict.rootOffset = binary.BigEndian.Uint64(d.data[pos+8 : pos+16])
	d.posDict.cutoff = binary.BigEndian.Uint64(d.data[pos+16 : pos+24])
	d.posDict.data = d.data[pos+24 : pos+24+dictSize]
	d.wordsStart = pos + 24 + dictSize
	return d, nil
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

type Dictionary struct {
	data       []byte
	rootOffset uint64
	cutoff     uint64
}

type Getter struct {
	data        []byte
	dataP       uint64
	patternDict *Dictionary
	posDict     *Dictionary
	offset      uint64
	b           byte
	mask        byte
	uncovered   []int // Buffer for uncovered portions of the word
	word        []byte
}

func (g *Getter) zero() bool {
	g.offset, _ = binary.Uvarint(g.patternDict.data[g.offset:])
	return g.offset < g.patternDict.cutoff
}

func (g *Getter) one() bool {
	_, n := binary.Uvarint(g.patternDict.data[g.offset:])
	g.offset, _ = binary.Uvarint(g.patternDict.data[g.offset+uint64(n):])
	return g.offset < g.patternDict.cutoff
}

func (g *Getter) posZero() bool {
	g.offset, _ = binary.Uvarint(g.posDict.data[g.offset:])
	return g.offset < g.posDict.cutoff
}

func (g *Getter) posOne() bool {
	_, n := binary.Uvarint(g.posDict.data[g.offset:])
	g.offset, _ = binary.Uvarint(g.posDict.data[g.offset+uint64(n):])
	return g.offset < g.posDict.cutoff
}

func (g *Getter) pattern() []byte {
	l, n := binary.Uvarint(g.patternDict.data[g.offset:])
	return g.patternDict.data[g.offset+uint64(n) : g.offset+uint64(n)+l]
}

func (g *Getter) pos() uint64 {
	pos, _ := binary.Uvarint(g.posDict.data[g.offset:])
	return pos
}

func (g *Getter) nextPos(clean bool) uint64 {
	if clean {
		g.mask = 0
	}
	g.offset = g.posDict.rootOffset
	if g.offset < g.posDict.cutoff {
		return g.pos()
	}
	for {
		if g.mask == 0 {
			g.mask = 1
			g.b = g.data[g.dataP]
			g.dataP++
		}
		if g.b&g.mask == 0 {
			g.mask <<= 1
			if g.posZero() {
				break
			}
		} else {
			g.mask <<= 1
			if g.posOne() {
				break
			}
		}
	}
	return g.pos()
}

func (g *Getter) nextPattern() []byte {
	g.offset = g.patternDict.rootOffset
	if g.offset < g.patternDict.cutoff {
		return g.pattern()
	}

	for {
		if g.mask == 0 {
			g.mask = 1
			g.b = g.data[g.dataP]
			g.dataP++
		}
		if g.b&g.mask == 0 {
			g.mask <<= 1
			if g.zero() {
				break
			}
		} else {
			g.mask <<= 1
			if g.one() {
				break
			}
		}
	}
	return g.pattern()
}

func (d *Decompressor) Count() int { return int(d.count) }

// MakeGetter creates an object that can be used to access superstrings in the decompressor's file
// Getter is not thread-safe, but there can be multiple getters used simultaneously and concurrently
// for the same decompressor
func (d *Decompressor) MakeGetter() *Getter {
	return &Getter{patternDict: &d.dict, posDict: &d.posDict, data: d.data[d.wordsStart:], uncovered: make([]int, 0, 128)}
}

func (g *Getter) Reset(offset uint64) {
	g.dataP = offset
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
			panic("likely .idx is invalid")
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
			panic("likely .idx is invalid")
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
	l := g.nextPos(true)
	l-- // because when create huffman tree we do ++ , because 0 is terminator
	if l == 0 {
		return false, g.dataP
	}
	// count available space for word without actual reallocating memory
	wordLen := len(g.word)
	if int(l) > wordLen {
		wordLen = int(l)
	}

	var add uint64
	var pos uint64
	var lastPos int
	var lastUncovered int
	var pattern []byte
	res := true
	for pos = g.nextPos(false /* clean */); pos != 0; pos = g.nextPos(false) {
		intPos := lastPos + int(pos) - 1
		lastPos = intPos
		if wordLen < intPos {
			panic("likely .idx is invalid")
		}
		pattern = g.nextPattern()

		if len(buf) < len(pattern) || !bytes.Equal(buf[:len(pattern)], pattern) {
			res = false
		}
		if intPos > lastUncovered {
			dif := uint64(intPos - lastUncovered)
			add += dif
			if res && !bytes.Equal(buf[len(pattern):], g.data[g.dataP:g.dataP+dif]) {
				res = false
			}
		}

		lastUncovered = intPos + len(pattern)
	}
	if int(l) > lastUncovered {
		dif := l - uint64(lastUncovered)
		add += dif
		if res && !bytes.Equal(buf[len(pattern):], g.data[g.dataP:g.dataP+dif]) {
			res = false
		}
	}
	g.dataP += add
	if !res {
		g.dataP = savePos
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
			panic("likely .idx is invalid")
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
