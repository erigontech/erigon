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
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/flanglet/kanzi-go/transform"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/patricia"
)

const ASSERT = false

// Compressor is the main operating type for performing per-word compression
// After creating a compression, one needs to add words to it, using `AddWord` function
// After that, `Compress` function needs to be called to perform the compression
// and eventually create output file
type Compressor struct {
	outputFile      string // File where to output the dictionary and compressed data
	tmpDir          string // temporary directory to use for ETL when building dictionary
	minPatternScore uint64 //minimum score (per superstring) required to consider including pattern into the dictionary
	// Buffer for "superstring" - transformation of words where each byte of a word, say b,
	// is turned into 2 bytes, 0x01 and b, and two zero bytes 0x00 0x00 are inserted after each word
	// this is needed for using ordinary (one string) suffix sorting algorithm instead of a generalised (many words) suffix
	// sorting algorithm
	superstring []byte
	divsufsort  *transform.DivSufSort       // Instance of DivSufSort - algorithm for building suffix array for the superstring
	suffixarray []int32                     // Suffix array - output for divsufsort algorithm
	lcp         []int32                     // LCP array (Longest Common Prefix)
	collector   *etl.Collector              // Collector used to handle very large sets of words
	numBuf      [binary.MaxVarintLen64]byte // Buffer for producing var int serialisation
	collectBuf  []byte                      // Buffer for forming key to call collector
	dictBuilder DictionaryBuilder           // Priority queue that selects dictionary patterns with highest scores, and then sorts them by scores
	pt          patricia.PatriciaTree       // Patricia tree of dictionary patterns
	mf          patricia.MatchFinder        // Match finder to use together with patricia tree (it stores search context and buffers matches)
	ring        *Ring                       // Cycling ring for dynamic programming algorithm determining optimal coverage of word by dictionary patterns
	wordFile    *os.File                    // Temporary file to keep words in for the second pass
	wordW       *bufio.Writer               // Bufferred writer for temporary file
	interFile   *os.File                    // File to write intermediate compression to
	interW      *bufio.Writer               // Buffered writer associate to interFile
	patterns    []int                       // Buffer of pattern ids (used in the dynamic programming algorithm to remember patterns corresponding to dynamic cells)
	uncovered   []int                       // Buffer of intervals that are not covered by patterns
	posMap      map[uint64]uint64           // Counter of use for each position within compressed word (for building huffman code for positions)

	wordsCount uint64
}

type Phase int

const (
	BuildDict Phase = iota
	Compress
)

// superstringLimit limits how large can one "superstring" get before it is processed
// Compressor allocates 7 bytes for each uint of superstringLimit. For example,
// superstingLimit 16m will result in 112Mb being allocated for various arrays
const superstringLimit = 16 * 1024 * 1024

// minPatternLen is minimum length of pattern we consider to be included into the dictionary
const minPatternLen = 5
const maxPatternLen = 64

// maxDictPatterns is the maximum number of patterns allowed in the initial (not reduced dictionary)
// Large values increase memory consumption of dictionary reduction phase
const maxDictPatterns = 2 * 1024 * 1024

//nolint
const compressLogPrefix = "compress"

type DictionaryBuilder struct {
	limit         int
	lastWord      []byte
	lastWordScore uint64
	items         []*Pattern
}

func (db *DictionaryBuilder) Reset(limit int) {
	db.limit = limit
	db.items = db.items[:0]
}

func (db DictionaryBuilder) Len() int {
	return len(db.items)
}

func (db DictionaryBuilder) Less(i, j int) bool {
	if db.items[i].score == db.items[j].score {
		return bytes.Compare(db.items[i].word, db.items[j].word) < 0
	}
	return db.items[i].score < db.items[j].score
}

func (db *DictionaryBuilder) Swap(i, j int) {
	db.items[i], db.items[j] = db.items[j], db.items[i]
}

func (db *DictionaryBuilder) Push(x interface{}) {
	db.items = append(db.items, x.(*Pattern))
}

func (db *DictionaryBuilder) Pop() interface{} {
	old := db.items
	n := len(old)
	x := old[n-1]
	db.items = old[0 : n-1]
	return x
}

func (db *DictionaryBuilder) processWord(chars []byte, score uint64) {
	heap.Push(db, &Pattern{word: common.Copy(chars), score: score})
	if db.Len() > db.limit {
		// Remove the element with smallest score
		heap.Pop(db)
	}
}

func (db *DictionaryBuilder) loadFunc(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
	score := binary.BigEndian.Uint64(v)
	if bytes.Equal(k, db.lastWord) {
		db.lastWordScore += score
	} else {
		if db.lastWord != nil {
			db.processWord(db.lastWord, db.lastWordScore)
		}
		db.lastWord = append(db.lastWord[:0], k...)
		db.lastWordScore = score
	}
	return nil
}

func (db *DictionaryBuilder) finish() {
	if db.lastWord != nil {
		db.processWord(db.lastWord, db.lastWordScore)
	}
}

func (db *DictionaryBuilder) ForEach(f func(score uint64, word []byte)) {
	for i := db.Len(); i > 0; i-- {
		f(db.items[i-1].score, db.items[i-1].word)
	}
}

// Pattern is representation of a pattern that is searched in the words to compress them
// patterns are stored in a patricia tree and contain pattern score (calculated during
// the initial dictionary building), frequency of usage, and code
type Pattern struct {
	score    uint64 // Score assigned to the pattern during dictionary building
	uses     uint64 // How many times this pattern has been used during search and optimisation
	code     uint64 // Allocated numerical code
	codeBits int    // Number of bits in the code
	word     []byte // Pattern characters
	offset   uint64 // Offset of this patten in the dictionary representation
}

// PatternList is a sorted list of pattern for the purpose of
// building Huffman tree to determine efficient coding.
// Patterns with least usage come first, we use numerical code
// as a tie breaker to make sure the resulting Huffman code is canonical
type PatternList []*Pattern

func (pl PatternList) Len() int {
	return len(pl)
}

func (pl PatternList) Less(i, j int) bool {
	if pl[i].uses == pl[j].uses {
		return pl[i].code < pl[j].code
	}
	return pl[i].uses < pl[j].uses
}

func (pl *PatternList) Swap(i, j int) {
	(*pl)[i], (*pl)[j] = (*pl)[j], (*pl)[i]
}

// PatternHuff is an intermediate node in a huffman tree of patterns
// It has two children, each of which may either be another intermediate node (h0 or h1)
// or leaf node, which is Pattern (p0 or p1).
type PatternHuff struct {
	uses       uint64
	tieBreaker uint64
	p0, p1     *Pattern
	h0, h1     *PatternHuff
	offset     uint64 // Offset of this huffman tree node in the dictionary representation
}

func (h *PatternHuff) AddZero() {
	if h.p0 != nil {
		h.p0.code <<= 1
		h.p0.codeBits++
	} else {
		h.h0.AddZero()
	}
	if h.p1 != nil {
		h.p1.code <<= 1
		h.p1.codeBits++
	} else {
		h.h1.AddZero()
	}
}

func (h *PatternHuff) AddOne() {
	if h.p0 != nil {
		h.p0.code <<= 1
		h.p0.code++
		h.p0.codeBits++
	} else {
		h.h0.AddOne()
	}
	if h.p1 != nil {
		h.p1.code <<= 1
		h.p1.code++
		h.p1.codeBits++
	} else {
		h.h1.AddOne()
	}
}

// PatternHeap is priority queue of pattern for the purpose of building
// Huffman tree to determine efficient coding. Patterns with least usage
// have highest priority. We use a tie-breaker to make sure
// the resulting Huffman code is canonical
type PatternHeap []*PatternHuff

func (ph PatternHeap) Len() int {
	return len(ph)
}

func (ph PatternHeap) Less(i, j int) bool {
	if ph[i].uses == ph[j].uses {
		return ph[i].tieBreaker < ph[j].tieBreaker
	}
	return ph[i].uses < ph[j].uses
}

func (ph *PatternHeap) Swap(i, j int) {
	(*ph)[i], (*ph)[j] = (*ph)[j], (*ph)[i]
}

func (ph *PatternHeap) Push(x interface{}) {
	*ph = append(*ph, x.(*PatternHuff))
}

func (ph *PatternHeap) Pop() interface{} {
	old := *ph
	n := len(old)
	x := old[n-1]
	*ph = old[0 : n-1]
	return x
}

type Position struct {
	pos      uint64
	uses     uint64
	code     uint64
	codeBits int
	offset   uint64
}

type PositionHuff struct {
	uses       uint64
	tieBreaker uint64
	p0, p1     *Position
	h0, h1     *PositionHuff
	offset     uint64
}

func (h *PositionHuff) AddZero() {
	if h.p0 != nil {
		h.p0.code <<= 1
		h.p0.codeBits++
	} else {
		h.h0.AddZero()
	}
	if h.p1 != nil {
		h.p1.code <<= 1
		h.p1.codeBits++
	} else {
		h.h1.AddZero()
	}
}

func (h *PositionHuff) AddOne() {
	if h.p0 != nil {
		h.p0.code <<= 1
		h.p0.code++
		h.p0.codeBits++
	} else {
		h.h0.AddOne()
	}
	if h.p1 != nil {
		h.p1.code <<= 1
		h.p1.code++
		h.p1.codeBits++
	} else {
		h.h1.AddOne()
	}
}

type PositionList []*Position

func (pl PositionList) Len() int {
	return len(pl)
}

func (pl PositionList) Less(i, j int) bool {
	if pl[i].uses == pl[j].uses {
		return pl[i].pos < pl[j].pos
	}
	return pl[i].uses < pl[j].uses
}

func (pl *PositionList) Swap(i, j int) {
	(*pl)[i], (*pl)[j] = (*pl)[j], (*pl)[i]
}

type PositionHeap []*PositionHuff

func (ph PositionHeap) Len() int {
	return len(ph)
}

func (ph PositionHeap) Less(i, j int) bool {
	if ph[i].uses == ph[j].uses {
		return ph[i].tieBreaker < ph[j].tieBreaker
	}
	return ph[i].uses < ph[j].uses
}

func (ph *PositionHeap) Swap(i, j int) {
	(*ph)[i], (*ph)[j] = (*ph)[j], (*ph)[i]
}

func (ph *PositionHeap) Push(x interface{}) {
	*ph = append(*ph, x.(*PositionHuff))
}

func (ph *PositionHeap) Pop() interface{} {
	old := *ph
	n := len(old)
	x := old[n-1]
	*ph = old[0 : n-1]
	return x
}

type HuffmanCoder struct {
	w          *bufio.Writer
	outputBits int
	outputByte byte
}

func (hf *HuffmanCoder) encode(code uint64, codeBits int) error {
	for codeBits > 0 {
		var bitsUsed int
		if hf.outputBits+codeBits > 8 {
			bitsUsed = 8 - hf.outputBits
		} else {
			bitsUsed = codeBits
		}
		mask := (uint64(1) << bitsUsed) - 1
		hf.outputByte |= byte((code & mask) << hf.outputBits)
		code >>= bitsUsed
		codeBits -= bitsUsed
		hf.outputBits += bitsUsed
		if hf.outputBits == 8 {
			if e := hf.w.WriteByte(hf.outputByte); e != nil {
				return e
			}
			hf.outputBits = 0
			hf.outputByte = 0
		}
	}
	return nil
}

func (hf *HuffmanCoder) flush() error {
	if hf.outputBits > 0 {
		if e := hf.w.WriteByte(hf.outputByte); e != nil {
			return e
		}
		hf.outputBits = 0
		hf.outputByte = 0
	}
	return nil
}

// DynamicCell represents result of dynamic programming for certain starting position
type DynamicCell struct {
	optimStart  int
	coverStart  int
	compression int
	score       uint64
	patternIdx  int // offset of the last element in the pattern slice
}

type Ring struct {
	cells             []DynamicCell
	head, tail, count int
}

func NewRing() *Ring {
	return &Ring{
		cells: make([]DynamicCell, 16),
		head:  0,
		tail:  0,
		count: 0,
	}
}

func (r *Ring) Reset() {
	r.count = 0
	r.head = 0
	r.tail = 0
}

func (r *Ring) ensureSize() {
	if r.count < len(r.cells) {
		return
	}
	newcells := make([]DynamicCell, r.count*2)
	if r.tail > r.head {
		copy(newcells, r.cells[r.head:r.tail])
	} else {
		n := copy(newcells, r.cells[r.head:])
		copy(newcells[n:], r.cells[:r.tail])
	}
	r.head = 0
	r.tail = r.count
	r.cells = newcells
}

func (r *Ring) PushFront() *DynamicCell {
	r.ensureSize()
	if r.head == 0 {
		r.head = len(r.cells)
	}
	r.head--
	r.count++
	return &r.cells[r.head]
}

func (r *Ring) PushBack() *DynamicCell {
	r.ensureSize()
	if r.tail == len(r.cells) {
		r.tail = 0
	}
	result := &r.cells[r.tail]
	r.tail++
	r.count++
	return result
}

func (r Ring) Len() int {
	return r.count
}

func (r *Ring) Get(i int) *DynamicCell {
	if i < 0 || i >= r.count {
		return nil
	}
	return &r.cells[(r.head+i)&(len(r.cells)-1)]
}

// Truncate removes all items starting from i
func (r *Ring) Truncate(i int) {
	r.count = i
	r.tail = (r.head + i) & (len(r.cells) - 1)
}

func NewCompressor(logPrefix, outputFile string, tmpDir string, minPatternScore uint64) (*Compressor, error) {
	c := &Compressor{
		minPatternScore: minPatternScore,
		outputFile:      outputFile,
		tmpDir:          tmpDir,
		superstring:     make([]byte, 0, superstringLimit), // Allocate enough, so we never need to resize
		suffixarray:     make([]int32, superstringLimit),
		lcp:             make([]int32, superstringLimit/2),
		collectBuf:      make([]byte, 8, 256),
		ring:            NewRing(),
		patterns:        make([]int, 0, 32),
		uncovered:       make([]int, 0, 32),
		posMap:          make(map[uint64]uint64),
	}
	var err error
	if c.divsufsort, err = transform.NewDivSufSort(); err != nil {
		return nil, err
	}
	if c.wordFile, err = ioutil.TempFile(c.tmpDir, "words-"); err != nil {
		return nil, err
	}
	c.wordW = bufio.NewWriterSize(c.wordFile, etl.BufIOSize)
	c.collector = etl.NewCollector(logPrefix, tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	return c, nil
}

// AddWord needs to be called repeatedly to provide all the words to compress
func (c *Compressor) AddWord(word []byte) error {
	c.wordsCount++
	if len(c.superstring)+2*len(word)+2 > superstringLimit {
		// Adding this word would make superstring go over the limit
		if err := c.processSuperstring(); err != nil {
			return fmt.Errorf("buildDictNextWord: error processing superstring: %w", err)
		}
	}
	for _, b := range word {
		c.superstring = append(c.superstring, 1, b)
	}
	c.superstring = append(c.superstring, 0, 0)
	n := binary.PutUvarint(c.numBuf[:], uint64(len(word)))
	if _, err := c.wordW.Write(c.numBuf[:n]); err != nil {
		return err
	}
	if len(word) > 0 {
		if _, err := c.wordW.Write(word); err != nil {
			return err
		}
	}
	return nil
}

func (c *Compressor) Compress() error {
	if c.wordW != nil {
		if err := c.wordW.Flush(); err != nil {
			return err
		}
	}
	if err := c.buildDictionary(); err != nil {
		return err
	}
	if err := c.findMatches(); err != nil {
		return err
	}
	if err := c.optimiseCodes(); err != nil {
		return err
	}
	return nil
}

func (c *Compressor) Close() {
	c.collector.Close()
	c.wordFile.Close()
	c.interFile.Close()
}

func (c *Compressor) findMatches() error {
	// Build patricia tree out of the patterns in the dictionary, for further matching in individual words
	// Allocate temporary initial codes to the patterns so that patterns with higher scores get smaller code
	// This helps reduce the size of intermediate compression
	for i, p := range c.dictBuilder.items {
		p.code = uint64(len(c.dictBuilder.items) - i - 1)
		c.pt.Insert(p.word, p)
	}
	var err error
	if c.interFile, err = ioutil.TempFile(c.tmpDir, "inter-compress-"); err != nil {
		return err
	}
	c.interW = bufio.NewWriterSize(c.interFile, etl.BufIOSize)
	if _, err := c.wordFile.Seek(0, 0); err != nil {
		return err
	}
	defer os.Remove(c.wordFile.Name())
	defer c.wordFile.Close()
	r := bufio.NewReaderSize(c.wordFile, etl.BufIOSize)
	var readBuf []byte
	l, e := binary.ReadUvarint(r)
	for ; e == nil; l, e = binary.ReadUvarint(r) {
		c.posMap[l+1]++
		c.posMap[0]++
		if int(l) > len(readBuf) {
			readBuf = make([]byte, l)
		}
		if _, e := io.ReadFull(r, readBuf[:l]); e != nil {
			return e
		}
		word := readBuf[:l]
		// Encode length of the word as var int for the intermediate compression
		n := binary.PutUvarint(c.numBuf[:], uint64(len(word)))
		if _, err := c.interW.Write(c.numBuf[:n]); err != nil {
			return err
		}
		if len(word) > 0 {
			matches := c.mf.FindLongestMatches(&c.pt, word)
			if len(matches) == 0 {
				n = binary.PutUvarint(c.numBuf[:], 0)
				if _, err := c.interW.Write(c.numBuf[:n]); err != nil {
					return err
				}
				if _, err := c.interW.Write(word); err != nil {
					return err
				}
				continue
			}
			c.ring.Reset()
			c.patterns = append(c.patterns[:0], 0, 0) // Sentinel entry - no meaning
			lastF := matches[len(matches)-1]
			for j := lastF.Start; j < lastF.End; j++ {
				d := c.ring.PushBack()
				d.optimStart = j + 1
				d.coverStart = len(word)
				d.compression = 0
				d.patternIdx = 0
				d.score = 0
			}
			// Starting from the last match
			for i := len(matches); i > 0; i-- {
				f := matches[i-1]
				p := f.Val.(*Pattern)
				firstCell := c.ring.Get(0)
				maxCompression := firstCell.compression
				maxScore := firstCell.score
				maxCell := firstCell
				var maxInclude bool
				for e := 0; e < c.ring.Len(); e++ {
					cell := c.ring.Get(e)
					comp := cell.compression - 4
					if cell.coverStart >= f.End {
						comp += f.End - f.Start
					} else {
						comp += cell.coverStart - f.Start
					}
					score := cell.score + p.score
					if comp > maxCompression || (comp == maxCompression && score > maxScore) {
						maxCompression = comp
						maxScore = score
						maxInclude = true
						maxCell = cell
					}
					if cell.optimStart > f.End {
						c.ring.Truncate(e)
						break
					}
				}
				d := c.ring.PushFront()
				d.optimStart = f.Start
				d.score = maxScore
				d.compression = maxCompression
				if maxInclude {
					d.coverStart = f.Start
					d.patternIdx = len(c.patterns)
					c.patterns = append(c.patterns, i-1, maxCell.patternIdx)
				} else {
					d.coverStart = maxCell.coverStart
					d.patternIdx = maxCell.patternIdx
				}
			}
			optimCell := c.ring.Get(0)
			// Count number of patterns
			var patternCount uint64
			patternIdx := optimCell.patternIdx
			for patternIdx != 0 {
				patternCount++
				patternIdx = c.patterns[patternIdx+1]
			}
			n = binary.PutUvarint(c.numBuf[:], patternCount)
			if _, err := c.interW.Write(c.numBuf[:n]); err != nil {
				return err
			}
			patternIdx = optimCell.patternIdx
			lastStart := 0
			var lastUncovered int
			c.uncovered = c.uncovered[:0]
			for patternIdx != 0 {
				pattern := c.patterns[patternIdx]
				p := matches[pattern].Val.(*Pattern)
				if matches[pattern].Start > lastUncovered {
					c.uncovered = append(c.uncovered, lastUncovered, matches[pattern].Start)
				}
				lastUncovered = matches[pattern].End
				// Starting position
				c.posMap[uint64(matches[pattern].Start-lastStart+1)]++
				lastStart = matches[pattern].Start
				n = binary.PutUvarint(c.numBuf[:], uint64(matches[pattern].Start))
				if _, err := c.interW.Write(c.numBuf[:n]); err != nil {
					return err
				}
				// Code
				n = binary.PutUvarint(c.numBuf[:], p.code)
				if _, err := c.interW.Write(c.numBuf[:n]); err != nil {
					return err
				}
				p.uses++
				patternIdx = c.patterns[patternIdx+1]
			}
			if len(word) > lastUncovered {
				c.uncovered = append(c.uncovered, lastUncovered, len(word))
			}
			// Add uncoded input
			for i := 0; i < len(c.uncovered); i += 2 {
				if _, err := c.interW.Write(word[c.uncovered[i]:c.uncovered[i+1]]); err != nil {
					return err
				}
			}
		}
	}
	if e != nil && !errors.Is(e, io.EOF) {
		return e
	}
	if err = c.interW.Flush(); err != nil {
		return err
	}
	return nil
}

// optimises coding for patterns and positions
func (c *Compressor) optimiseCodes() error {
	if _, err := c.interFile.Seek(0, 0); err != nil {
		return err
	}
	defer os.Remove(c.interFile.Name())
	defer c.interFile.Close()
	// Select patterns with non-zero use and sort them by increasing frequency of use (in preparation for building Huffman tree)
	var patternList PatternList
	for _, p := range c.dictBuilder.items {
		if p.uses > 0 {
			patternList = append(patternList, p)
		}
	}
	sort.Sort(&patternList)
	// Calculate offsets of the dictionary patterns and total size
	var offset uint64
	for _, p := range patternList {
		p.offset = offset
		n := binary.PutUvarint(c.numBuf[:], uint64(len(p.word)))
		offset += uint64(n + len(p.word))
	}
	patternCutoff := offset // All offsets below this will be considered patterns
	i := 0                  // Will be going over the patternList
	// Build Huffman tree for codes
	var codeHeap PatternHeap
	heap.Init(&codeHeap)
	tieBreaker := uint64(0)
	var huffs []*PatternHuff // To be used to output dictionary
	for codeHeap.Len()+(patternList.Len()-i) > 1 {
		// New node
		h := &PatternHuff{
			tieBreaker: tieBreaker,
			offset:     offset,
		}
		if codeHeap.Len() > 0 && (i >= patternList.Len() || codeHeap[0].uses < patternList[i].uses) {
			// Take h0 from the heap
			h.h0 = heap.Pop(&codeHeap).(*PatternHuff)
			h.h0.AddZero()
			h.uses += h.h0.uses
			n := binary.PutUvarint(c.numBuf[:], h.h0.offset)
			offset += uint64(n)
		} else {
			// Take p0 from the list
			h.p0 = patternList[i]
			h.p0.code = 0
			h.p0.codeBits = 1
			h.uses += h.p0.uses
			n := binary.PutUvarint(c.numBuf[:], h.p0.offset)
			offset += uint64(n)
			i++
		}
		if codeHeap.Len() > 0 && (i >= patternList.Len() || codeHeap[0].uses < patternList[i].uses) {
			// Take h1 from the heap
			h.h1 = heap.Pop(&codeHeap).(*PatternHuff)
			h.h1.AddOne()
			h.uses += h.h1.uses
			n := binary.PutUvarint(c.numBuf[:], h.h1.offset)
			offset += uint64(n)
		} else {
			// Take p1 from the list
			h.p1 = patternList[i]
			h.p1.code = 1
			h.p1.codeBits = 1
			h.uses += h.p1.uses
			n := binary.PutUvarint(c.numBuf[:], h.p1.offset)
			offset += uint64(n)
			i++
		}
		tieBreaker++
		heap.Push(&codeHeap, h)
		huffs = append(huffs, h)
	}
	var root *PatternHuff
	if codeHeap.Len() > 0 {
		root = heap.Pop(&codeHeap).(*PatternHuff) // Root node of huffman tree
	}

	// Start writing to result file
	cf, err := os.Create(c.outputFile)
	if err != nil {
		return err
	}
	defer cf.Close()
	defer cf.Sync()
	cw := bufio.NewWriterSize(cf, etl.BufIOSize)
	defer cw.Flush()
	// 1-st, output amount of words in file
	binary.BigEndian.PutUint64(c.numBuf[:], c.wordsCount)
	if _, err = cw.Write(c.numBuf[:8]); err != nil {
		return err
	}
	// 2-nd, output dictionary size
	binary.BigEndian.PutUint64(c.numBuf[:], offset) // Dictionary size
	if _, err = cw.Write(c.numBuf[:8]); err != nil {
		return err
	}
	// 3-rd, output directory root
	if root == nil {
		binary.BigEndian.PutUint64(c.numBuf[:], 0)
	} else {
		binary.BigEndian.PutUint64(c.numBuf[:], root.offset)
	}
	if _, err = cw.Write(c.numBuf[:8]); err != nil {
		return err
	}
	// 4-th, output pattern cutoff offset
	binary.BigEndian.PutUint64(c.numBuf[:], patternCutoff)
	if _, err = cw.Write(c.numBuf[:8]); err != nil {
		return err
	}
	// Write all the pattens
	for _, p := range patternList {
		n := binary.PutUvarint(c.numBuf[:], uint64(len(p.word)))
		if _, err = cw.Write(c.numBuf[:n]); err != nil {
			return err
		}
		if _, err = cw.Write(p.word); err != nil {
			return err
		}
	}
	// Write all the huffman nodes
	for _, h := range huffs {
		var n int
		if h.h0 != nil {
			n = binary.PutUvarint(c.numBuf[:], h.h0.offset)
		} else {
			n = binary.PutUvarint(c.numBuf[:], h.p0.offset)
		}
		if _, err = cw.Write(c.numBuf[:n]); err != nil {
			return err
		}
		if h.h1 != nil {
			n = binary.PutUvarint(c.numBuf[:], h.h1.offset)
		} else {
			n = binary.PutUvarint(c.numBuf[:], h.p1.offset)
		}
		if _, err = cw.Write(c.numBuf[:n]); err != nil {
			return err
		}
	}
	var positionList PositionList
	pos2code := make(map[uint64]*Position)
	for pos, uses := range c.posMap {
		p := &Position{pos: pos, uses: uses, code: 0, codeBits: 0, offset: 0}
		positionList = append(positionList, p)
		pos2code[pos] = p
	}
	sort.Sort(&positionList)
	// Calculate offsets of the dictionary positions and total size
	offset = 0
	for _, p := range positionList {
		p.offset = offset
		n := binary.PutUvarint(c.numBuf[:], p.pos)
		offset += uint64(n)
	}
	positionCutoff := offset // All offsets below this will be considered positions
	i = 0                    // Will be going over the positionList
	// Build Huffman tree for codes
	var posHeap PositionHeap
	heap.Init(&posHeap)
	tieBreaker = uint64(0)
	var posHuffs []*PositionHuff // To be used to output dictionary
	for posHeap.Len()+(positionList.Len()-i) > 1 {
		// New node
		h := &PositionHuff{
			tieBreaker: tieBreaker,
			offset:     offset,
		}
		if posHeap.Len() > 0 && (i >= positionList.Len() || posHeap[0].uses < positionList[i].uses) {
			// Take h0 from the heap
			h.h0 = heap.Pop(&posHeap).(*PositionHuff)
			h.h0.AddZero()
			h.uses += h.h0.uses
			n := binary.PutUvarint(c.numBuf[:], h.h0.offset)
			offset += uint64(n)
		} else {
			// Take p0 from the list
			h.p0 = positionList[i]
			h.p0.code = 0
			h.p0.codeBits = 1
			h.uses += h.p0.uses
			n := binary.PutUvarint(c.numBuf[:], h.p0.offset)
			offset += uint64(n)
			i++
		}
		if posHeap.Len() > 0 && (i >= positionList.Len() || posHeap[0].uses < positionList[i].uses) {
			// Take h1 from the heap
			h.h1 = heap.Pop(&posHeap).(*PositionHuff)
			h.h1.AddOne()
			h.uses += h.h1.uses
			n := binary.PutUvarint(c.numBuf[:], h.h1.offset)
			offset += uint64(n)
		} else {
			// Take p1 from the list
			h.p1 = positionList[i]
			h.p1.code = 1
			h.p1.codeBits = 1
			h.uses += h.p1.uses
			n := binary.PutUvarint(c.numBuf[:], h.p1.offset)
			offset += uint64(n)
			i++
		}
		tieBreaker++
		heap.Push(&posHeap, h)
		posHuffs = append(posHuffs, h)
	}
	var posRoot *PositionHuff
	if posHeap.Len() > 0 {
		posRoot = heap.Pop(&posHeap).(*PositionHuff)
	}
	// First, output dictionary size
	binary.BigEndian.PutUint64(c.numBuf[:], offset) // Dictionary size
	if _, err = cw.Write(c.numBuf[:8]); err != nil {
		return err
	}
	// Secondly, output directory root
	if posRoot == nil {
		binary.BigEndian.PutUint64(c.numBuf[:], 0)
	} else {
		binary.BigEndian.PutUint64(c.numBuf[:], posRoot.offset)
	}
	if _, err = cw.Write(c.numBuf[:8]); err != nil {
		return err
	}
	// Thirdly, output pattern cutoff offset
	binary.BigEndian.PutUint64(c.numBuf[:], positionCutoff)
	if _, err = cw.Write(c.numBuf[:8]); err != nil {
		return err
	}
	// Write all the positions
	for _, p := range positionList {
		n := binary.PutUvarint(c.numBuf[:], p.pos)
		if _, err = cw.Write(c.numBuf[:n]); err != nil {
			return err
		}
	}
	// Write all the huffman nodes
	for _, h := range posHuffs {
		var n int
		if h.h0 != nil {
			n = binary.PutUvarint(c.numBuf[:], h.h0.offset)
		} else {
			n = binary.PutUvarint(c.numBuf[:], h.p0.offset)
		}
		if _, err = cw.Write(c.numBuf[:n]); err != nil {
			return err
		}
		if h.h1 != nil {
			n = binary.PutUvarint(c.numBuf[:], h.h1.offset)
		} else {
			n = binary.PutUvarint(c.numBuf[:], h.p1.offset)
		}
		if _, err = cw.Write(c.numBuf[:n]); err != nil {
			return err
		}
	}
	r := bufio.NewReaderSize(c.interFile, etl.BufIOSize)
	var hc HuffmanCoder
	hc.w = cw
	l, e := binary.ReadUvarint(r)
	for ; e == nil; l, e = binary.ReadUvarint(r) {
		posCode := pos2code[l+1]
		if posCode != nil {
			if e = hc.encode(posCode.code, posCode.codeBits); e != nil {
				return e
			}
		}
		if l == 0 {
			if e = hc.flush(); e != nil {
				return e
			}
		} else {
			var pNum uint64 // Number of patterns
			if pNum, e = binary.ReadUvarint(r); e != nil {
				return e
			}
			// Now reading patterns one by one
			var lastPos uint64
			var lastUncovered int
			var uncoveredCount int
			for i := 0; i < int(pNum); i++ {
				var pos uint64 // Starting position for pattern
				if pos, e = binary.ReadUvarint(r); e != nil {
					return e
				}
				posCode = pos2code[pos-lastPos+1]
				lastPos = pos
				if posCode != nil {
					if e = hc.encode(posCode.code, posCode.codeBits); e != nil {
						return e
					}
				}
				var code uint64 // Code of the pattern
				if code, e = binary.ReadUvarint(r); e != nil {
					return e
				}
				patternCode := c.dictBuilder.items[len(c.dictBuilder.items)-1-int(code)]
				if int(pos) > lastUncovered {
					uncoveredCount += int(pos) - lastUncovered
				}
				lastUncovered = int(pos) + len(patternCode.word)
				if e = hc.encode(patternCode.code, patternCode.codeBits); e != nil {
					return e
				}
			}
			if int(l) > lastUncovered {
				uncoveredCount += int(l) - lastUncovered
			}
			// Terminating position and flush
			posCode = pos2code[0]
			if posCode != nil {
				if e = hc.encode(posCode.code, posCode.codeBits); e != nil {
					return e
				}
			}
			if e = hc.flush(); e != nil {
				return e
			}
			// Copy uncovered characters
			if uncoveredCount > 0 {
				if _, e = io.CopyN(cw, r, int64(uncoveredCount)); e != nil {
					return e
				}
			}
		}
	}
	if e != nil && !errors.Is(e, io.EOF) {
		return e
	}
	return nil
}

func (c *Compressor) buildDictionary() error {
	if len(c.superstring) > 0 {
		// Process any residual words
		if err := c.processSuperstring(); err != nil {
			return fmt.Errorf("buildDictionary: error processing superstring: %w", err)
		}
	}
	c.dictBuilder.Reset(maxDictPatterns)
	if err := c.collector.Load(nil, "", c.dictBuilder.loadFunc, etl.TransformArgs{}); err != nil {
		return err
	}
	c.dictBuilder.finish()
	c.collector.Close()
	// Sort dictionary inside the dictionary bilder in the order of increasing scores
	sort.Sort(&c.dictBuilder)
	return nil
}

func (c *Compressor) processSuperstring() error {
	c.divsufsort.ComputeSuffixArray(c.superstring, c.suffixarray[:len(c.superstring)])
	// filter out suffixes that start with odd positions - we reuse the first half of sa.suffixarray for that
	// because it won't be used after filtration
	n := len(c.superstring) / 2
	saFiltered := c.suffixarray[:n]
	j := 0
	for _, s := range c.suffixarray[:len(c.superstring)] {
		if (s & 1) == 0 {
			saFiltered[j] = s >> 1
			j++
		}
	}
	// Now create an inverted array - we reuse the second half of sa.suffixarray for that
	saInverted := c.suffixarray[:n]
	for i := 0; i < n; i++ {
		saInverted[saFiltered[i]] = int32(i)
	}
	// Create LCP array (Kasai's algorithm)
	var k int
	// Process all suffixes one by one starting from
	// first suffix in superstring
	for i := 0; i < n; i++ {
		/* If the current suffix is at n-1, then we don’t
		   have next substring to consider. So lcp is not
		   defined for this substring, we put zero. */
		if saInverted[i] == int32(n-1) {
			k = 0
			continue
		}

		/* j contains index of the next substring to
		   be considered  to compare with the present
		   substring, i.e., next string in suffix array */
		j := int(saFiltered[saInverted[i]+1])

		// Directly start matching from k'th index as
		// at-least k-1 characters will match
		for i+k < n && j+k < n && c.superstring[(i+k)*2] != 0 && c.superstring[(j+k)*2] != 0 && c.superstring[(i+k)*2+1] == c.superstring[(j+k)*2+1] {
			k++
		}

		c.lcp[saInverted[i]] = int32(k) // lcp for the present suffix.

		// Deleting the starting character from the string.
		if k > 0 {
			k--
		}
	}
	// Walk over LCP array and compute the scores of the strings
	b := saInverted
	j = 0
	for i := 0; i < n-1; i++ {
		// Only when there is a drop in LCP value
		if c.lcp[i+1] >= c.lcp[i] {
			j = i
			continue
		}
		for l := c.lcp[i]; l > c.lcp[i+1]; l-- {
			if l < minPatternLen || l > maxPatternLen {
				continue
			}
			// Go back
			var new bool
			for j > 0 && c.lcp[j-1] >= l {
				j--
				new = true
			}
			if !new {
				break
			}
			window := i - j + 2
			copy(b, saFiltered[j:i+2])
			sort.Slice(b[:window], func(i1, i2 int) bool { return b[i1] < b[i2] })
			repeats := 1
			lastK := 0
			for k := 1; k < window; k++ {
				if b[k] >= b[lastK]+l {
					repeats++
					lastK = k
				}
			}
			score := uint64(repeats * int(l))
			if score >= c.minPatternScore {
				// Dictionary key is the concatenation of the score and the dictionary word (to later aggregate the scores from multiple chunks)
				c.collectBuf = c.collectBuf[:8]
				for s := int32(0); s < l; s++ {
					c.collectBuf = append(c.collectBuf, c.superstring[(saFiltered[i]+s)*2+1])
				}
				binary.BigEndian.PutUint64(c.collectBuf[:8], score)
				if err := c.collector.Collect(c.collectBuf[8:], c.collectBuf[:8]); err != nil { // key will be copied by Collect function
					return fmt.Errorf("collecting %x with score %d: %w", c.collectBuf[8:], score, err)
				}
			}
		}
	}
	c.superstring = c.superstring[:0]
	return nil
}

type DictAggregator struct {
	lastWord      []byte
	lastWordScore uint64
	collector     *etl.Collector
}

func (da *DictAggregator) processWord(word []byte, score uint64) error {
	var scoreBuf [8]byte
	binary.BigEndian.PutUint64(scoreBuf[:], score)
	return da.collector.Collect(word, scoreBuf[:])
}

func (da *DictAggregator) Load(loadFunc etl.LoadFunc, args etl.TransformArgs) error {
	defer da.collector.Close()
	return da.collector.Load(nil, "", loadFunc, args)
}

func (da *DictAggregator) aggLoadFunc(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
	score := binary.BigEndian.Uint64(v)
	if bytes.Equal(k, da.lastWord) {
		da.lastWordScore += score
	} else {
		if da.lastWord != nil {
			if err := da.processWord(da.lastWord, da.lastWordScore); err != nil {
				return err
			}
		}
		da.lastWord = append(da.lastWord[:0], k...)
		da.lastWordScore = score
	}
	return nil
}

func (da *DictAggregator) finish() error {
	if da.lastWord != nil {
		return da.processWord(da.lastWord, da.lastWordScore)
	}
	return nil
}

func DictionaryBuilderFromCollectors(ctx context.Context, logPrefix, tmpDir string, collectors []*etl.Collector) (*DictionaryBuilder, error) {
	dictCollector := etl.NewCollector(logPrefix, tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer dictCollector.Close()
	dictAggregator := &DictAggregator{collector: dictCollector}
	for _, collector := range collectors {
		if err := collector.Load(nil, "", dictAggregator.aggLoadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
			return nil, err
		}
		collector.Close()
	}
	if err := dictAggregator.finish(); err != nil {
		return nil, err
	}
	db := &DictionaryBuilder{limit: maxDictPatterns} // Only collect 1m words with highest scores
	if err := dictCollector.Load(nil, "", db.loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return nil, err
	}
	db.finish()

	sort.Sort(db)
	return db, nil
}

func PersistDictrionary(fileName string, db *DictionaryBuilder) error {
	df, err := os.Create(fileName)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(df, etl.BufIOSize)
	db.ForEach(func(score uint64, word []byte) { fmt.Fprintf(w, "%d %x\n", score, word) })
	if err = w.Flush(); err != nil {
		return err
	}
	if err := df.Sync(); err != nil {
		return err
	}
	return df.Close()
}

func ReadDictrionary(fileName string, walker func(score uint64, word []byte) error) error {
	df, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer df.Close()
	// DictonaryBuilder is for sorting words by their freuency (to assign codes)
	ds := bufio.NewScanner(df)
	for ds.Scan() {
		tokens := strings.Split(ds.Text(), " ")
		score, err := strconv.ParseInt(tokens[0], 10, 64)
		if err != nil {
			return err
		}
		word, err := hex.DecodeString(tokens[1])
		if err != nil {
			return err
		}
		if err := walker(uint64(score), word); err != nil {
			return err
		}
	}
	return df.Close()
}

func ReadDatFile(fileName string, walker func(v []byte) error) error {
	// Read keys from the file and generate superstring (with extra byte 0x1 prepended to each character, and with 0x0 0x0 pair inserted between keys and values)
	// We only consider values with length > 2, because smaller values are not compressible without going into bits
	f, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, etl.BufIOSize)
	var buf []byte
	l, e := binary.ReadUvarint(r)
	for ; e == nil; l, e = binary.ReadUvarint(r) {
		if len(buf) < int(l) {
			buf = make([]byte, l)
		}
		if _, e = io.ReadFull(r, buf[:l]); e != nil {
			return e
		}
		if err := walker(buf[:l]); err != nil {
			return err
		}
	}
	if e != nil && !errors.Is(e, io.EOF) {
		return e
	}
	return nil
}
