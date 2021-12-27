package parallelcompress

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flanglet/kanzi-go/transform"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/patricia"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
	atomic2 "go.uber.org/atomic"
)

const ASSERT = false

const CompressLogPrefix = "compress"

// superstringLimit limits how large can one "superstring" get before it is processed
// Compressor allocates 7 bytes for each uint of superstringLimit. For example,
// superstingLimit 16m will result in 112Mb being allocated for various arrays
const superstringLimit = 16 * 1024 * 1024

// minPatternLen is minimum length of pattern we consider to be included into the dictionary
const minPatternLen = 5
const maxPatternLen = 64

// minPatternScore is minimum score (per superstring) required to consider including pattern into the dictionary
const minPatternScore = 1024

func Compress(logPrefix, fileName, segmentFileName string) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	// Read keys from the file and generate superstring (with extra byte 0x1 prepended to each character, and with 0x0 0x0 pair inserted between keys and values)
	// We only consider values with length > 2, because smaller values are not compressible without going into bits
	var superstring []byte

	workers := runtime.NumCPU() / 2
	// Collector for dictionary words (sorted by their score)
	tmpDir := ""
	ch := make(chan []byte, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	collectors := make([]*etl.Collector, workers)
	defer func() {
		for _, c := range collectors {
			c.Close()
		}
	}()
	for i := 0; i < workers; i++ {
		//nolint
		collector := etl.NewCollector(CompressLogPrefix, tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
		collectors[i] = collector
		go processSuperstring(ch, collector, &wg)
	}
	i := 0
	if err := snapshotsync.ReadSimpleFile(fileName+".dat", func(v []byte) error {
		if len(superstring)+2*len(v)+2 > superstringLimit {
			ch <- superstring
			superstring = nil
		}
		for _, a := range v {
			superstring = append(superstring, 1, a)
		}
		superstring = append(superstring, 0, 0)
		i++
		select {
		default:
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Dictionary preprocessing", logPrefix), "processed", fmt.Sprintf("%dK", i/1_000))
		}
		return nil
	}); err != nil {
		return err
	}
	if len(superstring) > 0 {
		ch <- superstring
	}
	close(ch)
	wg.Wait()

	db, err := compress.DictionaryBuilderFromCollectors(context.Background(), CompressLogPrefix, tmpDir, collectors)
	if err != nil {
		panic(err)
	}
	if err := compress.PersistDictrionary(fileName+".dictionary.txt", db); err != nil {
		return err
	}

	if err := reducedict(fileName, segmentFileName); err != nil {
		return err
	}
	return nil
}

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

func optimiseCluster(trace bool, numBuf []byte, input []byte, trie *patricia.PatriciaTree, mf *patricia.MatchFinder, output []byte, uncovered []int, patterns []int, cellRing *Ring, posMap map[uint64]uint64) ([]byte, []int, []int) {
	matches := mf.FindLongestMatches(trie, input)
	if len(matches) == 0 {
		n := binary.PutUvarint(numBuf, 0)
		output = append(output, numBuf[:n]...)
		output = append(output, input...)
		return output, patterns, uncovered
	}
	if trace {
		fmt.Printf("Cluster | input = %x\n", input)
		for _, match := range matches {
			fmt.Printf(" [%x %d-%d]", input[match.Start:match.End], match.Start, match.End)
		}
	}
	cellRing.Reset()
	patterns = append(patterns[:0], 0, 0) // Sentinel entry - no meaning
	lastF := matches[len(matches)-1]
	for j := lastF.Start; j < lastF.End; j++ {
		d := cellRing.PushBack()
		d.optimStart = j + 1
		d.coverStart = len(input)
		d.compression = 0
		d.patternIdx = 0
		d.score = 0
	}
	// Starting from the last match
	for i := len(matches); i > 0; i-- {
		f := matches[i-1]
		p := f.Val.(*Pattern)
		firstCell := cellRing.Get(0)
		if firstCell == nil {
			fmt.Printf("cellRing.Len() = %d\n", cellRing.Len())
		}
		maxCompression := firstCell.compression
		maxScore := firstCell.score
		maxCell := firstCell
		var maxInclude bool
		for e := 0; e < cellRing.Len(); e++ {
			cell := cellRing.Get(e)
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
				cellRing.Truncate(e)
				break
			}
		}
		d := cellRing.PushFront()
		d.optimStart = f.Start
		d.score = maxScore
		d.compression = maxCompression
		if maxInclude {
			if trace {
				fmt.Printf("[include] cell for %d: with patterns", f.Start)
				fmt.Printf(" [%x %d-%d]", input[f.Start:f.End], f.Start, f.End)
				patternIdx := maxCell.patternIdx
				for patternIdx != 0 {
					pattern := patterns[patternIdx]
					fmt.Printf(" [%x %d-%d]", input[matches[pattern].Start:matches[pattern].End], matches[pattern].Start, matches[pattern].End)
					patternIdx = patterns[patternIdx+1]
				}
				fmt.Printf("\n\n")
			}
			d.coverStart = f.Start
			d.patternIdx = len(patterns)
			patterns = append(patterns, i-1, maxCell.patternIdx)
		} else {
			if trace {
				fmt.Printf("cell for %d: with patterns", f.Start)
				patternIdx := maxCell.patternIdx
				for patternIdx != 0 {
					pattern := patterns[patternIdx]
					fmt.Printf(" [%x %d-%d]", input[matches[pattern].Start:matches[pattern].End], matches[pattern].Start, matches[pattern].End)
					patternIdx = patterns[patternIdx+1]
				}
				fmt.Printf("\n\n")
			}
			d.coverStart = maxCell.coverStart
			d.patternIdx = maxCell.patternIdx
		}
	}
	optimCell := cellRing.Get(0)
	if trace {
		fmt.Printf("optimal =")
	}
	// Count number of patterns
	var patternCount uint64
	patternIdx := optimCell.patternIdx
	for patternIdx != 0 {
		patternCount++
		patternIdx = patterns[patternIdx+1]
	}
	p := binary.PutUvarint(numBuf, patternCount)
	output = append(output, numBuf[:p]...)
	patternIdx = optimCell.patternIdx
	lastStart := 0
	var lastUncovered int
	uncovered = uncovered[:0]
	for patternIdx != 0 {
		pattern := patterns[patternIdx]
		p := matches[pattern].Val.(*Pattern)
		if trace {
			fmt.Printf(" [%x %d-%d]", input[matches[pattern].Start:matches[pattern].End], matches[pattern].Start, matches[pattern].End)
		}
		if matches[pattern].Start > lastUncovered {
			uncovered = append(uncovered, lastUncovered, matches[pattern].Start)
		}
		lastUncovered = matches[pattern].End
		// Starting position
		posMap[uint64(matches[pattern].Start-lastStart+1)]++
		lastStart = matches[pattern].Start
		n := binary.PutUvarint(numBuf, uint64(matches[pattern].Start))
		output = append(output, numBuf[:n]...)
		// Code
		n = binary.PutUvarint(numBuf, p.code)
		output = append(output, numBuf[:n]...)
		atomic.AddUint64(&p.uses, 1)
		patternIdx = patterns[patternIdx+1]
	}
	if len(input) > lastUncovered {
		uncovered = append(uncovered, lastUncovered, len(input))
	}
	if trace {
		fmt.Printf("\n\n")
	}
	// Add uncoded input
	for i := 0; i < len(uncovered); i += 2 {
		output = append(output, input[uncovered[i]:uncovered[i+1]]...)
	}
	return output, patterns, uncovered
}

func reduceDictWorker(inputCh chan []byte, completion *sync.WaitGroup, trie *patricia.PatriciaTree, collector *etl.Collector, inputSize, outputSize *atomic2.Uint64, posMap map[uint64]uint64) {
	defer completion.Done()
	var output = make([]byte, 0, 256)
	var uncovered = make([]int, 256)
	var patterns = make([]int, 0, 256)
	cellRing := NewRing()
	var mf patricia.MatchFinder
	numBuf := make([]byte, binary.MaxVarintLen64)
	for input := range inputCh {
		// First 8 bytes are idx
		n := binary.PutUvarint(numBuf, uint64(len(input)-8))
		output = append(output[:0], numBuf[:n]...)
		if len(input) > 8 {
			output, patterns, uncovered = optimiseCluster(false, numBuf, input[8:], trie, &mf, output, uncovered, patterns, cellRing, posMap)
			if err := collector.Collect(input[:8], output); err != nil {
				log.Error("Could not collect", "error", err)
				return
			}
		}
		inputSize.Add(1 + uint64(len(input)-8))
		outputSize.Add(uint64(len(output)))
		posMap[uint64(len(input)-8+1)]++
		posMap[0]++
	}
}

type Pattern struct {
	score    uint64
	uses     uint64
	code     uint64 // Allocated numerical code
	codeBits int    // Number of bits in the code
	w        []byte
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

// reduceDict reduces the dictionary by trying the substitutions and counting frequency for each word
func reducedict(name string, segmentFileName string) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	// DictionaryBuilder is for sorting words by their freuency (to assign codes)
	var pt patricia.PatriciaTree
	code2pattern := make([]*Pattern, 0, 256)
	if err := compress.ReadDictrionary(name+".dictionary.txt", func(score uint64, word []byte) error {
		p := &Pattern{
			score:    score,
			uses:     0,
			code:     uint64(len(code2pattern)),
			codeBits: 0,
			w:        word,
		}
		pt.Insert(word, p)
		code2pattern = append(code2pattern, p)
		return nil
	}); err != nil {
		return err
	}
	log.Info("dictionary file parsed", "entries", len(code2pattern))
	tmpDir := ""
	ch := make(chan []byte, 10000)
	inputSize, outputSize := atomic2.NewUint64(0), atomic2.NewUint64(0)
	var wg sync.WaitGroup
	workers := runtime.NumCPU() / 2
	var collectors []*etl.Collector
	defer func() {
		for _, c := range collectors {
			c.Close()
		}
	}()
	var posMaps []map[uint64]uint64
	for i := 0; i < workers; i++ {
		//nolint
		collector := etl.NewCollector(CompressLogPrefix, tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
		collectors = append(collectors, collector)
		posMap := make(map[uint64]uint64)
		posMaps = append(posMaps, posMap)
		wg.Add(1)
		go reduceDictWorker(ch, &wg, &pt, collector, inputSize, outputSize, posMap)
	}
	var wordsCount uint64
	if err := snapshotsync.ReadSimpleFile(name+".dat", func(v []byte) error {
		input := make([]byte, 8+int(len(v)))
		binary.BigEndian.PutUint64(input, wordsCount)
		copy(input[8:], v)
		ch <- input
		wordsCount++
		select {
		default:
		case <-logEvery.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			log.Info("Replacement preprocessing", "processed", fmt.Sprintf("%dK", wordsCount/1_000), "input", common.StorageSize(inputSize.Load()), "output", common.StorageSize(outputSize.Load()), "alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
		}
		return nil
	}); err != nil {
		return err
	}
	close(ch)
	wg.Wait()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Info("Done", "input", common.StorageSize(inputSize.Load()), "output", common.StorageSize(outputSize.Load()), "alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
	posMap := make(map[uint64]uint64)
	for _, m := range posMaps {
		for l, c := range m {
			posMap[l] += c
		}
	}
	//fmt.Printf("posMap = %v\n", posMap)
	var patternList PatternList
	for _, p := range code2pattern {
		if p.uses > 0 {
			patternList = append(patternList, p)
		}
	}
	sort.Sort(&patternList)
	if len(patternList) == 0 {
		log.Warn("dictionary is empty")
		//err := fmt.Errorf("dictionary is empty")
		//panic(err)
		//return err
	}
	// Calculate offsets of the dictionary patterns and total size
	var offset uint64
	numBuf := make([]byte, binary.MaxVarintLen64)
	for _, p := range patternList {
		p.offset = offset
		n := binary.PutUvarint(numBuf, uint64(len(p.w)))
		offset += uint64(n + len(p.w))
	}
	patternCutoff := offset // All offsets below this will be considered patterns
	i := 0
	log.Info("Effective dictionary", "size", patternList.Len())
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
			n := binary.PutUvarint(numBuf, h.h0.offset)
			offset += uint64(n)
		} else {
			// Take p0 from the list
			h.p0 = patternList[i]
			h.p0.code = 0
			h.p0.codeBits = 1
			h.uses += h.p0.uses
			n := binary.PutUvarint(numBuf, h.p0.offset)
			offset += uint64(n)
			i++
		}
		if codeHeap.Len() > 0 && (i >= patternList.Len() || codeHeap[0].uses < patternList[i].uses) {
			// Take h1 from the heap
			h.h1 = heap.Pop(&codeHeap).(*PatternHuff)
			h.h1.AddOne()
			h.uses += h.h1.uses
			n := binary.PutUvarint(numBuf, h.h1.offset)
			offset += uint64(n)
		} else {
			// Take p1 from the list
			h.p1 = patternList[i]
			h.p1.code = 1
			h.p1.codeBits = 1
			h.uses += h.p1.uses
			n := binary.PutUvarint(numBuf, h.p1.offset)
			offset += uint64(n)
			i++
		}
		tieBreaker++
		heap.Push(&codeHeap, h)
		huffs = append(huffs, h)
	}
	root := &PatternHuff{}
	if len(patternList) > 0 {
		root = heap.Pop(&codeHeap).(*PatternHuff)
	}
	var cf *os.File
	var err error
	if cf, err = os.Create(segmentFileName); err != nil {
		return err
	}
	cw := bufio.NewWriterSize(cf, etl.BufIOSize)
	// 1-st, output dictionary
	binary.BigEndian.PutUint64(numBuf, wordsCount) // Dictionary size
	if _, err = cw.Write(numBuf[:8]); err != nil {
		return err
	}
	// 2-nd, output dictionary
	binary.BigEndian.PutUint64(numBuf, offset) // Dictionary size
	if _, err = cw.Write(numBuf[:8]); err != nil {
		return err
	}
	// 3-rd, output directory root
	binary.BigEndian.PutUint64(numBuf, root.offset)
	if _, err = cw.Write(numBuf[:8]); err != nil {
		return err
	}
	// 4-th, output pattern cutoff offset
	binary.BigEndian.PutUint64(numBuf, patternCutoff)
	if _, err = cw.Write(numBuf[:8]); err != nil {
		return err
	}
	// Write all the pattens
	for _, p := range patternList {
		n := binary.PutUvarint(numBuf, uint64(len(p.w)))
		if _, err = cw.Write(numBuf[:n]); err != nil {
			return err
		}
		if _, err = cw.Write(p.w); err != nil {
			return err
		}
	}
	// Write all the huffman nodes
	for _, h := range huffs {
		var n int
		if h.h0 != nil {
			n = binary.PutUvarint(numBuf, h.h0.offset)
		} else {
			n = binary.PutUvarint(numBuf, h.p0.offset)
		}
		if _, err = cw.Write(numBuf[:n]); err != nil {
			return err
		}
		if h.h1 != nil {
			n = binary.PutUvarint(numBuf, h.h1.offset)
		} else {
			n = binary.PutUvarint(numBuf, h.p1.offset)
		}
		if _, err = cw.Write(numBuf[:n]); err != nil {
			return err
		}
	}
	log.Info("Dictionary", "size", offset, "pattern cutoff", patternCutoff)

	var positionList PositionList
	pos2code := make(map[uint64]*Position)
	for pos, uses := range posMap {
		p := &Position{pos: pos, uses: uses, code: 0, codeBits: 0, offset: 0}
		positionList = append(positionList, p)
		pos2code[pos] = p
	}
	sort.Sort(&positionList)
	// Calculate offsets of the dictionary positions and total size
	offset = 0
	for _, p := range positionList {
		p.offset = offset
		n := binary.PutUvarint(numBuf, p.pos)
		offset += uint64(n)
	}
	positionCutoff := offset // All offsets below this will be considered positions
	i = 0
	log.Info("Positional dictionary", "size", positionList.Len())
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
			n := binary.PutUvarint(numBuf, h.h0.offset)
			offset += uint64(n)
		} else {
			// Take p0 from the list
			h.p0 = positionList[i]
			h.p0.code = 0
			h.p0.codeBits = 1
			h.uses += h.p0.uses
			n := binary.PutUvarint(numBuf, h.p0.offset)
			offset += uint64(n)
			i++
		}
		if posHeap.Len() > 0 && (i >= positionList.Len() || posHeap[0].uses < positionList[i].uses) {
			// Take h1 from the heap
			h.h1 = heap.Pop(&posHeap).(*PositionHuff)
			h.h1.AddOne()
			h.uses += h.h1.uses
			n := binary.PutUvarint(numBuf, h.h1.offset)
			offset += uint64(n)
		} else {
			// Take p1 from the list
			h.p1 = positionList[i]
			h.p1.code = 1
			h.p1.codeBits = 1
			h.uses += h.p1.uses
			n := binary.PutUvarint(numBuf, h.p1.offset)
			offset += uint64(n)
			i++
		}
		tieBreaker++
		heap.Push(&posHeap, h)
		posHuffs = append(posHuffs, h)
	}
	posRoot := heap.Pop(&posHeap).(*PositionHuff)
	// First, output dictionary
	binary.BigEndian.PutUint64(numBuf, offset) // Dictionary size
	if _, err = cw.Write(numBuf[:8]); err != nil {
		return err
	}
	// Secondly, output directory root
	binary.BigEndian.PutUint64(numBuf, posRoot.offset)
	if _, err = cw.Write(numBuf[:8]); err != nil {
		return err
	}
	// Thirdly, output pattern cutoff offset
	binary.BigEndian.PutUint64(numBuf, positionCutoff)
	if _, err = cw.Write(numBuf[:8]); err != nil {
		return err
	}
	// Write all the positions
	for _, p := range positionList {
		n := binary.PutUvarint(numBuf, p.pos)
		if _, err = cw.Write(numBuf[:n]); err != nil {
			return err
		}
	}
	// Write all the huffman nodes
	for _, h := range posHuffs {
		var n int
		if h.h0 != nil {
			n = binary.PutUvarint(numBuf, h.h0.offset)
		} else {
			n = binary.PutUvarint(numBuf, h.p0.offset)
		}
		if _, err = cw.Write(numBuf[:n]); err != nil {
			return err
		}
		if h.h1 != nil {
			n = binary.PutUvarint(numBuf, h.h1.offset)
		} else {
			n = binary.PutUvarint(numBuf, h.p1.offset)
		}
		if _, err = cw.Write(numBuf[:n]); err != nil {
			return err
		}
	}
	log.Info("Positional dictionary", "size", offset, "position cutoff", positionCutoff)
	df, err := os.Create("huffman_codes.txt")
	if err != nil {
		return err
	}
	defer df.Close()
	w := bufio.NewWriterSize(df, etl.BufIOSize)
	defer w.Flush()
	for _, p := range positionList {
		fmt.Fprintf(w, "%d %x %d uses %d\n", p.codeBits, p.code, p.pos, p.uses)
	}
	if err = w.Flush(); err != nil {
		return err
	}
	df.Close()

	aggregator := etl.NewCollector(CompressLogPrefix, tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer aggregator.Close()
	for _, collector := range collectors {
		if err = collector.Load(nil, "", func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
			return aggregator.Collect(k, v)
		}, etl.TransformArgs{}); err != nil {
			return err
		}
		collector.Close()
	}

	wc := 0
	var hc HuffmanCoder
	hc.w = cw
	if err = aggregator.Load(nil, "", func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		// Re-encode it
		r := bytes.NewReader(v)
		var l uint64
		var e error
		if l, err = binary.ReadUvarint(r); e != nil {
			return e
		}
		posCode := pos2code[l+1]
		if e = hc.encode(posCode.code, posCode.codeBits); e != nil {
			return e
		}
		if l > 0 {
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
				if e = hc.encode(posCode.code, posCode.codeBits); e != nil {
					return e
				}
				var code uint64 // Code of the pattern
				if code, e = binary.ReadUvarint(r); e != nil {
					return e
				}
				patternCode := code2pattern[code]
				if int(pos) > lastUncovered {
					uncoveredCount += int(pos) - lastUncovered
				}
				lastUncovered = int(pos) + len(patternCode.w)
				if e = hc.encode(patternCode.code, patternCode.codeBits); e != nil {
					return e
				}
			}
			if int(l) > lastUncovered {
				uncoveredCount += int(l) - lastUncovered
			}
			// Terminating position and flush
			posCode = pos2code[0]
			if e = hc.encode(posCode.code, posCode.codeBits); e != nil {
				return e
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
		wc++
		if wc%10_000_000 == 0 {
			log.Info("Compressed", "millions", wc/1_000_000)
		}
		return nil
	}, etl.TransformArgs{}); err != nil {
		return err
	}
	aggregator.Close()
	if err = cw.Flush(); err != nil {
		return err
	}
	if err = cf.Close(); err != nil {
		return err
	}
	return nil
}

// processSuperstring is the worker that processes one superstring and puts results
// into the collector, using lock to mutual exclusion. At the end (when the input channel is closed),
// it notifies the waitgroup before exiting, so that the caller known when all work is done
// No error channels for now
func processSuperstring(superstringCh chan []byte, dictCollector *etl.Collector, completion *sync.WaitGroup) {
	for superstring := range superstringCh {
		//log.Info("Superstring", "len", len(superstring))
		sa := make([]int32, len(superstring))
		divsufsort, err := transform.NewDivSufSort()
		if err != nil {
			log.Error("processSuperstring", "create divsufsoet", err)
		}
		//start := time.Now()
		divsufsort.ComputeSuffixArray(superstring, sa)
		//log.Info("Suffix array built", "in", time.Since(start))
		// filter out suffixes that start with odd positions
		n := len(sa) / 2
		filtered := make([]int, n)
		var j int
		for i := 0; i < len(sa); i++ {
			if sa[i]&1 == 0 {
				filtered[j] = int(sa[i] >> 1)
				j++
			}
		}
		//log.Info("Suffix array filtered")
		// invert suffixes
		inv := make([]int, n)
		for i := 0; i < n; i++ {
			inv[filtered[i]] = i
		}
		//log.Info("Inverted array done")
		lcp := make([]int32, n)
		var k int
		// Process all suffixes one by one starting from
		// first suffix in txt[]
		for i := 0; i < n; i++ {
			/* If the current suffix is at n-1, then we don’t
			   have next substring to consider. So lcp is not
			   defined for this substring, we put zero. */
			if inv[i] == n-1 {
				k = 0
				continue
			}

			/* j contains index of the next substring to
			   be considered  to compare with the present
			   substring, i.e., next string in suffix array */
			j := int(filtered[inv[i]+1])

			// Directly start matching from k'th index as
			// at-least k-1 characters will match
			for i+k < n && j+k < n && superstring[(i+k)*2] != 0 && superstring[(j+k)*2] != 0 && superstring[(i+k)*2+1] == superstring[(j+k)*2+1] {
				k++
			}
			lcp[inv[i]] = int32(k) // lcp for the present suffix.

			// Deleting the starting character from the string.
			if k > 0 {
				k--
			}
		}
		//log.Info("Kasai algorithm finished")
		// Checking LCP array

		if ASSERT {
			for i := 0; i < n-1; i++ {
				var prefixLen int
				p1 := int(filtered[i])
				p2 := int(filtered[i+1])
				for p1+prefixLen < n &&
					p2+prefixLen < n &&
					superstring[(p1+prefixLen)*2] != 0 &&
					superstring[(p2+prefixLen)*2] != 0 &&
					superstring[(p1+prefixLen)*2+1] == superstring[(p2+prefixLen)*2+1] {
					prefixLen++
				}
				if prefixLen != int(lcp[i]) {
					log.Error("Mismatch", "prefixLen", prefixLen, "lcp[i]", lcp[i], "i", i)
					break
				}
				l := int(lcp[i]) // Length of potential dictionary word
				if l < 2 {
					continue
				}
				dictKey := make([]byte, l)
				for s := 0; s < l; s++ {
					dictKey[s] = superstring[(filtered[i]+s)*2+1]
				}
				//fmt.Printf("%d %d %s\n", filtered[i], lcp[i], dictKey)
			}
		}
		//log.Info("LCP array checked")
		// Walk over LCP array and compute the scores of the strings
		b := inv
		j = 0
		for i := 0; i < n-1; i++ {
			// Only when there is a drop in LCP value
			if lcp[i+1] >= lcp[i] {
				j = i
				continue
			}
			for l := int(lcp[i]); l > int(lcp[i+1]); l-- {
				if l < minPatternLen || l > maxPatternLen {
					continue
				}
				// Go back
				var new bool
				for j > 0 && int(lcp[j-1]) >= l {
					j--
					new = true
				}
				if !new {
					break
				}
				window := i - j + 2
				copy(b, filtered[j:i+2])
				sort.Ints(b[:window])
				repeats := 1
				lastK := 0
				for k := 1; k < window; k++ {
					if b[k] >= b[lastK]+l {
						repeats++
						lastK = k
					}
				}
				score := uint64(repeats * int(l-4))
				if score > minPatternScore {
					dictKey := make([]byte, l)
					for s := 0; s < l; s++ {
						dictKey[s] = superstring[(filtered[i]+s)*2+1]
					}
					var dictVal [8]byte
					binary.BigEndian.PutUint64(dictVal[:], score)
					if err = dictCollector.Collect(dictKey, dictVal[:]); err != nil {
						log.Error("processSuperstring", "collect", err)
					}
				}
			}
		}
	}
	completion.Done()
}
