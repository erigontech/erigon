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
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flanglet/kanzi-go/transform"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/patricia"
	"github.com/ledgerwatch/log/v3"
	atomic2 "go.uber.org/atomic"
)

// MinPatternScore is minimum score (per superstring) required to consider including pattern into the dictionary
const MinPatternScore = 1024

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
			} else if cell.optimStart > f.End {
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

func reduceDictWorker(trace bool, inputCh chan []byte, outCh chan *pair, completion *sync.WaitGroup, trie *patricia.PatriciaTree, inputSize, outputSize *atomic2.Uint64, posMap map[uint64]uint64) {
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
		output, patterns, uncovered = optimiseCluster(trace, numBuf, input[8:], trie, &mf, output, uncovered, patterns, cellRing, posMap)
		outCh <- &pair{k: input[:8], v: common.Copy(output)}
		inputSize.Add(1 + uint64(len(input)-8))
		outputSize.Add(uint64(len(output)))
		posMap[uint64(len(input)-8+1)]++
		posMap[0]++
	}
}

type pair struct{ k, v []byte }

// reduceDict reduces the dictionary by trying the substitutions and counting frequency for each word
func reducedict(trace bool, logPrefix, segmentFilePath, tmpDir string, datFile *DecompressedFile, workers int, dictBuilder *DictionaryBuilder) error {
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	// DictionaryBuilder is for sorting words by their freuency (to assign codes)
	var pt patricia.PatriciaTree
	code2pattern := make([]*Pattern, 0, 256)
	dictBuilder.ForEach(func(score uint64, word []byte) {
		p := &Pattern{
			score:    score,
			uses:     0,
			code:     uint64(len(code2pattern)),
			codeBits: 0,
			word:     word,
		}
		pt.Insert(word, p)
		code2pattern = append(code2pattern, p)
	})
	dictBuilder.Close()
	log.Debug(fmt.Sprintf("[%s] dictionary file parsed", logPrefix), "entries", len(code2pattern))
	ch := make(chan []byte, 10_000)
	inputSize, outputSize := atomic2.NewUint64(0), atomic2.NewUint64(0)

	var collectors []*etl.Collector
	defer func() {
		for _, c := range collectors {
			c.Close()
		}
	}()
	out := make(chan *pair, 1024)

	aggregator := etl.NewCollector(compressLogPrefix, tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer aggregator.Close()
	var wgAggregator sync.WaitGroup
	wgAggregator.Add(1)
	go func() {
		defer wgAggregator.Done()
		for a := range out {
			if err := aggregator.Collect(a.k, a.v); err != nil {
				panic(err)
			}
		}
	}()
	var posMaps []map[uint64]uint64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		//nolint
		posMap := make(map[uint64]uint64)
		posMaps = append(posMaps, posMap)
		wg.Add(1)
		go reduceDictWorker(trace, ch, out, &wg, &pt, inputSize, outputSize, posMap)
	}
	var wordsCount uint64
	if err := datFile.ForEach(func(v []byte) error {
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
			log.Debug(fmt.Sprintf("[%s] Replacement preprocessing", logPrefix),
				"processed", fmt.Sprintf("%.2f%%", 100*float64(wordsCount)/float64(datFile.count)),
				//"input", common.ByteCount(inputSize.Load()), "output", common.ByteCount(outputSize.Load()),
				"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
		}
		return nil
	}); err != nil {
		return err
	}
	close(ch)
	wg.Wait()
	close(out)
	wgAggregator.Wait()

	//var m runtime.MemStats
	//runtime.ReadMemStats(&m)
	//log.Info(fmt.Sprintf("[%s] Dictionary build done", logPrefix), "input", common.ByteCount(inputSize.Load()), "output", common.ByteCount(outputSize.Load()), "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
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
	// Calculate offsets of the dictionary patterns and total size
	var offset uint64
	numBuf := make([]byte, binary.MaxVarintLen64)
	for _, p := range patternList {
		p.offset = offset
		n := binary.PutUvarint(numBuf, uint64(len(p.word)))
		offset += uint64(n + len(p.word))
	}
	patternCutoff := offset // All offsets below this will be considered patterns
	i := 0
	log.Debug(fmt.Sprintf("[%s] Effective dictionary", logPrefix), "size", patternList.Len())
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
	if codeHeap.Len() > 0 {
		root = heap.Pop(&codeHeap).(*PatternHuff)
	}
	var cf *os.File
	var err error
	if cf, err = os.Create(segmentFilePath); err != nil {
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
		n := binary.PutUvarint(numBuf, uint64(len(p.word)))
		if _, err = cw.Write(numBuf[:n]); err != nil {
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
	log.Debug(fmt.Sprintf("[%s] Dictionary", logPrefix), "size", common.ByteCount(offset), "pattern cutoff", patternCutoff)

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
	log.Debug(fmt.Sprintf("[%s] Positional dictionary", logPrefix), "size", positionList.Len())
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
	var posRoot *PositionHuff
	if posHeap.Len() > 0 {
		posRoot = heap.Pop(&posHeap).(*PositionHuff)
	}
	// First, output dictionary
	binary.BigEndian.PutUint64(numBuf, offset) // Dictionary size
	if _, err = cw.Write(numBuf[:8]); err != nil {
		return err
	}
	// Secondly, output directory root
	if posRoot == nil {
		binary.BigEndian.PutUint64(numBuf, 0)
	} else {
		binary.BigEndian.PutUint64(numBuf, posRoot.offset)
	}
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
	log.Debug(fmt.Sprintf("[%s] Positional dictionary", logPrefix), "size", common.ByteCount(offset), "position cutoff", positionCutoff)

	wc := 0
	var hc HuffmanCoder
	hc.w = cw
	r := bytes.NewReader(nil)
	if err = aggregator.Load(nil, "", func(_, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		// Re-encode it
		r.Reset(v)
		var l uint64
		var e error
		if l, err = binary.ReadUvarint(r); err != nil {
			return err
		}
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
				patternCode := code2pattern[code]
				if int(pos) > lastUncovered {
					uncoveredCount += int(pos) - lastUncovered
				}
				lastUncovered = int(pos) + len(patternCode.word)
				if patternCode != nil {
					if e = hc.encode(patternCode.code, patternCode.codeBits); e != nil {
						return e
					}
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
			log.Info(fmt.Sprintf("[%s] Compressed", logPrefix), "millions", wc/1_000_000)
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
func processSuperstring(superstringCh chan []byte, dictCollector *etl.Collector, minPatternScore uint64, completion *sync.WaitGroup) {
	defer completion.Done()
	var dictVal [8]byte
	dictKey := make([]byte, maxPatternLen)
	sa := make([]int32, superstringLimit)
	divsufsort, err := transform.NewDivSufSort()
	if err != nil {
		log.Error("processSuperstring", "create divsufsoet", err)
	}
	for superstring := range superstringCh {
		sa = sa[:len(superstring)]
		//log.Info("Superstring", "len", len(superstring))
		//start := time.Now()
		divsufsort.ComputeSuffixArray(superstring, sa)
		//log.Info("Suffix array built", "in", time.Since(start))
		// filter out suffixes that start with odd positions
		n := len(sa) / 2
		filtered := make([]int32, n)
		var j int
		for i := 0; i < len(sa); i++ {
			if sa[i]&1 == 0 {
				filtered[j] = sa[i] >> 1
				j++
			}
		}
		// Now create an inverted array - we reuse the second half of suffix array for that
		/*
			inv := sa[:n]
			for i := 0; i < n; i++ {
				inv[filtered[i]] = int32(i)
			}
		*/
		inv := make([]int32, n)
		for i := 0; i < n; i++ {
			inv[filtered[i]] = int32(i)
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
			if inv[i] == int32(n-1) {
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
			}
		}
		//log.Info("LCP array checked")
		// Walk over LCP array and compute the scores of the strings
		var b Int32Sort = inv
		j = 0
		for i := 0; i < n-1; i++ {
			// Only when there is a drop in LCP value
			if lcp[i+1] >= lcp[i] {
				j = i
				continue
			}
			prevSkipped := false
			for l := int(lcp[i]); l > int(lcp[i+1]) && l >= minPatternLen; l-- {
				if l > maxPatternLen ||
					l > 20 && (l&(l-1)) != 0 { // is power of 2
					prevSkipped = true
					continue
				}

				// Go back
				var new bool
				for j > 0 && int(lcp[j-1]) >= l {
					j--
					new = true
				}

				if !new && !prevSkipped {
					break
				}

				window := i - j + 2
				copy(b, filtered[j:i+2])
				sort.Sort(b[:window])
				repeats := 1
				lastK := 0
				for k := 1; k < window; k++ {
					if b[k] >= b[lastK]+int32(l) {
						repeats++
						lastK = k
					}
				}

				if (l < 8 && repeats < int(minPatternScore)) ||
					(l > 64 && repeats < 200) {
					prevSkipped = true
					continue
				}

				score := uint64(repeats * (l))
				if score < minPatternScore {
					prevSkipped = true
					continue
				}

				dictKey = dictKey[:l]
				for s := 0; s < l; s++ {
					dictKey[s] = superstring[(int(filtered[i])+s)*2+1]
				}
				binary.BigEndian.PutUint64(dictVal[:], score)
				if err = dictCollector.Collect(dictKey, dictVal[:]); err != nil {
					log.Error("processSuperstring", "collect", err)
				}
				prevSkipped = false
				break
			}
		}
	}
}

func DictionaryBuilderFromCollectors(ctx context.Context, logPrefix, tmpDir string, collectors []*etl.Collector) (*DictionaryBuilder, error) {
	dictCollector := etl.NewCollector(logPrefix, tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer dictCollector.Close()
	dictAggregator := &DictAggregator{collector: dictCollector, dist: map[int]int{}}
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

func ReadSimpleFile(fileName string, walker func(v []byte) error) error {
	// Read keys from the file and generate superstring (with extra byte 0x1 prepended to each character, and with 0x0 0x0 pair inserted between keys and values)
	// We only consider values with length > 2, because smaller values are not compressible without going into bits
	f, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, etl.BufIOSize)
	buf := make([]byte, 4096)
	for l, e := binary.ReadUvarint(r); ; l, e = binary.ReadUvarint(r) {
		if e != nil {
			if errors.Is(e, io.EOF) {
				break
			}
			return e
		}
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
	return nil
}

type Int32Sort []int32

func (f Int32Sort) Len() int           { return len(f) }
func (f Int32Sort) Less(i, j int) bool { return f[i] < f[j] }
func (f Int32Sort) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }
