// Copyright 2021 The Erigon Authors
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
	"bufio"
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/assert"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/seg/patricia"
	"github.com/erigontech/erigon/db/seg/sais"
)

func coverWordByPatterns(trace bool, input []byte, mf2 *patricia.MatchFinder2, output []byte, uncovered []int, patterns []int, cellRing *Ring, posMap map[uint64]uint64) ([]byte, []int, []int) {
	matches := mf2.FindLongestMatches(input)

	if len(matches) == 0 {
		output = append(output, 0) // Encoding of 0 in VarUint is 1 zero byte
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
	var numBuf [binary.MaxVarintLen64]byte
	p := binary.PutUvarint(numBuf[:], patternCount)
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
		n := binary.PutUvarint(numBuf[:], uint64(matches[pattern].Start))
		output = append(output, numBuf[:n]...)
		// Code
		n = binary.PutUvarint(numBuf[:], p.code)
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

func coverWordsByPatternsWorker(trace bool, inputCh chan *CompressionWord, outCh chan *CompressionWord, completion *sync.WaitGroup, trie *patricia.PatriciaTree, inputSize, outputSize *atomic.Uint64, posMap map[uint64]uint64) {
	defer completion.Done()
	var output = make([]byte, 0, 256)
	var uncovered = make([]int, 256)
	var patterns = make([]int, 0, 256)
	cellRing := NewRing()
	mf2 := patricia.NewMatchFinder2(trie)
	var numBuf [binary.MaxVarintLen64]byte
	for compW := range inputCh {
		wordLen := uint64(len(compW.word))
		n := binary.PutUvarint(numBuf[:], wordLen)
		output = append(output[:0], numBuf[:n]...) // Prepend with the encoding of length
		output, patterns, uncovered = coverWordByPatterns(trace, compW.word, mf2, output, uncovered, patterns, cellRing, posMap)
		compW.word = append(compW.word[:0], output...)
		outCh <- compW
		inputSize.Add(1 + wordLen)
		outputSize.Add(uint64(len(output)))
		posMap[wordLen+1]++
		posMap[0]++
	}
}

// CompressionWord hold a word to be compressed (if flag is set), and the result of compression
// To allow multiple words to be processed concurrently, order field is used to collect all
// the words after processing without disrupting their order
type CompressionWord struct {
	word  []byte
	order uint64
}

type CompressionQueue []*CompressionWord

func (cq CompressionQueue) Len() int {
	return len(cq)
}

func (cq CompressionQueue) Less(i, j int) bool {
	return cq[i].order < cq[j].order
}

func (cq *CompressionQueue) Swap(i, j int) {
	(*cq)[i], (*cq)[j] = (*cq)[j], (*cq)[i]
}

func (cq *CompressionQueue) Push(x interface{}) {
	*cq = append(*cq, x.(*CompressionWord))
}

func (cq *CompressionQueue) Pop() interface{} {
	old := *cq
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*cq = old[0 : n-1]
	return x
}

func compressWithPatternCandidates(ctx context.Context, trace bool, cfg Cfg, logPrefix, segmentFilePath string, cf *os.File, uncompressedFile *RawWordsFile, dictBuilder *DictionaryBuilder, lvl log.Lvl, logger log.Logger) error {
	logEvery := time.NewTicker(60 * time.Second)
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
	if lvl < log.LvlTrace {
		logger.Log(lvl, fmt.Sprintf("[%s] dictionary file parsed", logPrefix), "entries", len(code2pattern))
	}
	ch := make(chan *CompressionWord, 10_000)
	inputSize, outputSize := &atomic.Uint64{}, &atomic.Uint64{}

	var collectors []*etl.Collector
	defer func() {
		for _, c := range collectors {
			c.Close()
		}
	}()
	out := make(chan *CompressionWord, 1024)
	var compressionQueue CompressionQueue
	heap.Init(&compressionQueue)
	queueLimit := 128 * 1024

	// For the case of workers == 1
	var output = make([]byte, 0, 256)
	var uncovered = make([]int, 256)
	var patterns = make([]int, 0, 256)
	cellRing := NewRing()
	mf2 := patricia.NewMatchFinder2(&pt)

	var posMaps []map[uint64]uint64
	uncompPosMap := make(map[uint64]uint64) // For the uncompressed words
	posMaps = append(posMaps, uncompPosMap)
	var wg sync.WaitGroup
	if cfg.Workers > 1 {
		for i := 0; i < cfg.Workers; i++ {
			posMap := make(map[uint64]uint64)
			posMaps = append(posMaps, posMap)
			wg.Add(1)
			go coverWordsByPatternsWorker(trace, ch, out, &wg, &pt, inputSize, outputSize, posMap)
		}
	}
	t := time.Now()

	var err error
	intermediatePath := segmentFilePath + ".tmp"
	defer dir.RemoveFile(intermediatePath)
	var intermediateFile *os.File
	if intermediateFile, err = os.Create(intermediatePath); err != nil {
		return fmt.Errorf("create intermediate file: %w", err)
	}
	defer intermediateFile.Close()
	intermediateW := bufio.NewWriterSize(intermediateFile, 8*etl.BufIOSize)

	var inCount, outCount, emptyWordsCount uint64 // Counters words sent to compression and returned for compression
	var numBuf [binary.MaxVarintLen64]byte
	totalWords := uncompressedFile.count

	ii := 0
	if err = uncompressedFile.ForEach(func(v []byte, compression bool) error {
		ii++
		if ii%1024 == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-logEvery.C:
				if lvl < log.LvlTrace {
					logger.Log(lvl, fmt.Sprintf("[%s] Replacement preprocessing", logPrefix), "processed", fmt.Sprintf("%.2f%%", 100*float64(outCount)/float64(totalWords)), "ch", len(ch), "queue", compressionQueue.Len(), "workers", cfg.Workers)
				}
			default:
			}
		}

		if cfg.Workers > 1 {
			// take processed words in non-blocking way and push them to the queue
		outer:
			for {
				select {
				case compW := <-out:
					heap.Push(&compressionQueue, compW)
				default:
					break outer
				}
			}
			// take processed words in blocking way until either:
			// 1. compressionQueue is below the limit so that new words can be allocated
			// 2. there is word in order on top of the queue which can be written down and reused
			for compressionQueue.Len() >= queueLimit && compressionQueue[0].order < outCount {
				// Blocking wait to receive some outputs until the top of queue can be processed
				compW := <-out
				heap.Push(&compressionQueue, compW)
			}
			var compW *CompressionWord
			// Either take the word from the top, write it down and reuse for the next unprocessed word
			// Or allocate new word
			if compressionQueue.Len() > 0 && compressionQueue[0].order == outCount {
				compW = heap.Pop(&compressionQueue).(*CompressionWord)
				outCount++
				// Write to intermediate file
				if _, e := intermediateW.Write(compW.word); e != nil {
					return e
				}
				// Reuse compW for the next word
			} else {
				compW = &CompressionWord{}
			}
			compW.order = inCount
			if len(v) == 0 {
				// Empty word, cannot be compressed
				compW.word = append(compW.word[:0], 0)
				uncompPosMap[1]++
				uncompPosMap[0]++
				heap.Push(&compressionQueue, compW) // Push to the queue directly, bypassing compression
			} else if compression {
				compW.word = append(compW.word[:0], v...)
				ch <- compW // Send for compression
			} else {
				// Prepend word with encoding of length + zero byte, which indicates no patterns to be found in this word
				wordLen := uint64(len(v))
				n := binary.PutUvarint(numBuf[:], wordLen)
				uncompPosMap[wordLen+1]++
				uncompPosMap[0]++
				compW.word = append(append(append(compW.word[:0], numBuf[:n]...), 0), v...)
				heap.Push(&compressionQueue, compW) // Push to the queue directly, bypassing compression
			}
		} else {
			outCount++
			wordLen := uint64(len(v))
			n := binary.PutUvarint(numBuf[:], wordLen)
			if _, e := intermediateW.Write(numBuf[:n]); e != nil {
				return e
			}
			if wordLen > 0 {
				if compression {
					output, patterns, uncovered = coverWordByPatterns(trace, v, mf2, output[:0], uncovered, patterns, cellRing, uncompPosMap)
					if _, e := intermediateW.Write(output); e != nil {
						return e
					}
					outputSize.Add(uint64(len(output)))
				} else {
					if e := intermediateW.WriteByte(0); e != nil {
						return e
					}
					if _, e := intermediateW.Write(v); e != nil {
						return e
					}
					outputSize.Add(1 + uint64(len(v)))
				}
			}
			inputSize.Add(1 + wordLen)
			uncompPosMap[wordLen+1]++
			uncompPosMap[0]++
		}
		inCount++
		if len(v) == 0 {
			emptyWordsCount++
		}

		return nil
	}); err != nil {
		return err
	}
	close(ch)
	// Drain the out queue if necessary
	if inCount > outCount {
		for compressionQueue.Len() > 0 && compressionQueue[0].order == outCount {
			compW := heap.Pop(&compressionQueue).(*CompressionWord)
			outCount++
			if outCount == inCount {
				close(out)
			}
			// Write to intermediate file
			if _, e := intermediateW.Write(compW.word); e != nil {
				return e
			}
		}
		for compW := range out {
			heap.Push(&compressionQueue, compW)
			for compressionQueue.Len() > 0 && compressionQueue[0].order == outCount {
				compW = heap.Pop(&compressionQueue).(*CompressionWord)
				outCount++
				if outCount == inCount {
					close(out)
				}
				// Write to intermediate file
				if _, e := intermediateW.Write(compW.word); e != nil {
					return e
				}
			}
		}
	}
	if err = intermediateW.Flush(); err != nil {
		return err
	}
	wg.Wait()
	if lvl < log.LvlTrace {
		log.Log(lvl, fmt.Sprintf("[%s] Replacement preprocessing", logPrefix), "took", time.Since(t))
	}
	if _, err = intermediateFile.Seek(0, 0); err != nil {
		return fmt.Errorf("return to the start of intermediate file: %w", err)
	}

	//var m runtime.MemStats
	//common.ReadMemStats(&m)
	//logger.Info(fmt.Sprintf("[%s] Dictionary build done", logPrefix), "input", common.ByteCount(inputSize.Load()), "output", common.ByteCount(outputSize.Load()), "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
	posMap := make(map[uint64]uint64)
	for _, m := range posMaps {
		for l, c := range m {
			posMap[l] += c
		}
	}
	//fmt.Printf("posMap = %v\n", posMap)
	var patternList PatternList
	distribution := make([]int, cfg.MaxPatternLen+1)
	for _, p := range code2pattern {
		if p.uses > 0 {
			patternList = append(patternList, p)
			distribution[len(p.word)]++
		}
	}
	slices.SortFunc(patternList, patternListCmp)
	logCtx := make([]interface{}, 0, 8)
	logCtx = append(logCtx, "patternList.Len", patternList.Len())

	i := 0
	// Build Huffman tree for codes
	var codeHeap PatternHeap
	heap.Init(&codeHeap)
	tieBreaker := uint64(0)
	for codeHeap.Len()+(patternList.Len()-i) > 1 {
		// New node
		h := &PatternHuff{
			tieBreaker: tieBreaker,
		}
		if codeHeap.Len() > 0 && (i >= patternList.Len() || codeHeap[0].uses < patternList[i].uses) {
			// Take h0 from the heap
			h.h0 = heap.Pop(&codeHeap).(*PatternHuff)
			h.h0.AddZero()
			h.uses += h.h0.uses
		} else {
			// Take p0 from the list
			h.p0 = patternList[i]
			h.p0.code = 0
			h.p0.codeBits = 1
			h.uses += h.p0.uses
			i++
		}
		if codeHeap.Len() > 0 && (i >= patternList.Len() || codeHeap[0].uses < patternList[i].uses) {
			// Take h1 from the heap
			h.h1 = heap.Pop(&codeHeap).(*PatternHuff)
			h.h1.AddOne()
			h.uses += h.h1.uses
		} else {
			// Take p1 from the list
			h.p1 = patternList[i]
			h.p1.code = 1
			h.p1.codeBits = 1
			h.uses += h.p1.uses
			i++
		}
		tieBreaker++
		heap.Push(&codeHeap, h)
	}
	if codeHeap.Len() > 0 {
		root := heap.Pop(&codeHeap).(*PatternHuff)
		root.SetDepth(0)
	}
	// Calculate total size of the dictionary
	var patternsSize uint64
	for _, p := range patternList {
		ns := binary.PutUvarint(numBuf[:], uint64(p.depth))    // Length of the word's depth
		n := binary.PutUvarint(numBuf[:], uint64(len(p.word))) // Length of the word's length
		patternsSize += uint64(ns + n + len(p.word))
	}

	logCtx = append(logCtx, "patternsSize", common.ByteCount(patternsSize))
	for i, n := range distribution {
		if n == 0 {
			continue
		}
		logCtx = append(logCtx, strconv.Itoa(i), strconv.Itoa(n))
	}
	if lvl < log.LvlTrace {
		logger.Log(lvl, fmt.Sprintf("[%s] Effective dictionary", logPrefix), logCtx...)
	}
	cw := bufio.NewWriterSize(cf, 4*etl.BufIOSize)
	// 1-st, output amount of words - just a useful metadata
	binary.BigEndian.PutUint64(numBuf[:], inCount) // Dictionary size
	if _, err = cw.Write(numBuf[:8]); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(numBuf[:], emptyWordsCount)
	if _, err = cw.Write(numBuf[:8]); err != nil {
		return err
	}
	// 2-nd, output dictionary size
	binary.BigEndian.PutUint64(numBuf[:], patternsSize) // Dictionary size
	if _, err = cw.Write(numBuf[:8]); err != nil {
		return err
	}
	//fmt.Printf("patternsSize = %d\n", patternsSize)
	// Write all the pattens
	slices.SortFunc(patternList, patternListCmp)
	for _, p := range patternList {
		ns := binary.PutUvarint(numBuf[:], uint64(p.depth))
		if _, err = cw.Write(numBuf[:ns]); err != nil {
			return err
		}
		n := binary.PutUvarint(numBuf[:], uint64(len(p.word)))
		if _, err = cw.Write(numBuf[:n]); err != nil {
			return err
		}
		if _, err = cw.Write(p.word); err != nil {
			return err
		}
		//fmt.Printf("[comp] depth=%d, code=[%b], codeLen=%d pattern=[%x]\n", p.depth, p.code, p.codeBits, p.word)
	}

	var positionList PositionList
	pos2code := make(map[uint64]*Position)
	for pos, uses := range posMap {
		p := &Position{pos: pos, uses: uses, code: pos, codeBits: 0}
		positionList = append(positionList, p)
		pos2code[pos] = p
	}
	slices.SortFunc(positionList, positionListCmp)
	i = 0
	// Build Huffman tree for codes
	var posHeap PositionHeap
	heap.Init(&posHeap)
	tieBreaker = uint64(0)
	for posHeap.Len()+(positionList.Len()-i) > 1 {
		// New node
		h := &PositionHuff{
			tieBreaker: tieBreaker,
		}
		if posHeap.Len() > 0 && (i >= positionList.Len() || posHeap[0].uses < positionList[i].uses) {
			// Take h0 from the heap
			h.h0 = heap.Pop(&posHeap).(*PositionHuff)
			h.h0.AddZero()
			h.uses += h.h0.uses
		} else {
			// Take p0 from the list
			h.p0 = positionList[i]
			h.p0.code = 0
			h.p0.codeBits = 1
			h.uses += h.p0.uses
			i++
		}
		if posHeap.Len() > 0 && (i >= positionList.Len() || posHeap[0].uses < positionList[i].uses) {
			// Take h1 from the heap
			h.h1 = heap.Pop(&posHeap).(*PositionHuff)
			h.h1.AddOne()
			h.uses += h.h1.uses
		} else {
			// Take p1 from the list
			h.p1 = positionList[i]
			h.p1.code = 1
			h.p1.codeBits = 1
			h.uses += h.p1.uses
			i++
		}
		tieBreaker++
		heap.Push(&posHeap, h)
	}
	if posHeap.Len() > 0 {
		posRoot := heap.Pop(&posHeap).(*PositionHuff)
		posRoot.SetDepth(0)
	}
	// Calculate the size of pos dictionary
	var posSize uint64
	for _, p := range positionList {
		ns := binary.PutUvarint(numBuf[:], uint64(p.depth)) // Length of the position's depth
		n := binary.PutUvarint(numBuf[:], p.pos)
		posSize += uint64(ns + n)
	}
	// First, output dictionary size
	binary.BigEndian.PutUint64(numBuf[:], posSize) // Dictionary size
	if _, err = cw.Write(numBuf[:8]); err != nil {
		return err
	}
	//fmt.Printf("posSize = %d\n", posSize)
	// Write all the positions
	slices.SortFunc(positionList, positionListCmp)
	for _, p := range positionList {
		ns := binary.PutUvarint(numBuf[:], uint64(p.depth))
		if _, err = cw.Write(numBuf[:ns]); err != nil {
			return err
		}
		n := binary.PutUvarint(numBuf[:], p.pos)
		if _, err = cw.Write(numBuf[:n]); err != nil {
			return err
		}
		//fmt.Printf("[comp] depth=%d, code=[%b], codeLen=%d pos=%d\n", p.depth, p.code, p.codeBits, p.pos)
	}
	if lvl < log.LvlTrace {
		logger.Log(lvl, fmt.Sprintf("[%s] Positional dictionary", logPrefix), "positionList.len", positionList.Len(), "posSize", common.ByteCount(posSize))
	}
	// Re-encode all the words with the use of optimised (via Huffman coding) dictionaries
	wc := 0
	var hc BitWriter
	hc.w = cw
	r := bufio.NewReaderSize(intermediateFile, 2*etl.BufIOSize)
	copyNBuf := make([]byte, 32*1024)

	var l uint64
	var e error
	for l, e = binary.ReadUvarint(r); e == nil; l, e = binary.ReadUvarint(r) {
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
				if e = copyN(r, cw, uncoveredCount, copyNBuf); e != nil {
					return e
				}
			}
		}
		wc++
		if wc%1024 == 0 {
			select {
			case <-logEvery.C:
				if lvl < log.LvlTrace {
					logger.Log(lvl, fmt.Sprintf("[%s] Compressed", logPrefix), "processed", fmt.Sprintf("%.2f%%", 100*float64(wc)/float64(totalWords)))
				}
			default:
			}
		}
	}
	if !errors.Is(e, io.EOF) {
		return e
	}
	if err = intermediateFile.Close(); err != nil {
		return err
	}
	if err = cw.Flush(); err != nil {
		return err
	}
	return nil
}

// copyN - is alloc-free analog of io.CopyN func
func copyN(r io.Reader, w io.Writer, uncoveredCount int, buf []byte) error {
	// Replace the io.CopyN call with manual copy using the buffer
	if uncoveredCount <= 0 {
		return nil
	}
	remaining := int64(uncoveredCount)
	for remaining > 0 {
		bufLen := len(buf)
		if remaining < int64(bufLen) {
			bufLen = int(remaining)
		}
		if _, e := io.ReadFull(r, buf[:bufLen]); e != nil {
			return e
		}
		if _, e := w.Write(buf[:bufLen]); e != nil {
			return e
		}
		remaining -= int64(bufLen)
	}
	return nil
}

// extractPatternsInSuperstrings is the worker that processes one superstring and puts results
// into the collector, using lock to mutual exclusion. At the end (when the input channel is closed),
// it notifies the waitgroup before exiting, so that the caller known when all work is done
// No error channels for now
func extractPatternsInSuperstrings(ctx context.Context, superstringCh chan []byte, dictCollector *etl.Collector, cfg Cfg, completion *sync.WaitGroup, logger log.Logger) {
	minPatternScore, minPatternLen, maxPatternLen := cfg.MinPatternScore, cfg.MinPatternLen, cfg.MaxPatternLen
	defer completion.Done()
	dictVal := make([]byte, 8)
	dictKey := make([]byte, maxPatternLen)
	var lcp, sa, inv []int32
	for superstring := range superstringCh {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if cap(sa) < len(superstring) {
			sa = make([]int32, len(superstring))
		} else {
			sa = sa[:len(superstring)]
		}
		//log.Info("Superstring", "len", len(superstring))
		//start := time.Now()
		if err := sais.Sais(superstring, sa); err != nil {
			panic(err)
		}
		//log.Info("Suffix array built", "in", time.Since(start))
		// filter out suffixes that start with odd positions
		n := len(sa) / 2
		filtered := sa[:n]
		//filtered := make([]int32, n)
		var j int
		for i := 0; i < len(sa); i++ {
			if sa[i]&1 == 0 {
				filtered[j] = sa[i] >> 1
				j++
			}
		}
		// Now create an inverted array
		if cap(inv) < n {
			inv = make([]int32, n)
		} else {
			inv = inv[:n]
		}
		for i := int32(0); i < int32(n); i++ {
			inv[filtered[i]] = i
		}
		//logger.Info("Inverted array done")
		var k int
		// Process all suffixes one by one starting from
		// first suffix in txt[]
		if cap(lcp) < n {
			lcp = make([]int32, n)
		} else {
			lcp = lcp[:n]
		}
		for i := 0; i < n; i++ {
			/* If the current suffix is at n-1, then we don’t
			   have next substring to consider. So lcp is not
			   defined for this substring, we put zero. */
			if inv[i] == int32(n-1) {
				k = 0
				lcp[inv[i]] = 0
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

		if assert.Enable {
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
					logger.Error("Mismatch", "prefixLen", prefixLen, "lcp[i]", lcp[i], "i", i)
					break
				}
				l := int(lcp[i]) // Length of potential dictionary word
				if l < 2 {
					continue
				}
			}
		}
		//logger.Info("LCP array checked")
		// Walk over LCP array and compute the scores of the strings
		var b = inv
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
				var isNew bool
				for j > 0 && int(lcp[j-1]) >= l {
					j--
					isNew = true
				}

				if !isNew && !prevSkipped {
					break
				}

				window := i - j + 2
				copy(b, filtered[j:i+2])
				slices.Sort(b[:window])
				repeats := 1
				lastK := 0
				for k := 1; k < window; k++ {
					if b[k] >= b[lastK]+int32(l) {
						repeats++
						lastK = k
					}
				}

				if (l < 8 || l > 64) && repeats < int(minPatternScore) {
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
				binary.BigEndian.PutUint64(dictVal, score)
				if err := dictCollector.Collect(dictKey, dictVal); err != nil {
					logger.Error("extractPatternsInSuperstrings", "collect", err)
				}
				prevSkipped = false //nolint
				break
			}
		}

		superStringsPool.Put(superstring)
	}
}

func DictionaryBuilderFromCollectors(ctx context.Context, cfg Cfg, logPrefix, tmpDir string, collectors []*etl.Collector, lvl log.Lvl, logger log.Logger) (*DictionaryBuilder, error) {
	t := time.Now()
	dictCollector := etl.NewCollectorWithAllocator(logPrefix+"_collectDict", tmpDir, etl.LargeSortableBuffers, logger)
	defer dictCollector.Close()
	dictCollector.SortAndFlushInBackground(true)
	dictCollector.LogLvl(lvl)

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
	// We need `maxDictPatterns` words with highest score - but input is not sorted by score (it's sorted by `word`)
	// so, then let's just put to heap more items and then shrink at `finish()`
	db := &DictionaryBuilder{softLimit: cfg.DictReducerSoftLimit}
	if err := dictCollector.Load(nil, "", db.loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return nil, err
	}
	db.finish(cfg.MaxDictPatterns)

	db.Sort()
	if lvl < log.LvlTrace {
		logger.Log(lvl, fmt.Sprintf("[%s] BuildDict", logPrefix), "took", time.Since(t), "rev_total", dictAggregator.receivedWords, "recv_distribution", dictAggregator.dist, "hard_limit", cfg.MaxDictPatterns, "soft_limit", cfg.DictReducerSoftLimit)
	}

	return db, nil
}

func PersistDictionary(fileName string, db *DictionaryBuilder) error {
	df, err := os.Create(fileName)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(df, 2*etl.BufIOSize)
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
