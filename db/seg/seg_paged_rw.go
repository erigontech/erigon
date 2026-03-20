// Copyright 2024 The Erigon Authors
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
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/compress"
)

var be = binary.BigEndian

func GetFromPage(key, compressedPage []byte, compressionBuf []byte, compressionEnabled bool) (v []byte, compressionBufOut []byte) {
	var err error
	var page []byte
	compressionBuf, page, err = compress.DecodeZstdIfNeed(compressionBuf[:0], compressedPage, compressionEnabled)
	if err != nil {
		panic(err)
	}

	cnt := int(page[0])
	if cnt == 0 {
		return nil, compressionBuf
	}
	meta, data := page[1:1+cnt*4*2], page[1+cnt*4*2:]
	kLens, vLens := meta[:cnt*4], meta[cnt*4:]
	var kOffset, vOffset uint32
	for i := 0; i < cnt*4; i += 4 {
		vOffset += be.Uint32(kLens[i:])
	}
	keys := data[:vOffset]
	vals := data[vOffset:]
	vOffset = 0

	for i := 0; i < cnt*4; i += 4 {
		kLen, vLen := be.Uint32(kLens[i:]), be.Uint32(vLens[i:])
		foundKey := keys[kOffset : kOffset+kLen]
		if bytes.Equal(key, foundKey) {
			return vals[vOffset : vOffset+vLen], compressionBuf
		} else {
			_ = data
		}
		kOffset += kLen
		vOffset += vLen
	}
	return nil, compressionBuf
}

type Page struct {
	i, limit           int
	kLens, vLens, data []byte
	kOffset, vOffset   uint32

	compressionBuf []byte
}

func FromBytes(buf []byte, compressionEnabled bool) *Page {
	r := &Page{}
	r.Reset(buf, compressionEnabled)
	return r
}

func (r *Page) Reset(v []byte, compressionEnabled bool) (n int) {
	var err error
	r.compressionBuf, v, err = compress.DecodeZstdIfNeed(r.compressionBuf[:0], v, compressionEnabled)
	if err != nil {
		panic(fmt.Errorf("len(v): %d, %w", len(v), err))
	}

	r.i, r.kOffset, r.vOffset = 0, 0, 0
	r.limit = int(v[0])
	meta, data := v[1:1+r.limit*4*2], v[1+r.limit*4*2:]
	r.kLens, r.vLens, r.data = meta[:r.limit*4], meta[r.limit*4:r.limit*4*2], data

	for i := 0; i < r.limit*4; i += 4 {
		r.vOffset += be.Uint32(r.kLens[i:])
	}
	return
}
func (r *Page) HasNext() bool { return r.limit > r.i }
func (r *Page) Next() (k, v []byte) {
	kLen := be.Uint32(r.kLens[r.i*4:])
	k = r.data[r.kOffset : r.kOffset+kLen]
	vLen := be.Uint32(r.vLens[r.i*4:])
	v = r.data[r.vOffset : r.vOffset+vLen]
	r.i++
	r.kOffset += kLen
	r.vOffset += vLen
	return k, v
}

func WordsAmount2PagesAmount(wordsAmount int, pageSize int) (pagesAmount int) {
	pagesAmount = wordsAmount
	if wordsAmount == 0 {
		return 0
	}
	if pageSize > 0 {
		pagesAmount = (wordsAmount-1)/pageSize + 1 //amount of pages
	}
	return pagesAmount
}

type pageWorkItem struct {
	seq              int
	uncompressedData []byte   // assembled page: [header][keys][vals] (built by worker)
	keys             []byte   // raw keys data, swapped from producer
	vals             []byte   // raw vals data, swapped from producer
	kLengths         []uint32 // key lengths, swapped from producer
	vLengths         []uint32 // val lengths, swapped from producer
}

type pageResult struct {
	seq  int
	data []byte // compressed page; returned to pool after write
}

type PagedReader struct {
	file         ReaderI
	isCompressed bool
	pageSize     int
	page         *Page

	currentPageOffset, nextPageOffset uint64
}

func NewPagedReader(r ReaderI, pageSize int, snappy bool) *PagedReader {
	if pageSize == 0 {
		pageSize = 1
	}
	return &PagedReader{file: r, pageSize: pageSize, isCompressed: snappy, page: &Page{}}
}

func (g *PagedReader) Reset(offset uint64) {
	if g.pageSize <= 1 {
		g.file.Reset(offset)
		return
	}
	if g.currentPageOffset == offset { // don't reset internal state in this case: likely user just iterating over all values
		return
	}

	g.file.Reset(offset)
	g.currentPageOffset = offset
	g.nextPageOffset = offset
	g.page = &Page{} // TODO: optimize
	if g.file.HasNext() {
		g.NextPage()
	}
}

//	func (g *PagedReader) PrintPages() {
//		if g.pageSize <= 1 {
//			return
//		}
//		g.Reset(0)
//		i := 0
//		for {
//			var keys [][]byte
//			fst, _ := g.page.First()
//			fst = common.Copy(fst)
//			lst, _ := g.page.Last()
//			lst = common.Copy(lst)
//			fmt.Printf("page: %d, offset=%d, keys: %d %x-%x\n", i, g.currentPageOffset, len(keys), fst, lst)
//			i++
//			if !g.HasNextPage() {
//				break
//			}
//			g.NextPage()
//		}
//	}
func (g *PagedReader) MadvNormal() *PagedReader {
	g.file.MadvNormal()
	return g
}
func (g *PagedReader) DisableReadAhead()   { g.file.DisableReadAhead() }
func (g *PagedReader) FileName() string    { return g.file.FileName() }
func (g *PagedReader) Count() int          { return g.file.Count() }
func (g *PagedReader) Size() int           { return g.file.Size() }
func (g *PagedReader) PageSize() int       { return g.pageSize }
func (g *PagedReader) HasNextOnPage() bool { return g.pageSize > 1 && g.page.HasNext() }
func (g *PagedReader) HasNextPage() bool   { return g.file.HasNext() }
func (g *PagedReader) HasNext() bool       { return g.HasNextOnPage() || g.HasNextPage() }
func (g *PagedReader) GetMetadata() []byte { return g.file.GetMetadata() }
func (g *PagedReader) Next(buf []byte) ([]byte, uint64) {
	if g.pageSize <= 1 {
		return g.file.Next(buf)
	}

	if g.page.HasNext() {
		_, v := g.page.Next()
		if g.page.HasNext() {
			return v, g.currentPageOffset
		}
		return v, g.nextPageOffset
	}
	g.NextPage()
	_, v := g.page.Next()
	return v, g.currentPageOffset
}

func (g *PagedReader) NextPage() {
	g.currentPageOffset = g.nextPageOffset
	var pageV []byte
	pageV, g.nextPageOffset = g.file.Next(nil)
	g.page.Reset(pageV, g.isCompressed)
}

func (g *PagedReader) Next2(buf []byte) (k, v, bufOut []byte, pageOffset uint64) {
	if g.pageSize <= 1 {
		buf, pageOffset = g.file.Next(buf)
		return nil, buf, buf, pageOffset
	}

	if g.page.HasNext() {
		k, v = g.page.Next()
		return k, v, buf, g.currentPageOffset
	}
	g.NextPage()
	k, v = g.page.Next()
	return k, v, buf, g.currentPageOffset
}
func (g *PagedReader) Skip() (uint64, int) {
	v, offset := g.Next(nil)
	return offset, len(v)
}

var workers = dbg.EnvInt("PAGED_WRITER_WORKERS", 1)

func NewPagedWriter(ctx context.Context, parent CompressorI, compressionEnabled bool) *PagedWriter {
	return NewPagedWriterWithWorkers(ctx, parent, compressionEnabled, workers)
}

func NewPagedWriterWithWorkers(ctx context.Context, parent CompressorI, compressionEnabled bool, numWorkers int) *PagedWriter {
	pw := &PagedWriter{
		parent:             parent,
		pageSize:           parent.GetValuesOnCompressedPage(),
		compressionEnabled: compressionEnabled,
		ctx:                ctx,
		numWorkers:         numWorkers,
	}
	if compressionEnabled && pw.pageSize > 1 {
		if pw.numWorkers > 1 {
			pw.initWorkers()
		}
	}
	return pw
}

type CompressorI interface {
	io.Writer
	Close()
	Compress() error
	Count() int
	FileName() string
	SetMetadata(data []byte)
	GetValuesOnCompressedPage() int
}
type PagedWriter struct {
	parent             CompressorI
	pageSize           int
	keys, vals         []byte
	kLengths, vLengths []uint32

	compressionBuf     []byte
	compressionEnabled bool

	pairs int

	numWorkers      int
	workCh          chan *pageWorkItem
	resultCh        chan *pageResult
	eg              *errgroup.Group     // tracks workers + reducer; cancels all on first error
	egCtx           context.Context     // cancelled on first worker/reducer error
	seqIn           int                 // next seq to assign to work item
	seqOut          int                 // next seq to write to parent
	workersShutdown bool                // tracks if workers have been shut down
	pendingResults  map[int]*pageResult // out-of-order results waiting for seqOut
	ctx             context.Context     // caller context for cancellation

	// Metrics (optional, for diagnostics and testing)
	pagesCompressed int // number of pages processed through workers
}

func (c *PagedWriter) initWorkers() {
	queueDepth := c.numWorkers * 2
	c.workCh = make(chan *pageWorkItem, queueDepth)
	c.resultCh = make(chan *pageResult, queueDepth)
	c.pendingResults = make(map[int]*pageResult, queueDepth)
	c.eg, c.egCtx = errgroup.WithContext(c.ctx)

	var workerWg sync.WaitGroup
	workerWg.Add(c.numWorkers)
	for range c.numWorkers {
		c.eg.Go(func() error {
			defer workerWg.Done()
			return c.compressionWorker(c.egCtx)
		})
	}
	go func() { workerWg.Wait(); close(c.resultCh) }()
	c.eg.Go(c.reducer)
}

func (c *PagedWriter) compressionWorker(ctx context.Context) error {
	processItem := func(item *pageWorkItem) {
		defer putPageWorkItem(item)

		// Build full page: header + keys + vals (all work in parallel worker)
		totalSize := 1 + len(item.kLengths)*2*4 + len(item.keys) + len(item.vals)
		item.uncompressedData = pageHeaderTo(item.uncompressedData[:0], item.kLengths, item.vLengths, totalSize)
		item.uncompressedData = append(item.uncompressedData, item.keys...)
		item.uncompressedData = append(item.uncompressedData, item.vals...)

		result := getPageResult()
		result.seq = item.seq

		// Compress directly into result.data (no extra copy, each result owns its buffer)
		_, result.data = compress.EncodeZstdIfNeed(result.data[:0], item.uncompressedData, c.compressionEnabled)

		// Send result, respecting context cancellation
		select {
		case c.resultCh <- result:
		case <-ctx.Done():
			putPageResult(result)
		}
	}

	for {
		select {
		case item, ok := <-c.workCh:
			if !ok {
				return nil // channel closed
			}
			processItem(item)

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *PagedWriter) reducer() error {
	for r := range c.resultCh {
		c.pendingResults[r.seq] = r
		if err := c.writeInOrder(); err != nil {
			drainResultCh(c.resultCh)
			drainPendingResults(c.pendingResults)
			return err
		}
	}
	return nil
}

func (c *PagedWriter) writeInOrder() error {
	for {
		r, ok := c.pendingResults[c.seqOut]
		if !ok {
			return nil
		}
		if _, err := c.parent.Write(r.data); err != nil {
			return err
		}
		delete(c.pendingResults, c.seqOut)
		putPageResult(r)
		c.seqOut++
		c.pagesCompressed++
	}
}

func (c *PagedWriter) Empty() bool              { return c.pairs == 0 }
func (c *PagedWriter) IsAsyncCompression() bool { return c.workCh != nil }
func (c *PagedWriter) PagesCompressed() int     { return c.pagesCompressed }
func (c *PagedWriter) Close() {
	c.parent.Close()
}
func (c *PagedWriter) Compress() error {
	// Flush any remaining unwritten page data
	if err := c.Flush(); err != nil {
		return err
	}
	return c.parent.Compress()
}

func (c *PagedWriter) Count() int {
	if c.pageSize <= 1 {
		return c.parent.Count()
	}
	return c.pairs * 2
}

func (c *PagedWriter) FileName() string { return c.parent.FileName() }

func (c *PagedWriter) writePage() error {
	// When page-level compression is enabled, defer compression to Compress() method
	if !c.compressionEnabled {
		bts, ok := c.bytes()
		c.resetPage()
		if !ok {
			return nil
		}
		_, err := c.parent.Write(bts)
		return err
	}

	// Synchronous path (single-threaded or disabled workers)
	if c.workCh == nil {
		uncompressedPage, ok := c.bytesUncompressed()
		c.resetPage()
		if !ok {
			return nil
		}

		var compressedPage []byte
		c.compressionBuf, compressedPage = compress.EncodeZstdIfNeed(c.compressionBuf[:0], uncompressedPage, c.compressionEnabled)
		if _, err := c.parent.Write(compressedPage); err != nil {
			return err
		}
		return nil
	}

	// Async path: swap all buffers to worker (zero-copy producer)
	if len(c.kLengths) == 0 {
		return nil
	}

	item := getPageWorkItem()

	// Swap all buffers: give ours to worker, take recycled ones
	item.keys, c.keys = c.keys, item.keys
	item.vals, c.vals = c.vals, item.vals
	item.kLengths, c.kLengths = c.kLengths, item.kLengths
	item.vLengths, c.vLengths = c.vLengths, item.vLengths

	c.resetPage()

	item.seq = c.seqIn
	c.seqIn++

	select {
	case c.workCh <- item:
		return nil
	case <-c.egCtx.Done():
		putPageWorkItem(item)
		return c.egCtx.Err()
	}
}

func (c *PagedWriter) Add(k, v []byte) (err error) {
	if c.pageSize <= 1 {
		_, err = c.parent.Write(v)
		return err
	}

	c.pairs++
	c.kLengths = append(c.kLengths, uint32(len(k)))
	c.vLengths = append(c.vLengths, uint32(len(v)))
	c.keys = append(c.keys, k...)
	c.vals = append(c.vals, v...)
	isFull := c.pairs%c.pageSize == 0
	if isFull {
		return c.writePage()
	}
	return nil
}

func (c *PagedWriter) resetPage() {
	c.kLengths, c.vLengths = c.kLengths[:0], c.vLengths[:0]
	c.keys, c.vals = c.keys[:0], c.vals[:0]
}
func (c *PagedWriter) Flush() error {
	if c.pageSize <= 1 {
		return nil
	}
	// Flush partial page
	if err := c.writePage(); err != nil {
		return err
	}
	if c.workCh == nil {
		c.resetPage()
		return nil
	}
	// Signal workers to stop; reducer drains resultCh and writes in order
	if !c.workersShutdown {
		close(c.workCh)
		c.workersShutdown = true
	}
	defer func() {
		c.eg.Wait() //nolint:errcheck
		c.resetPage()
	}()
	return c.eg.Wait()
}

func (c *PagedWriter) bytesUncompressed() (wholePage []byte, notEmpty bool) {
	if len(c.kLengths) == 0 {
		return nil, false
	}

	c.keys = append(c.keys, c.vals...)
	keysAndVals := c.keys

	wholePage = pageHeaderTo(c.vals, c.kLengths, c.vLengths, 0)
	wholePage = append(wholePage, keysAndVals...)

	return wholePage, true
}

// pageHeaderTo encodes page header (count + key/value lengths) into buf.
// capacityHint, if > 0, pre-allocates buf to that size (for callers that know the full page size).
func pageHeaderTo(buf []byte, kLengths, vLengths []uint32, capacityHint int) []byte {
	headerSize := 1 + len(kLengths)*2*4
	if capacityHint > headerSize {
		buf = growslice(buf, capacityHint)[:headerSize]
	} else {
		buf = growslice(buf, headerSize)
	}
	buf[0] = uint8(len(kLengths))
	lensBuf := buf[1:]
	for i, l := range kLengths {
		binary.BigEndian.PutUint32(lensBuf[i*4:(i+1)*4], l)
	}
	lensBuf = lensBuf[len(kLengths)*4:]
	for i, l := range vLengths {
		binary.BigEndian.PutUint32(lensBuf[i*4:(i+1)*4], l)
	}
	return buf
}

func (c *PagedWriter) bytes() (wholePage []byte, notEmpty bool) {
	//TODO: alignment,compress+alignment
	wholePage, notEmpty = c.bytesUncompressed()
	if !notEmpty {
		return nil, false
	}
	c.compressionBuf, wholePage = compress.EncodeZstdIfNeed(c.compressionBuf[:0], wholePage, c.compressionEnabled)
	return wholePage, true
}

func (c *PagedWriter) DisableFsync() {
	if casted, ok := c.parent.(disableFsycn); ok {
		casted.DisableFsync()
	}
}

func (c *PagedWriter) SetMetadata(metadata []byte) {
	c.parent.SetMetadata(metadata)
}

type disableFsycn interface {
	DisableFsync()
}

// growslice ensures b has the wanted length by either expanding it to its capacity
// or allocating a new slice if b has insufficient capacity.
func growslice(b []byte, wantLength int) []byte {
	if cap(b) >= wantLength {
		return b[:wantLength]
	}
	return make([]byte, wantLength, max(wantLength, 2*cap(b)))
}

// Global pools for page work items and results - optimized for GC
var (
	pageWorkItemPool = sync.Pool{New: func() any { return &pageWorkItem{} }}
	pageResultPool   = sync.Pool{New: func() any { return &pageResult{} }}
)

func getPageWorkItem() *pageWorkItem { return pageWorkItemPool.Get().(*pageWorkItem) }
func putPageWorkItem(item *pageWorkItem) {
	if item == nil {
		return
	}
	item.seq = 0
	item.uncompressedData = item.uncompressedData[:0]
	item.keys = item.keys[:0]
	item.vals = item.vals[:0]
	item.kLengths = item.kLengths[:0]
	item.vLengths = item.vLengths[:0]
	pageWorkItemPool.Put(item)
}

func getPageResult() *pageResult { return pageResultPool.Get().(*pageResult) }
func putPageResult(r *pageResult) {
	r.seq = 0
	r.data = r.data[:0]
	pageResultPool.Put(r)
}

func drainResultCh(ch chan *pageResult) {
	for {
		select {
		case r := <-ch:
			putPageResult(r)
		default:
			return
		}
	}
}

func drainPendingResults(m map[int]*pageResult) {
	for _, r := range m {
		putPageResult(r)
	}
}
