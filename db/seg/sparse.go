package seg

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

// SparseDecompressor provides on-demand access to compressed segment data via
// an io.ReadSeeker (typically backed by a BitTorrent torrent.Reader).
// It parses the file header (Huffman dictionaries) once, then reads individual
// records on demand without requiring the full file to be present locally.
//
// It satisfies the same usage pattern as Decompressor: callers use MakeGetter()
// to create a Getter, then Reset(offset) + Next() to extract records. The
// difference is that the underlying data comes from the network on demand
// rather than from a memory-mapped local file.
type SparseDecompressor struct {
	reader   io.ReadSeeker
	readerMu sync.Mutex // protects reader (not thread-safe)
	fileSize int64
	fileName string

	// Parsed from file header (loaded once)
	initOnce sync.Once
	initErr  error
	dict     *patternTable
	posDict  *posTable
	patArena *patternArena
	posArena *posArena

	wordsStart uint64
	wordsCount uint64
}

// NewSparseDecompressor creates a decompressor backed by an io.ReadSeeker.
// The reader is typically a torrent.Reader that fetches pieces on demand.
// fileSize is the total size of the segment file.
func NewSparseDecompressor(reader io.ReadSeeker, fileSize int64, fileName string) *SparseDecompressor {
	return &SparseDecompressor{
		reader:   reader,
		fileSize: fileSize,
		fileName: fileName,
	}
}

// MakeGetter creates a Getter that can read records from this sparse segment.
// Unlike the standard Decompressor.MakeGetter(), the returned Getter's data
// is NOT pre-loaded. Instead, callers must use SparseGet() which handles
// on-demand data fetching internally.
//
// The Getter returned here has an empty data slice — it is only usable via
// the SparseGet() convenience method on the SparseDecompressor itself.
// This is a deliberate POC limitation. See DESIGN_NOTES.md for details.
func (sd *SparseDecompressor) MakeGetter() *Getter {
	sd.init()
	return &Getter{
		patternDict: sd.dict,
		posDict:     sd.posDict,
		data:        nil, // will be populated per-read via SparseGet
		dataLen:     0,
		fName:       sd.fileName,
	}
}

// SparseGet reads and decompresses a single record at the given word offset
// (relative to wordsStart). This is the primary access method for sparse segments.
func (sd *SparseDecompressor) SparseGet(wordOffset uint64) ([]byte, error) {
	if err := sd.init(); err != nil {
		return nil, err
	}

	absOffset := int64(sd.wordsStart + wordOffset)
	windowSize := int64(64 * 1024) // 64KB — generous for most records
	if absOffset+windowSize > sd.fileSize {
		windowSize = sd.fileSize - absOffset
	}
	if windowSize <= 0 {
		return nil, fmt.Errorf("[sparse] offset %d beyond file size %d", absOffset, sd.fileSize)
	}

	data, err := sd.readRange(absOffset, windowSize)
	if err != nil {
		return nil, fmt.Errorf("[sparse] read at offset %d: %w", wordOffset, err)
	}

	g := &Getter{
		patternDict: sd.dict,
		posDict:     sd.posDict,
		data:        data,
		dataLen:     uint64(len(data)),
		fName:       sd.fileName,
	}
	if sd.posDict != nil {
		g.posMask = sd.posDict.mask
		g.posEntries = sd.posDict.entries
	}

	if !g.HasNext() {
		return nil, nil
	}
	buf, _ := g.Next(nil)
	return buf, nil
}

func (sd *SparseDecompressor) FileName() string { return sd.fileName }

func (sd *SparseDecompressor) Count() int {
	sd.init()
	return int(sd.wordsCount)
}

func (sd *SparseDecompressor) Close() {
	if closer, ok := sd.reader.(io.Closer); ok {
		closer.Close() //nolint:errcheck
	}
}

// init parses the file header (dictionaries) from the reader. Called once.
func (sd *SparseDecompressor) init() error {
	sd.initOnce.Do(func() {
		sd.initErr = sd.parseHeader()
	})
	return sd.initErr
}

func (sd *SparseDecompressor) parseHeader() error {
	// Read generous header chunk. Dictionaries are typically <100KB.
	headerBuf, err := sd.readRange(0, min(512*1024, sd.fileSize))
	if err != nil {
		return fmt.Errorf("[sparse] read header: %w", err)
	}
	if len(headerBuf) < compressedMinSize {
		return fmt.Errorf("[sparse] file too small: %d bytes", len(headerBuf))
	}

	data := headerBuf
	version := data[0]
	headerSkip := uint64(0) // bytes skipped from start of file (version, flags)

	if version == FileCompressionFormatV1 {
		featureFlags := FeatureFlagBitmask(data[1])
		data = data[2:]
		headerSkip = 2
		if featureFlags.Has(PageLevelCompressionEnabled) {
			data = data[1:]
			headerSkip = 3
		}
	}

	if len(data) < 24 {
		return fmt.Errorf("[sparse] header too short")
	}

	sd.wordsCount = binary.BigEndian.Uint64(data[:8])
	pos := uint64(24)
	dictSize := binary.BigEndian.Uint64(data[16:24])

	// Ensure we have enough data for both dictionaries
	if err := sd.ensureData(&data, &headerBuf, version, headerSkip, pos+dictSize+64*1024); err != nil {
		return err
	}

	// --- Pattern dictionary ---
	if pos+dictSize > uint64(len(data)) {
		return fmt.Errorf("[sparse] pattern dict overflows buffer")
	}
	dictData := data[pos : pos+dictSize]
	sd.dict, sd.patArena, err = sd.buildPatternDict(dictData, dictSize)
	if err != nil {
		return err
	}
	pos += dictSize

	// --- Position dictionary ---
	if pos+8 > uint64(len(data)) {
		return fmt.Errorf("[sparse] pos dict size overflows buffer")
	}
	posDictSize := binary.BigEndian.Uint64(data[pos : pos+8])
	pos += 8

	if err := sd.ensureData(&data, &headerBuf, version, headerSkip, pos+posDictSize+1024); err != nil {
		return err
	}
	if pos+posDictSize > uint64(len(data)) {
		return fmt.Errorf("[sparse] pos dict overflows buffer")
	}

	posData := data[pos : pos+posDictSize]
	sd.posDict, sd.posArena, err = sd.buildPosDict(posData, posDictSize)
	if err != nil {
		return err
	}

	sd.wordsStart = headerSkip + pos + posDictSize
	return nil
}

// ensureData re-reads header if needed to cover the required size.
// needed is relative to the version-stripped data slice.
func (sd *SparseDecompressor) ensureData(data *[]byte, headerBuf *[]byte, version uint8, headerSkip uint64, needed uint64) error {
	if needed <= uint64(len(*data)) {
		return nil
	}
	// Need more data — add headerSkip to account for version/flag bytes
	readSize := needed + headerSkip
	buf, err := sd.readRange(0, min(int64(readSize), sd.fileSize))
	if err != nil {
		return fmt.Errorf("[sparse] re-read header for size %d: %w", needed, err)
	}
	*headerBuf = buf
	d := buf
	if version == FileCompressionFormatV1 {
		featureFlags := FeatureFlagBitmask(d[1])
		d = d[2:]
		if featureFlags.Has(PageLevelCompressionEnabled) {
			d = d[1:]
		}
	}
	*data = d
	return nil
}

func (sd *SparseDecompressor) buildPatternDict(dictData []byte, dictSize uint64) (*patternTable, *patternArena, error) {
	if dictSize == 0 {
		return nil, nil, nil
	}

	var depths []uint64
	var patterns [][]byte
	var dictPos uint64
	var patternMaxDepth uint64

	for dictPos < dictSize {
		depth, ns := binary.Uvarint(dictData[dictPos:])
		if depth > maxAllowedDepth {
			return nil, nil, fmt.Errorf("[sparse] pattern depth %d > max %d", depth, maxAllowedDepth)
		}
		depths = append(depths, depth)
		if depth > patternMaxDepth {
			patternMaxDepth = depth
		}
		dictPos += uint64(ns)
		l, n := binary.Uvarint(dictData[dictPos:])
		dictPos += uint64(n)
		pat := make([]byte, l)
		copy(pat, dictData[dictPos:dictPos+l])
		patterns = append(patterns, pat)
		dictPos += l
	}

	bitLen := int(patternMaxDepth)
	if bitLen > 9 {
		bitLen = 9
	}
	_, extraSlots, numSubTables := countHuffmanArena(depths, 0, 0, patternMaxDepth)
	arena := &patternArena{
		codewords: make([]codeword, len(patterns)+numSubTables),
		tables:    make([]patternTable, 1+numSubTables),
		slots:     make([]*codeword, (1<<bitLen)+extraSlots),
	}
	table := arena.allocTable(bitLen)
	if _, err := buildCondensedPatternTable(table, depths, patterns, 0, 0, 0, patternMaxDepth, arena); err != nil {
		return nil, nil, fmt.Errorf("[sparse] build pattern table: %w", err)
	}
	return table, arena, nil
}

func (sd *SparseDecompressor) buildPosDict(posData []byte, posDictSize uint64) (*posTable, *posArena, error) {
	if posDictSize == 0 {
		return nil, nil, nil
	}

	var posDepths []uint64
	var poss []uint64
	var posMaxDepth uint64
	var dictPos uint64

	for dictPos < posDictSize {
		depth, ns := binary.Uvarint(posData[dictPos:])
		if depth > maxAllowedDepth {
			return nil, nil, fmt.Errorf("[sparse] pos depth %d > max %d", depth, maxAllowedDepth)
		}
		posDepths = append(posDepths, depth)
		if depth > posMaxDepth {
			posMaxDepth = depth
		}
		dictPos += uint64(ns)
		dp, n := binary.Uvarint(posData[dictPos:])
		dictPos += uint64(n)
		poss = append(poss, dp)
	}

	bitLen := int(posMaxDepth)
	if bitLen > 9 {
		bitLen = 9
	}
	_, extraSlots, numSubTables := countHuffmanArena(posDepths, 0, 0, posMaxDepth)
	totalSlots := (1 << bitLen) + extraSlots
	arena := &posArena{
		tables:     make([]posTable, 1+numSubTables),
		entriesArr: make([]posEntry, totalSlots),
		ptrsArr:    make([]*posTable, totalSlots),
	}
	table := arena.allocTable(bitLen)
	if _, err := buildPosTable(posDepths, poss, table, 0, 0, 0, posMaxDepth, arena); err != nil {
		return nil, nil, fmt.Errorf("[sparse] build pos table: %w", err)
	}
	return table, arena, nil
}

// readRange reads size bytes starting at offset from the underlying reader.
func (sd *SparseDecompressor) readRange(offset, size int64) ([]byte, error) {
	sd.readerMu.Lock()
	defer sd.readerMu.Unlock()

	if _, err := sd.reader.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	buf := make([]byte, size)
	n, err := io.ReadFull(sd.reader, buf)
	if err == io.ErrUnexpectedEOF {
		return buf[:n], nil
	}
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}
