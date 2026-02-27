package seboost

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/erigontech/erigon/common/log/v3"
)

// Reader reads seboost txdeps binary files sequentially.
// It keeps a forward-only position in the current file — no seeks on repeated calls.
// Designed for sequential block access (monotonically increasing blockNum).
type Reader struct {
	dir    string
	logger log.Logger

	// current file state
	curFile  *os.File
	curBuf   *bufio.Reader
	curStart uint64
	fileOpen bool

	// lastBlock is the last block number we scanned past, so we know
	// not to seek backward for block numbers already behind us.
	lastBlock uint64
}

// NewReader creates a new seboost Reader that reads files from dir.
func NewReader(dir string, logger log.Logger) *Reader {
	return &Reader{
		dir:    dir,
		logger: logger,
	}
}

// GetDeps returns dependency lists for the given block number.
// Returns nil, nil if the block is not present or has < 3 txs (graceful fallback).
// Each element in the returned slice is a list of dep indices for that entry.
// Optimised for sequential (monotonically increasing) block access: scans forward
// from the current file position without seeking back to the start.
func (r *Reader) GetDeps(blockNum uint64) ([][]int, error) {
	start, end := fileRange(blockNum)

	// Open or reopen file when crossing a 500k-block boundary, or if
	// blockNum is behind the current scan position (non-sequential caller).
	if !r.fileOpen || start != r.curStart || blockNum < r.lastBlock {
		if err := r.openFile(start, end); err != nil {
			// file doesn't exist: graceful fallback
			return nil, nil //nolint:nilerr
		}
	}

	deps, err := r.scanForwardToBlock(blockNum)
	if err != nil {
		return nil, nil //nolint:nilerr
	}

	if deps != nil {
		r.logger.Debug("seboost: loaded txdeps for block", "block", blockNum, "txCount", len(deps))
	}
	return deps, nil
}

// Close closes the reader's open file.
func (r *Reader) Close() error {
	if r.curFile != nil {
		return r.curFile.Close()
	}
	return nil
}

func (r *Reader) openFile(start, end uint64) error {
	if r.curFile != nil {
		r.curFile.Close() //nolint:errcheck
		r.curFile = nil
		r.curBuf = nil
		r.fileOpen = false
	}

	name := fmt.Sprintf("seboost-txdeps-%d-%d.bin", start, end)
	path := filepath.Join(r.dir, name)
	f, err := os.Open(path)
	if err != nil {
		return err
	}

	// validate header (25 bytes: 4 magic + 1 version + 8 startBlock + 8 endBlock + 4 blockCount)
	var hdr [25]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		f.Close() //nolint:errcheck
		return fmt.Errorf("seboost: read header %s: %w", path, err)
	}
	if hdr[0] != fileMagic[0] || hdr[1] != fileMagic[1] || hdr[2] != fileMagic[2] || hdr[3] != fileMagic[3] {
		f.Close() //nolint:errcheck
		return fmt.Errorf("seboost: invalid magic in %s", path)
	}
	if hdr[4] != fileVersion {
		f.Close() //nolint:errcheck
		return fmt.Errorf("seboost: unsupported version %d in %s", hdr[4], path)
	}

	r.curFile = f
	r.curBuf = bufio.NewReaderSize(f, 256*1024) // 256 KB read-ahead
	r.curStart = start
	r.fileOpen = true
	r.lastBlock = 0

	return nil
}

// scanForwardToBlock reads sequentially from the current file position until it
// finds blockNum. It never seeks backward — if the file position is already past
// blockNum, it returns nil (block not found without re-reading).
func (r *Reader) scanForwardToBlock(blockNum uint64) ([][]int, error) {
	if r.curBuf == nil {
		return nil, fmt.Errorf("seboost: no file open")
	}

	for {
		// Read blockNum varint
		bn, err := binary.ReadUvarint(r.curBuf)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil, nil // end of file, block not present
			}
			return nil, err
		}

		// Read txCount varint
		tc, err := binary.ReadUvarint(r.curBuf)
		if err != nil {
			return nil, err
		}
		txCount := int(tc)

		// Read format byte
		formatByte, err := r.curBuf.ReadByte()
		if err != nil {
			return nil, err
		}

		// Read and optionally decode payload
		if bn == blockNum {
			r.lastBlock = bn
			return r.readPayload(formatByte, txCount)
		}

		// Skip this block's payload and continue scanning forward
		if err := r.skipPayload(formatByte, txCount); err != nil {
			return nil, err
		}

		r.lastBlock = bn

		// Since blocks are stored in ascending order, stop early
		// if we've passed the target block number.
		if bn > blockNum {
			return nil, nil
		}
	}
}

// readPayload reads and decodes the payload for the current record.
func (r *Reader) readPayload(format byte, txCount int) ([][]int, error) {
	switch format {
	case formatBitmap:
		size := estimateBitmapSize(txCount)
		payload := make([]byte, size)
		if _, err := io.ReadFull(r.curBuf, payload); err != nil {
			return nil, err
		}
		return decodeBitmap(payload, txCount), nil

	case formatSparse:
		return r.readSparsePayload(txCount)

	default:
		return nil, fmt.Errorf("seboost: unknown format byte %d", format)
	}
}

// skipPayload discards the payload bytes for a block we don't need.
func (r *Reader) skipPayload(format byte, txCount int) error {
	switch format {
	case formatBitmap:
		size := estimateBitmapSize(txCount)
		_, err := r.curBuf.Discard(size)
		return err

	case formatSparse:
		// Parse and discard all varint entries
		for i := 0; i < txCount; i++ {
			count, err := binary.ReadUvarint(r.curBuf)
			if err != nil {
				return err
			}
			for d := uint64(0); d < count; d++ {
				if _, err := binary.ReadUvarint(r.curBuf); err != nil {
					return err
				}
			}
		}
		return nil

	default:
		return fmt.Errorf("seboost: unknown format byte %d", format)
	}
}

// readSparsePayload reads a sparse-encoded dep list from the buffered reader.
func (r *Reader) readSparsePayload(txCount int) ([][]int, error) {
	result := make([][]int, txCount)
	for i := 0; i < txCount; i++ {
		count, err := binary.ReadUvarint(r.curBuf)
		if err != nil {
			return nil, err
		}
		if count > 0 {
			deps := make([]int, count)
			for d := range deps {
				v, err := binary.ReadUvarint(r.curBuf)
				if err != nil {
					return nil, err
				}
				deps[d] = int(v)
			}
			result[i] = deps
		}
	}
	return result, nil
}

// ListFiles returns sorted seboost txdeps files in the directory.
func ListFiles(dir string) ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "seboost-txdeps-*.bin"))
	if err != nil {
		return nil, err
	}
	sort.Strings(matches)
	return matches, nil
}
