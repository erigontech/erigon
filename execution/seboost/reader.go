package seboost

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/erigontech/erigon/common/log/v3"
)

// Reader reads seboost txdeps binary files lazily and sequentially.
type Reader struct {
	dir    string
	logger log.Logger

	// current file state
	curFile  *os.File
	curStart uint64
	curEnd   uint64
	fileOpen bool

	// cache of recently decoded block
	cachedBlock uint64
	cachedDeps  [][]int
	hasCached   bool
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
func (r *Reader) GetDeps(blockNum uint64) ([][]int, error) {
	// check cache
	if r.hasCached && r.cachedBlock == blockNum {
		return r.cachedDeps, nil
	}

	// ensure correct file is open
	start, end := fileRange(blockNum)
	if !r.fileOpen || start != r.curStart {
		if err := r.openFile(start, end); err != nil {
			// file doesn't exist: graceful fallback
			return nil, nil //nolint:nilerr
		}
	}

	// scan through the file looking for blockNum
	deps, err := r.scanForBlock(blockNum)
	if err != nil {
		return nil, nil //nolint:nilerr
	}

	if deps != nil {
		r.cachedBlock = blockNum
		r.cachedDeps = deps
		r.hasCached = true
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
		r.fileOpen = false
	}

	name := fmt.Sprintf("seboost-txdeps-%d-%d.bin", start, end)
	path := filepath.Join(r.dir, name)
	f, err := os.Open(path)
	if err != nil {
		return err
	}

	// validate header
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
	r.curStart = start
	r.curEnd = end
	r.fileOpen = true
	r.hasCached = false

	return nil
}

func (r *Reader) scanForBlock(blockNum uint64) ([][]int, error) {
	if r.curFile == nil {
		return nil, fmt.Errorf("no file open")
	}

	// re-read from start of data (after header)
	if _, err := r.curFile.Seek(25, 0); err != nil {
		return nil, err
	}

	// read all remaining data
	data, err := io.ReadAll(r.curFile)
	if err != nil {
		return nil, err
	}

	off := 0
	for off < len(data) {
		bn, k := binary.Uvarint(data[off:])
		if k <= 0 {
			break
		}
		off += k

		tc, k := binary.Uvarint(data[off:])
		if k <= 0 {
			break
		}
		off += k
		txCount := int(tc)

		if off >= len(data) {
			break
		}
		format := data[off]
		off++

		// compute payload size
		var payloadSize int
		switch format {
		case formatBitmap:
			totalBits := txCount * (txCount - 1) / 2
			payloadSize = (totalBits + 7) / 8
		case formatSparse:
			// must parse to find the end
			pOff := off
			for i := 0; i < txCount; i++ {
				count, k := binary.Uvarint(data[pOff:])
				pOff += k
				for d := 0; d < int(count); d++ {
					_, k = binary.Uvarint(data[pOff:])
					pOff += k
				}
			}
			payloadSize = pOff - off
		default:
			return nil, fmt.Errorf("seboost: unknown format byte %d", format)
		}

		if off+payloadSize > len(data) {
			break
		}

		payload := data[off : off+payloadSize]
		off += payloadSize

		if bn == blockNum {
			switch format {
			case formatBitmap:
				return decodeBitmap(payload, txCount), nil
			case formatSparse:
				return decodeSparse(payload, txCount), nil
			}
		}
	}

	return nil, nil
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
