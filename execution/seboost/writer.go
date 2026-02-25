package seboost

import (
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/erigontech/erigon/common/log/v3"
)

// Writer writes seboost txdeps binary files and optional CSV stats.
type Writer struct {
	dir    string
	logger log.Logger

	// current file state
	curFile    *os.File
	curStart   uint64
	curEnd     uint64
	blockCount uint32
	fileOpen   bool

	// CSV stats
	csvFile   *os.File
	csvWriter *csv.Writer
}

// NewWriter creates a new seboost Writer that writes files into dir.
// It also creates a CSV stats file at csvPath.
func NewWriter(dir string, csvPath string, logger log.Logger) (*Writer, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("seboost: mkdir %s: %w", dir, err)
	}

	w := &Writer{
		dir:    dir,
		logger: logger,
	}

	if csvPath != "" {
		if err := os.MkdirAll(filepath.Dir(csvPath), 0755); err != nil {
			return nil, fmt.Errorf("seboost: mkdir csv dir: %w", err)
		}
		f, err := os.Create(csvPath)
		if err != nil {
			return nil, fmt.Errorf("seboost: create csv %s: %w", csvPath, err)
		}
		w.csvFile = f
		w.csvWriter = csv.NewWriter(f)
		if err := w.csvWriter.Write([]string{"blockNum", "txCount", "bitmapBytes", "sparseBytes", "chosenBytes"}); err != nil {
			return nil, err
		}
	}

	return w, nil
}

// WriteBlock writes dependency data for a single block. deps is the output
// of state.GetDep(), txCount is the total number of entries (system tx + user txs).
func (w *Writer) WriteBlock(blockNum uint64, deps map[int]map[int]bool, txCount int) error {
	if txCount < minTxCount {
		return nil
	}

	start, end := fileRange(blockNum)

	// open new file if needed
	if !w.fileOpen || start != w.curStart {
		if err := w.closeCurrentFile(); err != nil {
			return err
		}
		if err := w.openFile(start, end); err != nil {
			return err
		}
	}

	// choose encoding
	payload, format, bitmapBytes, sparseBytes := chooseEncoding(deps, txCount)
	chosenBytes := len(payload)

	// write block entry
	tmp := make([]byte, binary.MaxVarintLen64)

	k := binary.PutUvarint(tmp, blockNum)
	if _, err := w.curFile.Write(tmp[:k]); err != nil {
		return err
	}

	k = binary.PutUvarint(tmp, uint64(txCount))
	if _, err := w.curFile.Write(tmp[:k]); err != nil {
		return err
	}

	if _, err := w.curFile.Write([]byte{format}); err != nil {
		return err
	}

	if _, err := w.curFile.Write(payload); err != nil {
		return err
	}

	w.blockCount++

	// CSV stats
	if w.csvWriter != nil {
		if err := w.csvWriter.Write([]string{
			strconv.FormatUint(blockNum, 10),
			strconv.Itoa(txCount),
			strconv.Itoa(bitmapBytes),
			strconv.Itoa(sparseBytes),
			strconv.Itoa(chosenBytes),
		}); err != nil {
			return err
		}
	}

	// log
	zeroDeps := zeroDepsCount(deps, txCount)
	w.logger.Info("seboost: generated txdeps", "block", blockNum, "txCount", txCount, "bytes", chosenBytes, "zeroDeps", zeroDeps)

	return nil
}

// Close flushes and closes all open files.
func (w *Writer) Close() error {
	if err := w.closeCurrentFile(); err != nil {
		return err
	}
	if w.csvWriter != nil {
		w.csvWriter.Flush()
	}
	if w.csvFile != nil {
		return w.csvFile.Close()
	}
	return nil
}

func (w *Writer) openFile(start, end uint64) error {
	name := fmt.Sprintf("seboost-txdeps-%d-%d.bin", start, end)
	path := filepath.Join(w.dir, name)
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("seboost: create %s: %w", path, err)
	}

	w.curFile = f
	w.curStart = start
	w.curEnd = end
	w.blockCount = 0
	w.fileOpen = true

	// write header placeholder (will be updated on close)
	if err := w.writeHeader(); err != nil {
		return err
	}

	return nil
}

func (w *Writer) writeHeader() error {
	var hdr [4 + 1 + 8 + 8 + 4]byte
	copy(hdr[0:4], fileMagic[:])
	hdr[4] = fileVersion
	binary.LittleEndian.PutUint64(hdr[5:13], w.curStart)
	binary.LittleEndian.PutUint64(hdr[13:21], w.curEnd)
	binary.LittleEndian.PutUint32(hdr[21:25], w.blockCount)

	if _, err := w.curFile.Seek(0, 0); err != nil {
		return err
	}
	_, err := w.curFile.Write(hdr[:])
	return err
}

func (w *Writer) closeCurrentFile() error {
	if !w.fileOpen || w.curFile == nil {
		return nil
	}

	// update header with final block count
	if err := w.writeHeader(); err != nil {
		return err
	}

	if err := w.curFile.Close(); err != nil {
		return err
	}

	w.curFile = nil
	w.fileOpen = false
	return nil
}
