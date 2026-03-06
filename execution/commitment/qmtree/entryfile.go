package qmtree

import (
	"fmt"

	"github.com/erigontech/erigon/execution/commitment/qmtree/hpfile"
)

// EntryFile implements EntryStorage backed by an hpfile.
// Each entry is stored as its 32-byte hash. The position returned by
// Append is the byte offset in the virtual file, used by TwigFile to
// record the end position of each twig's last entry.
type EntryFile struct {
	file *hpfile.File
}

const entryHashSize = 32

func NewEntryFile(bufferSize int, segmentSize uint64, dirName string) (*EntryFile, error) {
	file, err := hpfile.NewFile(bufferSize, segmentSize, dirName)
	if err != nil {
		return nil, err
	}
	return &EntryFile{file: file}, nil
}

func (ef *EntryFile) Append(entry Entry) int64 {
	h := entry.Hash()
	pos, err := ef.file.Append(h[:])
	if err != nil {
		panic(fmt.Sprintf("EntryFile.Append: %v", err))
	}
	return int64(pos + entryHashSize) // return end position (matches QMDB convention)
}

func (ef *EntryFile) Flush() {
	if err := ef.file.Flush(false); err != nil {
		panic(fmt.Sprintf("EntryFile.Flush: %v", err))
	}
}

func (ef *EntryFile) Close() {
	ef.file.Close()
}

func (ef *EntryFile) Size() int64 {
	return ef.file.Size()
}

func (ef *EntryFile) PruneHead(offset int64) {
	if err := ef.file.PruneHead(uint64(offset)); err != nil {
		panic(fmt.Sprintf("EntryFile.PruneHead: %v", err))
	}
}

func (ef *EntryFile) Truncate(size int64) {
	if err := ef.file.Truncate(size); err != nil {
		panic(fmt.Sprintf("EntryFile.Truncate: %v", err))
	}
}

func (ef *EntryFile) CloneTemp() EntryStorage {
	return ef
}
