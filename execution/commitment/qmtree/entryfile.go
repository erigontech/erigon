package qmtree

import (
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment/qmtree/hpfile"
)

// EntryFile implements EntryStorage backed by an hpfile.
// Each entry is stored as its 32-byte hash. The position returned by
// Append is the byte offset in the virtual file, used by TwigFile to
// record the end position of each twig's last entry.
type EntryFile struct {
	file *hpfile.File
}

// entrySize is the number of bytes stored per entry: three 32-byte hash components
// (preStateHash, stateChangeHash, transitionHash). The leaf hash itself is computed
// on read from the chain of component hashes, so it is not stored.
const entrySize = 3 * 32 // 96 bytes
// entryHashSize is kept as an alias used by tree.go for truncation math.
const entryHashSize = entrySize

func NewEntryFile(bufferSize int, segmentSize uint64, dirName string) (*EntryFile, error) {
	file, err := hpfile.NewFile(bufferSize, segmentSize, dirName)
	if err != nil {
		return nil, err
	}
	return &EntryFile{file: file}, nil
}

func (ef *EntryFile) Append(entry Entry) int64 {
	pre, stateChange, transition := entry.Components()
	var buf [entrySize]byte
	copy(buf[0:32], pre[:])
	copy(buf[32:64], stateChange[:])
	copy(buf[64:96], transition[:])
	pos, err := ef.file.Append(buf[:])
	if err != nil {
		panic(fmt.Sprintf("EntryFile.Append: %v", err))
	}
	return int64(pos + entrySize) // return end position (matches QMDB convention)
}

// ReadComponents reads the three stored hash components for a given serial number.
// The prevLeaf is not stored; callers derive it from the hash of entry sn-1.
func (ef *EntryFile) ReadComponents(sn uint64) (pre, stateChange, transition common.Hash, err error) {
	var buf [entrySize]byte
	off := sn * entrySize
	if _, err = ef.file.ReadAt(buf[:], off); err != nil {
		return
	}
	copy(pre[:], buf[0:32])
	copy(stateChange[:], buf[32:64])
	copy(transition[:], buf[64:96])
	return
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
