package qmtree

import "github.com/erigontech/erigon-lib/common"

const ENTRY_FIXED_LENGTH int = 1 + 3 + 1 + 32 + 8 + 8
const ENTRY_BASE_LENGTH int = ENTRY_FIXED_LENGTH

type Entry interface {
	Hash() common.Hash
	SerialNumber() uint64
	Len() int64
}

type NullEntry struct {
}

func (_ NullEntry) Hash() common.Hash {
	return common.Hash{}
}

type EntryStorage interface {
	Append(entry Entry) (pos int64)
	Flush()
	Close()
	Size() int64
	PruneHead(offset int64)
	Truncate(size int64)

	CloneTemp() EntryStorage
}
