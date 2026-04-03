package qmtree

import "github.com/erigontech/erigon/common"

const ENTRY_FIXED_LENGTH int = 1 + 3 + 1 + 32 + 8 + 8
const ENTRY_BASE_LENGTH int = ENTRY_FIXED_LENGTH

type Entry interface {
	Hash() common.Hash
	TxNum() uint64
	Len() int64
	// Components returns the three raw hash inputs used to compute the leaf hash.
	// These are stored in the entry file (96 bytes) instead of the derived hash,
	// eliminating a separate leafdata file. PreviousLeafHash is not stored here
	// as it is derivable by chaining leaf hashes from sn-1.
	Components() (pre, stateChange, transition common.Hash)
}

type NullEntry struct {
}

// nullEntryHash is SHA256 of the 53-byte null entry serialization:
// [0,0,0,0, 0, zeros_32, version=-2 (LE i64), sn=u64::MAX (LE u64)]
// This matches the Rust QMDB null_entry hash.
var nullEntryHash = common.Hash{
	0xca, 0x23, 0x37, 0x69, 0x10, 0x33, 0xab, 0x0a,
	0x24, 0xc1, 0x0f, 0xbc, 0x70, 0xb4, 0x9b, 0xea,
	0x8c, 0x59, 0x78, 0xdb, 0x1a, 0x0e, 0xc6, 0x51,
	0x0e, 0x7e, 0x97, 0xf5, 0x28, 0x30, 0x1c, 0x39,
}

func (_ NullEntry) Hash() common.Hash                                          { return nullEntryHash }
func (_ NullEntry) TxNum() uint64                                               { return ^uint64(0) }
func (_ NullEntry) Len() int64                                                  { return 0 }
func (_ NullEntry) Components() (pre, stateChange, transition common.Hash)      { return }

type EntryStorage interface {
	Append(entry Entry) (pos int64)
	Flush()
	Close()
	Size() int64
	PruneHead(offset int64)
	Truncate(size int64)

	CloneTemp() EntryStorage
}
