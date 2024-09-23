package types

import "math"

type EntryType uint32

var (
	EntryTypeUnspecified EntryType = 0
	EntryTypeBatchStart  EntryType = 1
	EntryTypeL2Block     EntryType = 2
	EntryTypeL2Tx        EntryType = 3
	EntryTypeBatchEnd    EntryType = 4
	EntryTypeGerUpdate   EntryType = 5
	EntryTypeL2BlockEnd  EntryType = 6
	BookmarkEntryType    EntryType = 176
	EntryTypeNotFound    EntryType = math.MaxUint32
)
