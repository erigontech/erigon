package appendables

import (
	"sync/atomic"

	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
)

type DirtySegment struct {
	Range
	version      uint8
	Decompressor *seg.Decompressor
	filePath     string
	indexes      []*recsplit.Index
	enum         ApEnum
	refcount     atomic.Int32
	canDelete    atomic.Bool
}

type VisibleSegment struct {
	DirtySegment
}

type VisibleSegments []VisibleSegment
