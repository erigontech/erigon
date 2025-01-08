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

func (v *VisibleSegment) Get(tsNum uint64) ([]byte, error) {
	idxSlot := v.indexes[0]

	if idxSlot == nil {
		return nil, nil
	}
	offset := idxSlot.OrdinalLookup(tsNum - idxSlot.BaseDataID())

	gg := v.Decompressor.MakeGetter()
	gg.Reset(offset)
	if !gg.HasNext() {
		return nil, nil
	}
	var buf []byte
	buf, _ = gg.Next(buf)
	if len(buf) == 0 {
		return nil, nil
	}

	return buf, nil
}
