package appendables

import (
	"sync/atomic"

	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
)

func DirtySegmentLess(i, j *DirtySegment) bool {
	if i.from != j.from {
		return i.from < j.from
	}
	if i.to != j.to {
		return i.to < j.to
	}
	return int(i.version) < int(j.version)
}

type Range struct {
	from, to uint64
}

func (r Range) From() uint64 { return r.from }
func (r Range) To() uint64   { return r.to }

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

func (v *DirtySegment) GetLastTsNum() uint64 {
	// TODO: store last tsnum in snapshot...
	return 0
}