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
	version                uint8
	Decompressor           *seg.Decompressor
	filePath               string
	indexes                []*recsplit.Index
	expectedCountOfIndexes int // count of indexes expected. might be different from len(indexes) since it might not be constructed yet.
	enum                   ApEnum
	refcount               atomic.Int32
	canDelete              atomic.Bool
}

type VisibleSegment struct {
	src *DirtySegment
}

type VisibleSegments []VisibleSegment

func (v *VisibleSegment) Get(tsNum TsNum) ([]byte, error) {
	idxSlot := v.src.indexes[0]

	if idxSlot == nil {
		return nil, nil
	}
	offset := idxSlot.OrdinalLookup(uint64(tsNum) - idxSlot.BaseDataID())

	gg := v.src.Decompressor.MakeGetter()
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

func (v *VisibleSegment) Src() *DirtySegment {
	return v.src
}

func (v *VisibleSegment) GetFirstTsNum() uint64 {
	// TODO: store first tsnum in snapshot...
	// https://github.com/erigontech/erigon/issues/13342 talks about storing first tsNum
	// in snapshots, but lastTsNum can be easily gotten from count and firstTsNum.
	return 0
}

func (v *DirtySegment) GetLastTsNum() uint64 {
	// TODO: store first tsnum in snapshot...
	// https://github.com/erigontech/erigon/issues/13342 talks about storing first tsNum
	// in snapshots, but lastTsNum can be easily gotten from count and firstTsNum.
	return 0
}

func (v *DirtySegment) isSubsetOf(w *DirtySegment) bool {
	return (w.from <= v.from && v.to <= w.to) && (w.from != v.from || v.to != w.to)
}

func (v *DirtySegment) closeFiles() {
	if v.Decompressor != nil {
		v.Decompressor.Close()
		v.Decompressor = nil
	}
	for _, idx := range v.indexes {
		idx.Close()
	}
	v.indexes = nil
}
