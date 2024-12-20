package appendables

import (
	"context"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/tidwall/btree"
)

type EENum string

const (
	BorSpans EENum = "appendable.borspans"
)

func (e EENum) GetSnapshotName(stepKeyFrom, stepKeyTo uint64) string {
	// assuming v1
	return ""
}

type Range struct {
	from, to uint64
}

func (r Range) From() uint64 { return r.from }
func (r Range) To() uint64   { return r.to }

type SDirtySegment struct {
	Range
	*seg.Decompressor
	version uint8
}
type SVisibleSegments struct{}

func DirtySegmentLess(i, j *SDirtySegment) bool {
	if i.from != j.from {
		return i.from < j.from
	}
	if i.to != j.to {
		return i.to < j.to
	}
	return int(i.version) < int(j.version)
}

type RoSnapshots[CollationType MinimalCollation] struct {
	enums          []EENum
	dirty          map[EENum]*btree.BTreeG[*SDirtySegment]
	visible        map[EENum]SVisibleSegments
	freezers       map[EENum]Freezer[CollationType]
	snapshotConfig map[EENum]SnapshotConfig
}

func NewRoSnapshots[CollationType MinimalCollation](enums []EENum) *RoSnapshots[CollationType] {
	ro := &RoSnapshots[CollationType]{
		enums:          enums,
		dirty:          make(map[EENum]*btree.BTreeG[*SDirtySegment]),
		visible:        make(map[EENum]SVisibleSegments),
		freezers:       make(map[EENum]Freezer[CollationType]),
		snapshotConfig: make(map[EENum]SnapshotConfig),
	}

	for _, enum := range ro.enums {
		ro.dirty[enum] = btree.NewBTreeGOptions[*SDirtySegment](DirtySegmentLess, btree.Options{
			NoLocks: true, Degree: 128,
		})
	}

	ro.recalcVisibleFiles()
	return ro
}

func (s *RoSnapshots[CollationType]) SetFreezer(enum EENum, freezer Freezer[CollationType]) {
	s.freezers[enum] = freezer
}

func (s *RoSnapshots[CollationType]) SetSnapConfig(enum EENum, cfg SnapshotConfig) {
	s.snapshotConfig[enum] = cfg
}

func (s *RoSnapshots[CollationType]) recalcVisibleFiles() {}

func (s *RoSnapshots[CollationType]) checkIfSnapshotCreatable(config *SnapshotConfig, stepKeyFrom, stepKeyTo uint64) error {
	// 1. ok so this needs db query
	// doesn't look like it belongs here...
	// should it be provided from outside?

	// so it seems like the only thing needed here is
	// size of snapshot...
	// while "number of elements in db" has to checked outside????

	// or I think this is abstracted to RoSnapshot interface and then
	// db or tx passed, and it can check
	// also need some way to access other snapshots...via a repository
	// or something...but rosnapshot here doens't need to access it
	// (and therefore know about it.) It can be supplied via appendable
	// for whom it's okay to know about other snapshots.
	// I want this snapshot to know just about itself (and not other snapshots)

}

func (s *RoSnapshots[CollationType]) Freeze(ctx context.Context, enum EENum, stepKeyFrom uint64, stepKeyTo uint64, db kv.RoDB) (CollationType, error) {
	freezer := s.freezers[enum]
	config := s.snapshotConfig[enum]

	// 1. find the last frzone step key
	// 2. check if snapshot can be created....
	// 3. get the filename, setup the compressor
	// 3. check if sn.Count() == blocksPerFrile
	// 4. sn.compress

	// lastStepInSnapshot, err := s.LastStepInSnapshot(enum)
	// if err != nil {
	// 	return err
	// }

	snapname = enum.GetSnapshotName(lastStepInSnapshot, stepKeyTo)
	freezer.SetCollector()
	coll, _, err := freezer.Freeze(ctx, stepKeyFrom, stepKeyTo, db)
	if err != nil {
		return err
	}

	// can build files and index builders too
	if err = coll.GetValuesComp().Compress(); err != nil {
		// return empty AppendableFiles, err?
		// who is managing these "files"
		// in aggregator with domain and history, it is the aggreagtor
		// maintaining a map of staticfiles
		// then integrating the staticfiles into the dirty files
		// and recalculating the visible files. etc.
		// The visibleFiles, fileItem, VisibleSegments, DirtySegments
		// etc. are all similar things...and should be repr by a generic arg
		// so that domains use a RoSnapsot[VisibleFiles=visibleFiles] for example.
		// given the "variety" in these structures (different indexes for example).

		// we're at crossroads here. I should think only of appendables for now, and
		// manage the above stuff (staticfiles) in here. The generalising with
		// domain/history can come later. The structural changes should only be about
		// introducing the right generics, and maybe some callbacks into domain/history/ii/appendable
		// to manage thing specific to them.
		return err
	}
	coll.GetValuesComp().Close()
	coll.SetValuesComp(nil)

	// indexbuilder needed to build indexes now...
	// better keep it as separate step, since Indexbuilder extensions (like AccessorIndexBuilder)
	// needs additional stuff like salt

	// TODO: what about merge?
	return coll, err
}

func (s *RoSnapshots[CollationType]) LastStepInSnapshot(enum EENum) (uint64, error) {
	// use dirty files
	return 0, nil
}

func (s *RoSnapshots[CollationType]) GetSnapshotConfig(enum EENum) SnapshotConfig {
	return s.snapshotConfig[enum]
}

// type RoSnapshot interface {
// 	OpenFolder() error
// 	OpenSegments(types []snaptype.Type, allowGaps bool) error
// 	SetFreezer(freezer Freezer)
// 	Freeze(ctx, stepKeyTo, stepKeyFrom uint64, kv.RoDb) error
// 	SetIndexBuilders(indexBuilders []*AccessorIndexBuilder)

//     // TODO: where's the range? index is corresponding to an enum and range
//     // should the index be part of visiblesegment?
// 	//GetIndexes(names ...IndexName) []*recsplit.RecSplit

// 	VisibleSegments() []*VisibleSegment
// 	VisibleSegment(stepKey, enum) (VisibleSegment, found? bool, error)

// 	Types() []snaptype.Type
// 	Close()
// }
