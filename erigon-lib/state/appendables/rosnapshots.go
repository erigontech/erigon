package appendables

type ApEnum string

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

// type RoSnapshots struct {
// 	// is cohort useful here?
// 	// helps with visibleFiles alignment, that's all...
// 	enums          []ApEnum
// 	dirty          map[ApEnum]*btree.BTreeG[*DirtySegment]
// 	visible        map[ApEnum]VisibleSegments
// 	snapshotConfig map[ApEnum]SnapshotConfig
// 	baseEnum       ApEnum
// }

// func NewRoSnapshots(enums []ApEnum, alignMin bool) *RoSnapshots {
// 	ro := &RoSnapshots{
// 		enums:          enums,
// 		dirty:          make(map[ApEnum]*btree.BTreeG[*DirtySegment]),
// 		visible:        make(map[ApEnum]VisibleSegments),
// 		snapshotConfig: make(map[ApEnum]SnapshotConfig),
// 	}

// 	for _, enum := range ro.enums {
// 		ro.dirty[enum] = btree.NewBTreeGOptions[*DirtySegment](DirtySegmentLess, btree.Options{
// 			NoLocks: true, Degree: 128,
// 		})
// 	}

// 	ro.baseEnum = enums[len(enums)-1]

// 	ro.recalcVisibleFiles()
// 	return ro
// }

// func (s *RoSnapshots) SetBaseEnum(enum ApEnum) {
// 	s.baseEnum = enum
// }

// func (s *RoSnapshots) RegisterSegment(enum ApEnum, seg *DirtySegment) {
// 	s.dirty[enum].Set(seg)
// 	s.recalcVisibleFiles()
// }

// func (s *RoSnapshots) VisibleSegMinimaxStepKey() uint64 { return 0 }

// func (s *RoSnapshots) DirtySegMinimaxStepKey() uint64 { return 0 }

// func (s *RoSnapshots) recalcVisibleFiles() {
// 	// based on alignMin, if true all enums are aligned to min step key
// 	// otherwise all enums are aligned to their own step key
// }

// func (s *RoSnapshots) LastStepInSnapshot(enum ApEnum) (uint64, error) {
// 	// use dirty files
// 	return 0, nil
// }

// func (s *RoSnapshots) SetSnapConfig(enum ApEnum, cfg SnapshotConfig) {
// 	s.snapshotConfig[enum] = cfg
// }

// func (s *RoSnapshots) GetSnapshotConfig(enum ApEnum) SnapshotConfig {
// 	return s.snapshotConfig[enum]
// }

// func (s *RoSnapshots) LogStat(label string) {}

// func (s *RoSnapshots) GetDirtySegment(enum ApEnum, stepKeyFrom, stepKeyTo uint64) (*DirtySegment, bool) {
// 	// walk over dirty segments and return the first that matches
// 	return nil, false
// }

// // ranges()
// func (s *RoSnapshots) Ranges() []Range {
// 	// use baseSegType
// 	return nil
// }

// // type RoSnapshot interface {
// // 	OpenFolder() error
// // 	OpenSegments(types []snaptype.Type, allowGaps bool) error
// // 	SetFreezer(freezer Freezer)
// // 	Freeze(ctx, stepKeyTo, stepKeyFrom uint64, kv.RoDb) error
// // 	SetIndexBuilders(indexBuilders []*AccessorIndexBuilder)

// //     // TODO: where's the range? index is corresponding to an enum and range
// //     // should the index be part of visiblesegment?
// // 	//GetIndexes(names ...IndexName) []*recsplit.RecSplit

// // 	VisibleSegments() []*VisibleSegment
// // 	VisibleSegment(stepKey, enum) (VisibleSegment, found? bool, error)

// // 	Types() []snaptype.Type
// // 	Close()
// // }

// // view stuff
// type View struct {
// 	s           *RoSnapshots
// 	segments    []*RoTx
// 	baseSegType ApEnum
// }

// type RoTx struct {
// 	Segments VisibleSegments
// }
