package state

import (
	"fmt"

	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/version"
)

// aggregate set level snapshot creation config
// entities in the set should have same config
type SnapshotCreationConfig struct {
	// number of RootNums per step
	// should be same for all entity in an entity set
	RootNumPerStep uint64

	// how many (root) entities to leave in db (and not consider for freezing) this is needed
	// since blockchains reorg and so we don't freeze latest entities.
	SafetyMargin uint64

	// progressively merge smaller files into large ones.
	// maximum size (merge limit) is the last element of MergeStages
	// decreasing order expected, each step is a multiple of the previous one
	// e.g. [1000, 20000, 600000] --> first stage creates files of size 1000; then 20 of these merged to
	// create size 10000; then 30 of these merged to create size 100000
	// each must be divisible by `RootNumPerStep`
	// last stage is considered to be "frozen".
	MergeStages []uint64

	// minimum snapshot size - number of "RootNums" in the minimum-sized file.
	// must be divisible by `RootNumPerStep`
	MinimumSize uint64

	// SeedableSize uint64 // TODO: minimum size of file for it to be seedable.

	// preverified can have larger files than that indicated by `MergeSteps.last`.
	// This is because previously, different values might have been used.
	//Preverified       snapcfg.Preverified
	PreverifiedParsed []*SnapInfo
}

type SnapshotConfig struct {
	*SnapshotCreationConfig

	// alignment means that the read-only snapshot view of this entity
	// is aligned to those of the root entity.
	RootAligned bool

	Integrity *DependencyIntegrityChecker

	Schema SnapNameSchema
}

func NewSnapshotConfig(cfg *SnapshotCreationConfig, schema SnapNameSchema) *SnapshotConfig {
	c := &SnapshotConfig{
		SnapshotCreationConfig: cfg,
		Schema:                 schema,
	}
	c.validate()
	return c
}

func (s *SnapshotConfig) validate() {
	// some validation
	for i := range s.MergeStages {
		if s.MergeStages[i]%s.RootNumPerStep != 0 {
			panic(fmt.Sprintf("MergeStages[%d] must be divisible by RootNumPerStep", i))
		}
	}
	if s.MinimumSize%s.RootNumPerStep != 0 {
		panic(fmt.Sprintf("MinimumSize(%d) must be divisible by RootNumPerStep(%d)", s.MinimumSize, s.RootNumPerStep))
	}
}

func (s *SnapshotConfig) StepsInFrozenFile() uint64 {
	if len(s.MergeStages) == 0 {
		return s.MinimumSize / s.RootNumPerStep
	}

	return s.MergeStages[len(s.MergeStages)-1] / s.RootNumPerStep
}

func (s *SnapshotConfig) LoadPreverified(pre snapcfg.PreverifiedItems) {
	if s.PreverifiedParsed != nil {
		return
	}
	s.PreverifiedParsed = make([]*SnapInfo, 0, len(pre))
	for _, item := range []snapcfg.PreverifiedItem(pre) {
		res, ok := s.Schema.Parse(item.Name)
		if !ok {
			continue
		}
		s.PreverifiedParsed = append(s.PreverifiedParsed, res)
	}
}

// common representation for any snapshot files
// seg, .v or indexes and existence filters, accessors.
type SnapInfo struct {
	Version  version.Version
	From, To uint64
	Name     string // filename
	FileType string
	// Path     string // full path
	Ext string // extension
}

// func (f *SnapInfo) IsSeg() bool      { return strings.Compare(f.Ext, ".seg") == 0 }
// func (f *SnapInfo) IsV() bool        { return strings.Compare(f.Ext, ".v") == 0 }
// func (f *SnapInfo) IsKV() bool       { return strings.Compare(f.Ext, ".kv") == 0 }
func (f *SnapInfo) IsDataFile() bool { return DataExtension(f.Ext).IsSet() }

func (f *SnapInfo) Len() uint64 { return f.To - f.From }
