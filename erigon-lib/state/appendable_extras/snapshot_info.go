package entity_extras

import (
	"fmt"
	"strings"

	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
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
	MergeStages []uint64

	// minimum snapshot size - number of "RootNums" in the minimum-sized file.
	// must be divisible by `RootNumPerStep`
	MinimumSize uint64

	// SeedableSize uint64 // TODO: minimum size of file for it to be seedable.

	// preverified can have larger files than that indicated by `MergeSteps.last`.
	// This is because previously, different values might have been used.
	//Preverified       snapcfg.Preverified
	preverifiedParsed []*SnapInfo
}

type SnapshotConfig struct {
	*SnapshotCreationConfig

	// alignment means that the read-only snapshot view of this entity
	// is aligned to those of the root entity.
	RootAligned bool

	// snapshot directory
	Directory string

	Parser SnapNameParser
}

func (s *SnapshotConfig) StepsInFrozenFile() uint64 {
	return s.MergeStages[len(s.MergeStages)-1] / s.RootNumPerStep
}

func (s *SnapshotConfig) LoadPreverified(pre snapcfg.Preverified) {
	if s.preverifiedParsed != nil {
		return
	}
	s.preverifiedParsed = make([]*SnapInfo, 0, len(pre))
	for _, item := range []snapcfg.PreverifiedItem(pre) {
		res, ok := s.Parser.Parse(item.Name)
		if !ok {
			continue
		}
		s.preverifiedParsed = append(s.preverifiedParsed, res)
	}

	// some validation
	for i := range s.MergeStages {
		if s.MergeStages[i]%s.RootNumPerStep != 0 {
			panic(fmt.Sprintf("MergeStages[%d] must be divisible by EntitiesPerStep", i))
		}
	}
	if s.MinimumSize%s.RootNumPerStep != 0 {
		panic(fmt.Sprintf("MinimumSize(%d) must be divisible by EntitiesPerStep(%d)", s.MinimumSize, s.RootNumPerStep))
	}
}

// common representation for any snapshot files
// seg, .v or indexes and existence filters, accessors.
type SnapInfo struct {
	Version  snaptype.Version
	From, To uint64
	Name     string // filename
	FileType string
	// Path     string // full path
	Ext string // extenstion
}

type Version = snaptype.Version

func (f *SnapInfo) IsSeg() bool      { return strings.Compare(f.Ext, ".seg") == 0 }
func (f *SnapInfo) IsV() bool        { return strings.Compare(f.Ext, ".v") == 0 }
func (f *SnapInfo) IsKV() bool       { return strings.Compare(f.Ext, ".kv") == 0 }
func (f *SnapInfo) IsDataFile() bool { return f.IsSeg() || f.IsV() || f.IsKV() }

func (f *SnapInfo) Len() uint64 { return f.To - f.From }

// determine freezing ranges, given snapshot creation config
func GetFreezingRange(rootFrom, rootTo RootNum, cfg *SnapshotConfig) (freezeFrom RootNum, freezeTo RootNum, canFreeze bool) {
	/**
	 1. `from`, `to` must be round off to minimum size (atleast)
	 2. mergeLimit is a function: (from, preverified files, mergeLimit default) -> biggest file size starting `from`
	 3. if mergeLimit size is not possible, then `freezeTo` should be next largest possible file size
	    as allowed by the MergeSteps or MinimumSize.
	**/

	if rootFrom >= rootTo {
		return rootFrom, rootTo, false
	}

	from := uint64(rootFrom)
	to := uint64(rootTo)

	to = to - cfg.SafetyMargin
	from = (from / cfg.MinimumSize) * cfg.MinimumSize
	to = (to / cfg.MinimumSize) * cfg.MinimumSize

	mergeLimit := getMergeLimit(cfg, from)
	maxJump := cfg.RootNumPerStep

	if from%mergeLimit == 0 {
		maxJump = mergeLimit
	} else {
		for i := len(cfg.MergeStages) - 1; i >= 0; i-- {
			if from%cfg.MergeStages[i] == 0 {
				maxJump = cfg.MergeStages[i]
				break
			}
		}
	}

	_freezeFrom := from
	var _freezeTo uint64
	jump := to - from

	switch {
	case jump >= maxJump:
		// enough data, max jump
		_freezeTo = _freezeFrom + maxJump
	case jump >= cfg.MergeStages[0]:
		// else find if a merge step can be used
		// assuming merge step multiple of each other
		for i := len(cfg.MergeStages) - 1; i >= 0; i-- {
			if jump >= cfg.MergeStages[i] {
				_freezeTo = _freezeFrom + cfg.MergeStages[i]
				break
			}
		}
	case jump >= cfg.MinimumSize:
		// else use minimum size
		_freezeTo = _freezeFrom + cfg.MinimumSize

	default:
		_freezeTo = _freezeFrom
	}

	return RootNum(_freezeFrom), RootNum(_freezeTo), _freezeTo-_freezeFrom >= cfg.MinimumSize
}

func getMergeLimit(cfg *SnapshotConfig, from uint64) uint64 {
	//return 0
	maxMergeLimit := cfg.MergeStages[len(cfg.MergeStages)-1]

	for _, info := range cfg.preverifiedParsed {
		if !info.IsDataFile() {
			continue
		}

		if from < info.From || from >= info.To {
			continue
		}

		if info.Len() >= maxMergeLimit {
			// info.Len() > maxMergeLimit --> this happens when previously a larger value
			// was used, and now the configured merge limit is smaller.
			return info.Len()
		}

		break
	}

	return maxMergeLimit
}
