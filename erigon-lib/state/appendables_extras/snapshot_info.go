package appendables_extras

import (
	"path/filepath"
	"strconv"
	"strings"

	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
)

const EntitiesPerStep = uint64(1000)

type SnapshotCreationConfig struct {
	// all the following configs are in terms of number of entities
	// 1 step has `EntitiesPerStep` entities

	// how many items to leave in db (and not consider for freezing) this is needed
	// since blockchains reorg
	SafetyMargin uint64

	// progressively merge smaller files into large ones maximum size (merge limit)
	// is the last element of MergeSteps
	MergeSteps []uint64

	// minimum snapshot size
	MinimumSize uint64

	// preverified can have larger files than that indicated by `MergeSteps.last`.
	// This is because previously different values might have been used.
	//Preverified       snapcfg.Preverified
	preverifiedParsed []*FileInfo
}

func (s *SnapshotCreationConfig) parseConfig(id AppendableId, dirs datadir.Dirs, pre snapcfg.Preverified) {
	if s.preverifiedParsed != nil {
		return
	}
	s.preverifiedParsed = make([]*FileInfo, 0, len(pre))
	for _, item := range []snapcfg.PreverifiedItem(pre) {
		res, ok := parseFileName(id, dirs.Snap, item.Name)
		if !ok {
			continue
		}
		s.preverifiedParsed = append(s.preverifiedParsed, res)
	}
}

func parseFileName(id AppendableId, dir, fileName string) (res *FileInfo, ok bool) {
	//	'v1-000000-000500-transactions.seg'
	// 'v1-017000-017500-transactions-to-block.idx'
	ext := filepath.Ext(fileName)
	if ext != ".seg" && ext != ".idx" {
		return nil, false
	}
	onlyName := fileName[:len(fileName)-len(ext)]
	parts := strings.SplitN(onlyName, "-", 4)
	res = &FileInfo{Path: filepath.Join(dir, fileName), Name: fileName, Ext: ext}

	if len(parts) < 4 {
		return nil, ok
	}

	var err error
	res.Version, err = snaptype.ParseVersion(parts[0])
	if err != nil {
		return res, false
	}

	from, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return res, false
	}
	res.From = from * EntitiesPerStep
	to, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return res, false
	}
	res.To = to * EntitiesPerStep

	res.Id = id
	name := parts[3]
	// it should either match snapshot or its indexes
	if strings.Compare(name, id.SnapshotPrefix()) == 0 {
		return res, true
	} else {
		for _, prefix := range id.IndexPrefix() {
			if strings.Compare(name, prefix) == 0 {
				return res, true
			}

		}
	}

	return nil, false
}

// parse snapshot file info
type FileInfo struct {
	Version  snaptype.Version
	From, To uint64
	Name     string // filename
	Path     string // full path
	Ext      string // extenstion
	Id       AppendableId
}

func (f *FileInfo) IsIndex() bool { return strings.Compare(f.Ext, ".idx") == 0 }

func (f *FileInfo) IsSeg() bool { return strings.Compare(f.Ext, ".seg") == 0 }

func (f *FileInfo) Len() uint64 { return f.To - f.From }

// determine freezing ranges, given snapshot creation config
func GetFreezingRange(from, to uint64, id AppendableId) (freezeFrom uint64, freezeTo uint64, canFreeze bool) {
	/**
	 1. `from`, `to` must be round off to minimum size (atleast) and then also mergeLimit
	 2. mergeLimit is a function: (from, preverified files, some mergeLimit defaults) -> biggest file size at `from`
	 3. if mergeLimit size is not possible, then `freezeTo` should be next largest possible file size
	    as allowed by the MergeSteps.
	**/

	cfg := id.SnapshotCreationConfig()

	to = to - cfg.SafetyMargin
	from = (from / cfg.MinimumSize) * cfg.MinimumSize
	to = (to / cfg.MinimumSize) * cfg.MinimumSize

	mergeLimit := getMergeLimit(id, from)
	maxJump := EntitiesPerStep

	if maxJump%mergeLimit == 0 {
		maxJump = mergeLimit
	} else {
		for i := len(cfg.MergeSteps) - 1; i >= 0; i-- {
			if maxJump%cfg.MergeSteps[i] == 0 {
				maxJump = cfg.MergeSteps[i]
				break
			}
		}
	}

	freezeFrom = from
	freezeTo = from
	jump := min(maxJump, to-from)
	if jump >= mergeLimit {
		freezeTo = from + mergeLimit
	} else {
		for i := len(cfg.MergeSteps) - 1; i >= 0; i-- {
			if jump >= cfg.MergeSteps[i] {
				freezeTo = from + cfg.MergeSteps[i]
				break
			}
		}
	}

	return freezeFrom, freezeTo, freezeTo-freezeFrom >= cfg.MinimumSize
}

func getMergeLimit(id AppendableId, from uint64) uint64 {
	//return 0
	cfg := id.SnapshotCreationConfig()
	maxMergeLimit := cfg.MergeSteps[len(cfg.MergeSteps)-1]

	for _, info := range cfg.preverifiedParsed {
		if !info.IsSeg() {
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
