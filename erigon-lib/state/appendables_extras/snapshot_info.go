package appendables_extras

import (
	"path/filepath"
	"strconv"
	"strings"

	"github.com/erigontech/erigon-lib/chain/snapcfg"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
)

const EntitiesPerStep = 1000

type FileInfo struct {
	Version  snaptype.Version
	From, To uint64
	Name     string // filename
	Path     string // full path
	Ext      string // extenstion
	Id       AppendableId
}

type SnapshotCreationConfig struct {
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
