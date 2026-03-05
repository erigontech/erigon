package app

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
)

// publishable check using snapnameschema

type CheckFilesParams struct {
	checkLastFileTo int64
	emptyOk         bool
	doesntStartAt0  bool // blobsidecars start at 1942 step on mainnet after dencun upgrade
}

// checkLastFileTo = -1 if don't need this check
func CheckFilesForSchema(schema state.SnapNameSchema, params CheckFilesParams) (lastFileTo uint64, empty bool, err error) {
	// check in schema specific directory (and accessor directories)

	// checks:
	// - no gaps
	// - starts from 0
	// - more than 0 files
	// - no overlaps
	// - each data file has corresponding index/bt etc.1
	// - versions: between min supported version and max
	// - lastFileTo check

	// collect all data files
	dataFiles := make([]state.SnapInfo, 0)
	if err := filepath.WalkDir(schema.DataDirectory(), func(path string, info fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) { //it's ok if some file get removed during walk
				return nil
			}
			return err
		}
		if info.IsDir() {
			return nil
		}

		basepath := filepath.Base(path)

		res, ok := schema.Parse(basepath)
		if !ok {
			return nil // not matching file
		}

		if !res.IsDataFile() {
			// ignore the idx files
			return nil
		}

		dataFiles = append(dataFiles, *res)

		return nil
	}); err != nil {
		return 0, false, err
	}

	sort.Slice(dataFiles, func(i, j int) bool {
		return (dataFiles[i].From < dataFiles[j].From) || (dataFiles[i].From == dataFiles[j].From && dataFiles[i].To < dataFiles[j].To)
	})

	if len(dataFiles) == 0 {
		if !params.emptyOk {
			return 0, true, fmt.Errorf("no %s snapshot files found in %s", schema.DataTag(), schema.DataDirectory())
		} else {
			return 0, true, nil
		}
	}

	if dataFiles[0].From != 0 && !params.doesntStartAt0 {
		return 0, false, fmt.Errorf("first %s snapshot file must start from 0, found from %d", schema.DataTag(), dataFiles[0].From)
	}

	if params.checkLastFileTo >= 0 && int64(dataFiles[len(dataFiles)-1].To) != params.checkLastFileTo {
		return 0, false, fmt.Errorf("last %s snapshot file must end at %d, found at %d (file: %s)", schema.DataTag(), params.checkLastFileTo, dataFiles[len(dataFiles)-1].To, dataFiles[len(dataFiles)-1].Name)
	}

	prevFrom, prevTo := dataFiles[0].From, dataFiles[0].To
	for i := 1; i < len(dataFiles); i++ {
		df := dataFiles[i]
		if prevFrom == df.From {
			return 0, false, fmt.Errorf("overlapping %s snapshot files found: %s and %s", schema.DataTag(), dataFiles[i-1].Name, df.Name)
		}

		if df.From < prevTo {
			return 0, false, fmt.Errorf("overlapping %s snapshot files found: %s and %s", schema.DataTag(), dataFiles[i-1].Name, df.Name)
		}
		if df.From > prevTo {
			return 0, false, fmt.Errorf("gap in %s snapshot files found between %s and %s", schema.DataTag(), dataFiles[i-1].Name, df.Name)
		}
		prevFrom, prevTo = df.From, df.To
	}

	accessors := schema.AccessorList()
	for _, dataFile := range dataFiles {
		// corresponding accessor exists?
		from, to := kv.RootNum(dataFile.From), kv.RootNum(dataFile.To)

		// should get the same name as dataFile...
		// this checks the version is correct (between min and current), and that there's only one such data file
		if _, err := schema.DataFile(version.StrictSearchVersion, from, to); err != nil {
			return 0, false, fmt.Errorf("unsupported data file version: %s: %v", dataFile.Name, err)
		}

		if accessors.Has(statecfg.AccessorHashMap) {
			for idxPos := uint16(0); idxPos < schema.AccessorIdxCount(); idxPos++ {
				_, err := schema.AccessorIdxFile(version.StrictSearchVersion, from, to, idxPos)
				if err != nil {
					return 0, false, fmt.Errorf("missing %s accessor idx file for data file %s (idx tag: %d): %v", schema.DataTag(), dataFile.Name, idxPos, err)
				}
			}
		}

		if accessors.Has(statecfg.AccessorBTree) {
			_, err := schema.BtIdxFile(version.StrictSearchVersion, from, to)
			if err != nil {
				return 0, false, fmt.Errorf("missing %s bt tree file for data file %s: %v", schema.DataTag(), dataFile.Name, err)
			}
		}

		if accessors.Has(statecfg.AccessorExistence) {
			_, err := schema.ExistenceFile(version.StrictSearchVersion, from, to)
			if err != nil {
				return 0, false, fmt.Errorf("missing %s existence filter for data file %s: %v", schema.DataTag(), dataFile.Name, err)
			}
		}
	}

	return dataFiles[len(dataFiles)-1].To, false, nil
}
