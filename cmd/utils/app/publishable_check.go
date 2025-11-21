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

// checkLastFileTo = -1 if don't need this check
func CheckFilesForSchema(schema state.SnapNameSchema, checkLastFileTo int64) (lastFileTo uint64, err error) {
	// check in schema specific directory (and accessor directories)

	// checks:
	// - no gaps
	// - starts from 0
	// - more than 0 files
	// - no overlaps
	// each data file has corresponding index/bt etc.1
	// versions: between min supported version and max
	// - lastFileTo check
	// - sum = maxTo check (probably redundant if no gaps/overlaps)

	// collect all data files
	dataFiles := make([]state.SnapInfo, 0)
	sumRange := uint64(0)
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

		sumRange += res.To - res.From
		dataFiles = append(dataFiles, *res)

		return nil
	}); err != nil {
		return 0, err
	}

	sort.Slice(dataFiles, func(i, j int) bool {
		return (dataFiles[i].From < dataFiles[j].From) || (dataFiles[i].From == dataFiles[j].From && dataFiles[i].To < dataFiles[j].To)
	})

	if len(dataFiles) == 0 {
		return 0, fmt.Errorf("no %s snapshot files found in %s", schema.DataTag(), schema.DataDirectory())
	}

	if dataFiles[0].From != 0 {
		return 0, fmt.Errorf("first %s snapshot file must start from 0, found from %d", schema.DataTag(), dataFiles[0].From)
	}

	if checkLastFileTo >= 0 && int64(dataFiles[len(dataFiles)-1].To) != checkLastFileTo {
		return 0, fmt.Errorf("last %s snapshot file must end at %d, found at %d (file: %s)", schema.DataTag(), checkLastFileTo, dataFiles[len(dataFiles)-1].To, dataFiles[len(dataFiles)-1].Name)
	}

	if sumRange != dataFiles[len(dataFiles)-1].To {
		return 0, fmt.Errorf("sum of ranges of %s snapshot files (%d) does not match last 'to' value (%d)", schema.DataTag(), sumRange, dataFiles[len(dataFiles)-1].To)
	}

	prevFrom, prevTo := dataFiles[0].From, dataFiles[0].To
	for i := 1; i < len(dataFiles); i++ {
		df := dataFiles[i]
		if prevFrom == df.From {
			return 0, fmt.Errorf("overlapping %s snapshot files found: %s and %s", schema.DataTag(), dataFiles[i-1].Name, df.Name)
		}

		if df.From < prevTo {
			return 0, fmt.Errorf("overlapping %s snapshot files found: %s and %s", schema.DataTag(), dataFiles[i-1].Name, df.Name)
		}
		if df.From > prevTo {
			return 0, fmt.Errorf("gap in %s snapshot files found between %s and %s", schema.DataTag(), dataFiles[i-1].Name, df.Name)
		}
		prevFrom, prevTo = df.From, df.To
	}

	accessors := schema.AccessorList()
	for _, dataFile := range dataFiles {
		// corresponding accessor exists?

		if accessors.Has(statecfg.AccessorHashMap) {
			for idxPos := uint8(0); idxPos < uint8(schema.AccessorIdxCount()); idxPos++ {
				file, found := schema.AccessorIdxFile(version.SearchVersion, kv.RootNum(dataFile.From), kv.RootNum(dataFile.To), 0)
				if !found {
					return 0, fmt.Errorf("missing %s accessor idx file for data file %s (idx tag: %s)", schema.DataTag(), dataFile.Name, idxPos)
				}
				accInfo, ok := schema.Parse(filepath.Base(file))
				if !ok {
					return 0, fmt.Errorf("unable to parse %s accessor idx file name %s", schema.DataTag(), file)
				}

				//TODO: I need schema to return supported version range
				//				if accInfo.Version.GreaterOrEqual()

			}

		}

	}

	return dataFiles[len(dataFiles)-1].To, nil
}
