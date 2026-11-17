package app

import (
	"cmp"
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/version"
)

// publishable check using snapnameschema

type CheckFilesParams struct {
	checkLastFileTo int64
	emptyOk         bool
	doesntStartAt0  bool // blobsidecars start at 1942 step on mainnet after dencun upgrade
}

// checkLastFileTo = -1 if don't need this check
func CheckFilesForSchema(schema *state.E2SnapSchema, params CheckFilesParams) (lastFileTo uint64, empty bool, err error) {
	// check in schema specific directory (and accessor directories)

	// checks:
	// - no gaps
	// - starts from 0
	// - more than 0 files
	// - no overlaps
	// - each data file has corresponding index/bt etc.1
	// - versions: between min supported version and max
	// - lastFileTo check

	entries, err := os.ReadDir(schema.DataDirectory())
	if err != nil && !os.IsNotExist(err) {
		return 0, false, err
	}
	files := map[snapFileKey][]state.SnapInfo{}
	dataFiles := make([]state.SnapInfo, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		res, ok := schema.Parse(entry.Name())
		if !ok {
			continue
		}
		key := snapFileKey{res.FileType, res.Ext, res.From, res.To}
		files[key] = append(files[key], *res)
		if res.IsDataFile() {
			dataFiles = append(dataFiles, *res)
		}
	}

	slices.SortFunc(dataFiles, func(a, b state.SnapInfo) int {
		if a.From != b.From {
			return cmp.Compare(a.From, b.From)
		}
		return cmp.Compare(a.To, b.To)
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

	versions := schema.Versions()
	for _, dataFile := range dataFiles {
		dataKey := snapFileKey{dataFile.FileType, dataFile.Ext, dataFile.From, dataFile.To}
		if err := onlySupportedFile(files[dataKey], versions.DataFileVersion); err != nil {
			return 0, false, fmt.Errorf("unsupported data file version: %s: %w", dataFile.Name, err)
		}

		for idxPos, idxTag := range schema.IndexTags() {
			idxKey := snapFileKey{idxTag, string(state.AccessorExtensionIdx), dataFile.From, dataFile.To}
			if err := onlySupportedFile(files[idxKey], versions.AccessorVersion); err != nil {
				return 0, false, fmt.Errorf("missing %s accessor idx file for data file %s (idx tag: %d): %w", schema.DataTag(), dataFile.Name, idxPos, err)
			}
		}
	}

	return dataFiles[len(dataFiles)-1].To, false, nil
}

type snapFileKey struct {
	fileType, ext string
	from, to      uint64
}

func onlySupportedFile(versions []state.SnapInfo, supported version.Versions) error {
	switch {
	case len(versions) == 0:
		return errors.New("file not found")
	case len(versions) > 1:
		return fmt.Errorf("more than one version found: %s and %s", versions[0].Name, versions[1].Name)
	case !supported.Supports(versions[0].Version):
		return fmt.Errorf("unsupported version: %s", versions[0].Name)
	}
	return nil
}
