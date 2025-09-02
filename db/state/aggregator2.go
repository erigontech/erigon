package state

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func NewAggregator(ctx context.Context, dirs datadir.Dirs, aggregationStep uint64, db kv.RoDB, logger log.Logger) (*Aggregator, error) {
	salt, err := GetStateIndicesSalt(dirs, false, logger)
	if err != nil {
		return nil, err
	}
	return NewAggregator2(ctx, dirs, aggregationStep, salt, db, logger)
}

func NewAggregator2(ctx context.Context, dirs datadir.Dirs, aggregationStep uint64, salt *uint32, db kv.RoDB, logger log.Logger) (*Aggregator, error) {
	err := checkSnapshotsCompatibility(dirs)
	if err != nil {
		return nil, err
	}
	a, err := newAggregatorOld(ctx, dirs, aggregationStep, db, logger)
	if err != nil {
		return nil, err
	}
	if err := statecfg.Configure(a, dirs, salt, logger); err != nil {
		return nil, err
	}

	a.dirtyFilesLock.Lock()
	defer a.dirtyFilesLock.Unlock()
	a.recalcVisibleFiles(a.dirtyFilesEndTxNumMinimax())

	return a, nil
}

func checkSnapshotsCompatibility(d datadir.Dirs) error {
	directories := []string{
		d.Chaindata, d.Tmp, d.SnapIdx, d.SnapHistory, d.SnapDomain,
		d.SnapAccessors, d.SnapCaplin, d.Downloader, d.TxPool, d.Snap,
		d.Nodes, d.CaplinBlobs, d.CaplinIndexing, d.CaplinLatest, d.CaplinGenesis,
	}
	for _, dirPath := range directories {
		err := filepath.WalkDir(dirPath, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				if os.IsNotExist(err) { //skip magically disappeared files
					return nil
				}
				return err
			}
			if entry.IsDir() {
				return nil
			}

			name := entry.Name()
			if strings.HasPrefix(name, "v1-") {
				return errors.New("The datadir has bad snapshot files or they are " +
					"incompatible with the current erigon version. If you want to upgrade from an" +
					"older version, you may run the following to rename files to the " +
					"new version: `erigon snapshots update-to-new-ver-format`")
			}
			fileInfo, _, _ := snaptype.ParseFileName("", name)

			currentFileVersion := fileInfo.Version

			msVs, ok := statecfg.SchemeMinSupportedVersions[fileInfo.TypeString]
			if !ok {
				//println("file type not supported", fileInfo.TypeString, name)
				return nil
			}
			requiredVersion, ok := msVs[fileInfo.Ext]
			if !ok {
				return nil
			}

			if currentFileVersion.Major < requiredVersion.Major {
				return fmt.Errorf("snapshot file major version mismatch for file %s, "+
					" requiredVersion: %d, currentVersion: %d"+
					" You may want to downgrade to an older version (not older than 3.1)",
					fileInfo.Name(), requiredVersion.Major, currentFileVersion.Major)
			}
			return nil
		})

		if err != nil {
			return err
		}
	}

	return nil
}
