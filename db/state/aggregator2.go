package state

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/state/statecfg"
)

// AggOpts is an Aggregator builder and contains only runtime-changeable configs (which may vary between Erigon nodes)
type AggOpts struct { //nolint:gocritic
	schema   statecfg.SchemaGen // biz-logic
	dirs     datadir.Dirs
	logger   log.Logger
	stepSize uint64

	genSaltIfNeed   bool
	sanityOldNaming bool // prevent start directory with old file names
	disableFsync    bool // for tests speed
}

func New(dirs datadir.Dirs) AggOpts { //nolint:gocritic
	return AggOpts{ //Defaults
		logger:          log.Root(),
		schema:          statecfg.Schema,
		dirs:            dirs,
		stepSize:        config3.DefaultStepSize,
		genSaltIfNeed:   false,
		sanityOldNaming: false,
		disableFsync:    false,
	}
}

func NewTest(dirs datadir.Dirs) AggOpts { //nolint:gocritic
	return New(dirs).DisableFsync().GenSaltIfNeed(true)
}

func (opts AggOpts) Open(ctx context.Context, db kv.RoDB) (*Aggregator, error) { //nolint:gocritic
	//TODO: rename `OpenFolder` to `ReopenFolder`
	if opts.sanityOldNaming {
		if err := CheckSnapshotsCompatibility(opts.dirs); err != nil {
			panic(err)
		}
	}

	salt, err := GetStateIndicesSalt(opts.dirs, opts.genSaltIfNeed, opts.logger)
	if err != nil {
		return nil, err
	}

	a, err := newAggregator(ctx, opts.dirs, opts.stepSize, db, opts.logger)
	if err != nil {
		return nil, err
	}
	if err := statecfg.AdjustReceiptCurrentVersionIfNeeded(opts.dirs, opts.logger); err != nil {
		return nil, err
	}
	if err := statecfg.Configure(statecfg.Schema, a, opts.dirs, salt, opts.logger); err != nil {
		return nil, err
	}

	func() {
		a.dirtyFilesLock.Lock()
		defer a.dirtyFilesLock.Unlock()
		a.recalcVisibleFiles(a.dirtyFilesEndTxNumMinimax())
	}()

	if opts.disableFsync {
		//TODO: maybe move it to some kind of config?
		for _, d := range a.d {
			d.DisableFsync()
		}
		for _, ii := range a.iis {
			ii.DisableFsync()
		}
	}

	return a, nil
}

func (opts AggOpts) MustOpen(ctx context.Context, db kv.RoDB) *Aggregator { //nolint:gocritic
	agg, err := opts.Open(ctx, db)
	if err != nil {
		panic(fmt.Errorf("fail to open mdbx: %w", err))
	}
	return agg
}

// Setters

func (opts AggOpts) StepSize(s uint64) AggOpts    { opts.stepSize = s; return opts }        //nolint:gocritic
func (opts AggOpts) GenSaltIfNeed(v bool) AggOpts { opts.genSaltIfNeed = v; return opts }   //nolint:gocritic
func (opts AggOpts) Logger(l log.Logger) AggOpts  { opts.logger = l; return opts }          //nolint:gocritic
func (opts AggOpts) DisableFsync() AggOpts        { opts.disableFsync = true; return opts } //nolint:gocritic
func (opts AggOpts) SanityOldNaming() AggOpts { //nolint:gocritic
	opts.sanityOldNaming = true
	return opts
}

// Getters

func CheckSnapshotsCompatibility(d datadir.Dirs) error {
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
