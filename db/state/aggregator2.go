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

/*
	var Schema = SchemaGen{
		AccountsDomain: domainCfg{
			name: kv.AccountsDomain, valuesTable: kv.TblAccountVals,
			CompressCfg: seg.Cfg{
				WordLvl:    seg.CompressNone,
				WordLvlCfg: DomainCompressCfg,
				//PageLvl:    seg.PageLvlCfg{PageSize: 64, Compress: true},
			},

			Accessors: statecfg.AccessorBTree | statecfg.AccessorExistence,

			hist: histCfg{
				valuesTable:   kv.TblAccountHistoryVals,
				CompressorCfg: seg.DefaultWordLvlCfg, Compression: seg.CompressNone,

				historyLargeValues: false,
				historyIdx:         kv.AccountsHistoryIdx,

				iiCfg: iiCfg{
					filenameBase: kv.AccountsDomain.String(), keysTable: kv.TblAccountHistoryKeys, valuesTable: kv.TblAccountIdx,
					CompressorCfg: seg.DefaultWordLvlCfg,
					Accessors:     statecfg.AccessorHashMap,
				},
			},
		},
		StorageDomain: domainCfg{
			name: kv.StorageDomain, valuesTable: kv.TblStorageVals,
			CompressCfg: seg.Cfg{
				WordLvl:    seg.CompressNone,
				WordLvlCfg: DomainCompressCfg,
				//PageLvl:    seg.PageLvlCfg{PageSize: 64, Compress: true},
			},

			Accessors: statecfg.AccessorBTree | statecfg.AccessorExistence,

			hist: histCfg{
				valuesTable:   kv.TblStorageHistoryVals,
				CompressorCfg: seg.DefaultWordLvlCfg, Compression: seg.CompressNone,

				historyLargeValues: false,
				historyIdx:         kv.StorageHistoryIdx,

				iiCfg: iiCfg{
					filenameBase: kv.StorageDomain.String(), keysTable: kv.TblStorageHistoryKeys, valuesTable: kv.TblStorageIdx,
					CompressorCfg: seg.DefaultWordLvlCfg,
					Accessors:     statecfg.AccessorHashMap,
				},
			},
		},
		CodeDomain: domainCfg{
			name: kv.CodeDomain, valuesTable: kv.TblCodeVals,
			CompressCfg: seg.Cfg{
				WordLvl:    seg.CompressVals, // compressing Code with keys doesn't show any benefits. Compression of values shows 4x ratio on eth-mainnet and 2.5x ratio on bor-mainnet
				WordLvlCfg: DomainCompressCfg,
				//PageLvl:    seg.PageLvlCfg{PageSize: 64, Compress: true},
			},

			Accessors:   statecfg.AccessorBTree | statecfg.AccessorExistence,
			largeValues: true,

			hist: histCfg{
				valuesTable:   kv.TblCodeHistoryVals,
				CompressorCfg: seg.DefaultWordLvlCfg, Compression: seg.CompressKeys | seg.CompressVals,

				historyLargeValues: true,
				historyIdx:         kv.CodeHistoryIdx,

				iiCfg: iiCfg{
					filenameBase: kv.CodeDomain.String(), keysTable: kv.TblCodeHistoryKeys, valuesTable: kv.TblCodeIdx,
					CompressorCfg: seg.DefaultWordLvlCfg,
					Accessors:     statecfg.AccessorHashMap,
				},
			},
		},
		CommitmentDomain: domainCfg{
			name: kv.CommitmentDomain, valuesTable: kv.TblCommitmentVals,
			CompressCfg: seg.Cfg{
				WordLvl:    seg.CompressNone, //seg.CompressKeys,
				WordLvlCfg: DomainCompressCfg,
				PageLvl:    seg.PageLvlCfg{PageSize: 16, Compress: false},
			},

			Accessors:           statecfg.AccessorHashMap,
			replaceKeysInValues: AggregatorSqueezeCommitmentValues,

			hist: histCfg{
				valuesTable:   kv.TblCommitmentHistoryVals,
				CompressorCfg: HistoryCompressCfg, Compression: seg.CompressNone, // seg.CompressKeys | seg.CompressVals,
				historyIdx: kv.CommitmentHistoryIdx,

				historyLargeValues:            false,
				historyValuesOnCompressedPage: 64,

				snapshotsDisabled: true,
				historyDisabled:   true,

				iiCfg: iiCfg{
					filenameBase: kv.CommitmentDomain.String(), keysTable: kv.TblCommitmentHistoryKeys, valuesTable: kv.TblCommitmentIdx,
					CompressorCfg: seg.DefaultWordLvlCfg,
					Accessors:     statecfg.AccessorHashMap,
				},
			},
		},
		ReceiptDomain: domainCfg{
			name: kv.ReceiptDomain, valuesTable: kv.TblReceiptVals,
			CompressCfg: seg.Cfg{WordLvl: seg.CompressNone},

			largeValues: false,

			Accessors: statecfg.AccessorBTree | statecfg.AccessorExistence,

			hist: histCfg{
				valuesTable:   kv.TblReceiptHistoryVals,
				CompressorCfg: seg.DefaultWordLvlCfg, Compression: seg.CompressNone,

				historyLargeValues: false,
				historyIdx:         kv.ReceiptHistoryIdx,

				iiCfg: iiCfg{
					filenameBase: kv.ReceiptDomain.String(), keysTable: kv.TblReceiptHistoryKeys, valuesTable: kv.TblReceiptIdx,
					CompressorCfg: seg.DefaultWordLvlCfg,
					Accessors:     statecfg.AccessorHashMap,
				},
			},
		},
		RCacheDomain: domainCfg{
			name: kv.RCacheDomain, valuesTable: kv.TblRCacheVals,
			CompressCfg: seg.Cfg{WordLvl: seg.CompressNone},

			largeValues: true,

			Accessors: statecfg.AccessorHashMap,

			hist: histCfg{
				valuesTable: kv.TblRCacheHistoryVals,
				Compression: seg.CompressNone, //seg.CompressKeys | seg.CompressVals,

				historyLargeValues: true,
				historyIdx:         kv.RCacheHistoryIdx,

				snapshotsDisabled:             true,
				historyValuesOnCompressedPage: 16,

				iiCfg: iiCfg{
					disable:      true, // disable everything by default
					filenameBase: kv.RCacheDomain.String(), keysTable: kv.TblRCacheHistoryKeys, valuesTable: kv.TblRCacheIdx,
					CompressorCfg: seg.DefaultWordLvlCfg,
					Accessors:     statecfg.AccessorHashMap,
				},
			},
		},

		LogAddrIdx: iiCfg{
			filenameBase: kv.FileLogAddressIdx, keysTable: kv.TblLogAddressKeys, valuesTable: kv.TblLogAddressIdx,

			Compression: seg.CompressNone,
			name:        kv.LogAddrIdx,
			Accessors:   statecfg.AccessorHashMap,
		},
		LogTopicIdx: iiCfg{
			filenameBase: kv.FileLogTopicsIdx, keysTable: kv.TblLogTopicsKeys, valuesTable: kv.TblLogTopicsIdx,

			Compression: seg.CompressNone,
			name:        kv.LogTopicIdx,
			Accessors:   statecfg.AccessorHashMap,
		},
		TracesFromIdx: iiCfg{
			filenameBase: kv.FileTracesFromIdx, keysTable: kv.TblTracesFromKeys, valuesTable: kv.TblTracesFromIdx,

			Compression: seg.CompressNone,
			name:        kv.TracesFromIdx,
			Accessors:   statecfg.AccessorHashMap,
		},
		TracesToIdx: iiCfg{
			filenameBase: kv.FileTracesToIdx, keysTable: kv.TblTracesToKeys, valuesTable: kv.TblTracesToIdx,

			Compression: seg.CompressNone,
			name:        kv.TracesToIdx,
			Accessors:   statecfg.AccessorHashMap,
		},
	}
*/
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
