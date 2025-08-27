package state

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
)

// this is supposed to register domains/iis
// salt file should exist, else agg created has nil salt.
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
	if err := AdjustReceiptCurrentVersionIfNeeded(dirs, logger); err != nil {
		return nil, err
	}
	if err := a.registerDomain(Schema.GetDomainCfg(kv.AccountsDomain), salt, dirs, logger); err != nil {
		return nil, err
	}
	if err := a.registerDomain(Schema.GetDomainCfg(kv.StorageDomain), salt, dirs, logger); err != nil {
		return nil, err
	}
	if err := a.registerDomain(Schema.GetDomainCfg(kv.CodeDomain), salt, dirs, logger); err != nil {
		return nil, err
	}
	if err := a.registerDomain(Schema.GetDomainCfg(kv.CommitmentDomain), salt, dirs, logger); err != nil {
		return nil, err
	}
	if err := a.registerDomain(Schema.GetDomainCfg(kv.ReceiptDomain), salt, dirs, logger); err != nil {
		return nil, err
	}
	if err := a.registerDomain(Schema.GetDomainCfg(kv.RCacheDomain), salt, dirs, logger); err != nil {
		return nil, err
	}
	if err := a.registerII(Schema.GetIICfg(kv.LogAddrIdx), salt, dirs, logger); err != nil {
		return nil, err
	}
	if err := a.registerII(Schema.GetIICfg(kv.LogTopicIdx), salt, dirs, logger); err != nil {
		return nil, err
	}
	if err := a.registerII(Schema.GetIICfg(kv.TracesFromIdx), salt, dirs, logger); err != nil {
		return nil, err
	}
	if err := a.registerII(Schema.GetIICfg(kv.TracesToIdx), salt, dirs, logger); err != nil {
		return nil, err
	}

	a.AddDependencyBtwnDomains(kv.AccountsDomain, kv.CommitmentDomain)
	a.AddDependencyBtwnDomains(kv.StorageDomain, kv.CommitmentDomain)

	a.KeepRecentTxnsOfHistoriesWithDisabledSnapshots(100_000) // ~1k blocks of history

	a.dirtyFilesLock.Lock()
	defer a.dirtyFilesLock.Unlock()
	a.recalcVisibleFiles(a.dirtyFilesEndTxNumMinimax())

	return a, nil
}

var dbgCommBtIndex = dbg.EnvBool("AGG_COMMITMENT_BT", false)

func init() {
	if dbgCommBtIndex {
		Schema.CommitmentDomain.Accessors = statecfg.AccessorBTree | statecfg.AccessorExistence
	}
	InitSchemas()
}

type SchemaGen struct {
	AccountsDomain   domainCfg
	StorageDomain    domainCfg
	CodeDomain       domainCfg
	CommitmentDomain domainCfg
	ReceiptDomain    domainCfg
	RCacheDomain     domainCfg
	LogAddrIdx       statecfg.InvIdx
	LogTopicIdx      statecfg.InvIdx
	TracesFromIdx    statecfg.InvIdx
	TracesToIdx      statecfg.InvIdx
}

type Versioned interface {
	GetVersions() statecfg.VersionTypes
}

func (s *SchemaGen) GetVersioned(name string) (Versioned, error) {
	switch name {
	case kv.AccountsDomain.String(), kv.StorageDomain.String(), kv.CodeDomain.String(), kv.CommitmentDomain.String(), kv.ReceiptDomain.String(), kv.RCacheDomain.String():
		domain, err := kv.String2Domain(name)
		if err != nil {
			return nil, err
		}
		return s.GetDomainCfg(domain), nil
	case kv.LogTopicIdx.String(), kv.LogAddrIdx.String(), kv.TracesFromIdx.String(), kv.TracesToIdx.String():
		ii, err := kv.String2InvertedIdx(name)
		if err != nil {
			return nil, err
		}
		return s.GetIICfg(ii), nil
	default:
		return nil, fmt.Errorf("unknown schema version '%s'", name)
	}
}

func (s *SchemaGen) GetDomainCfg(name kv.Domain) domainCfg {
	var v domainCfg
	switch name {
	case kv.AccountsDomain:
		v = s.AccountsDomain
	case kv.StorageDomain:
		v = s.StorageDomain
	case kv.CodeDomain:
		v = s.CodeDomain
	case kv.CommitmentDomain:
		v = s.CommitmentDomain
	case kv.ReceiptDomain:
		v = s.ReceiptDomain
	case kv.RCacheDomain:
		v = s.RCacheDomain
	default:
		v = domainCfg{}
	}
	return v
}

func (s *SchemaGen) GetIICfg(name kv.InvertedIdx) statecfg.InvIdx {
	var v statecfg.InvIdx
	switch name {
	case kv.LogAddrIdx:
		v = s.LogAddrIdx
	case kv.LogTopicIdx:
		v = s.LogTopicIdx
	case kv.TracesFromIdx:
		v = s.TracesFromIdx
	case kv.TracesToIdx:
		v = s.TracesToIdx
	default:
		v = statecfg.InvIdx{}
	}
	return v
}

var ExperimentalConcurrentCommitment = false // set true to use concurrent commitment by default

var Schema = SchemaGen{
	AccountsDomain: domainCfg{
		name: kv.AccountsDomain, valuesTable: kv.TblAccountVals,
		CompressCfg: DomainCompressCfg, Compression: seg.CompressNone,

		Accessors: statecfg.AccessorBTree | statecfg.AccessorExistence,

		hist: statecfg.HistCfg{
			ValuesTable:   kv.TblAccountHistoryVals,
			CompressorCfg: seg.DefaultCfg, Compression: seg.CompressNone,

			HistoryLargeValues: false,
			HistoryIdx:         kv.AccountsHistoryIdx,

			IiCfg: statecfg.InvIdx{
				FilenameBase: kv.AccountsDomain.String(), KeysTable: kv.TblAccountHistoryKeys, ValuesTable: kv.TblAccountIdx,
				CompressorCfg: seg.DefaultCfg,
				Accessors:     statecfg.AccessorHashMap,
			},
		},
	},
	StorageDomain: domainCfg{
		name: kv.StorageDomain, valuesTable: kv.TblStorageVals,
		CompressCfg: DomainCompressCfg, Compression: seg.CompressKeys,

		Accessors: statecfg.AccessorBTree | statecfg.AccessorExistence,

		hist: statecfg.HistCfg{
			ValuesTable:   kv.TblStorageHistoryVals,
			CompressorCfg: seg.DefaultCfg, Compression: seg.CompressNone,

			HistoryLargeValues: false,
			HistoryIdx:         kv.StorageHistoryIdx,

			IiCfg: statecfg.InvIdx{
				FilenameBase: kv.StorageDomain.String(), KeysTable: kv.TblStorageHistoryKeys, ValuesTable: kv.TblStorageIdx,
				CompressorCfg: seg.DefaultCfg,
				Accessors:     statecfg.AccessorHashMap,
			},
		},
	},
	CodeDomain: domainCfg{
		name: kv.CodeDomain, valuesTable: kv.TblCodeVals,
		CompressCfg: DomainCompressCfg, Compression: seg.CompressVals, // compressing Code with keys doesn't show any benefits. Compression of values shows 4x ratio on eth-mainnet and 2.5x ratio on bor-mainnet

		Accessors:   statecfg.AccessorBTree | statecfg.AccessorExistence,
		largeValues: true,

		hist: statecfg.HistCfg{
			ValuesTable:   kv.TblCodeHistoryVals,
			CompressorCfg: seg.DefaultCfg, Compression: seg.CompressKeys | seg.CompressVals,

			HistoryLargeValues: true,
			HistoryIdx:         kv.CodeHistoryIdx,

			IiCfg: statecfg.InvIdx{
				FilenameBase: kv.CodeDomain.String(), KeysTable: kv.TblCodeHistoryKeys, ValuesTable: kv.TblCodeIdx,
				CompressorCfg: seg.DefaultCfg,
				Accessors:     statecfg.AccessorHashMap,
			},
		},
	},
	CommitmentDomain: domainCfg{
		name: kv.CommitmentDomain, valuesTable: kv.TblCommitmentVals,
		CompressCfg: DomainCompressCfg, Compression: seg.CompressKeys,

		Accessors:           statecfg.AccessorHashMap,
		replaceKeysInValues: AggregatorSqueezeCommitmentValues,

		hist: statecfg.HistCfg{
			ValuesTable:   kv.TblCommitmentHistoryVals,
			CompressorCfg: HistoryCompressCfg, Compression: seg.CompressNone, // seg.CompressKeys | seg.CompressVals,
			HistoryIdx: kv.CommitmentHistoryIdx,

			HistoryLargeValues:            false,
			HistoryValuesOnCompressedPage: 64,

			SnapshotsDisabled: true,
			HistoryDisabled:   true,

			IiCfg: statecfg.InvIdx{
				FilenameBase: kv.CommitmentDomain.String(), KeysTable: kv.TblCommitmentHistoryKeys, ValuesTable: kv.TblCommitmentIdx,
				CompressorCfg: seg.DefaultCfg,
				Accessors:     statecfg.AccessorHashMap,
			},
		},
	},
	ReceiptDomain: domainCfg{
		name: kv.ReceiptDomain, valuesTable: kv.TblReceiptVals,
		CompressCfg: seg.DefaultCfg, Compression: seg.CompressNone,
		largeValues: false,

		Accessors: statecfg.AccessorBTree | statecfg.AccessorExistence,

		hist: statecfg.HistCfg{
			ValuesTable:   kv.TblReceiptHistoryVals,
			CompressorCfg: seg.DefaultCfg, Compression: seg.CompressNone,

			HistoryLargeValues: false,
			HistoryIdx:         kv.ReceiptHistoryIdx,

			IiCfg: statecfg.InvIdx{
				FilenameBase: kv.ReceiptDomain.String(), KeysTable: kv.TblReceiptHistoryKeys, ValuesTable: kv.TblReceiptIdx,
				CompressorCfg: seg.DefaultCfg,
				Accessors:     statecfg.AccessorHashMap,
			},
		},
	},
	RCacheDomain: domainCfg{
		name: kv.RCacheDomain, valuesTable: kv.TblRCacheVals,
		largeValues: true,

		Accessors:   statecfg.AccessorHashMap,
		CompressCfg: DomainCompressCfg, Compression: seg.CompressNone, //seg.CompressKeys | seg.CompressVals,

		hist: statecfg.HistCfg{
			ValuesTable: kv.TblRCacheHistoryVals,
			Compression: seg.CompressNone, //seg.CompressKeys | seg.CompressVals,

			HistoryLargeValues: true,
			HistoryIdx:         kv.RCacheHistoryIdx,

			SnapshotsDisabled:             true,
			HistoryValuesOnCompressedPage: 16,

			IiCfg: statecfg.InvIdx{
				Disable:      true, // disable everything by default
				FilenameBase: kv.RCacheDomain.String(), KeysTable: kv.TblRCacheHistoryKeys, ValuesTable: kv.TblRCacheIdx,
				CompressorCfg: seg.DefaultCfg,
				Accessors:     statecfg.AccessorHashMap,
			},
		},
	},

	LogAddrIdx: statecfg.InvIdx{
		FilenameBase: kv.FileLogAddressIdx, KeysTable: kv.TblLogAddressKeys, ValuesTable: kv.TblLogAddressIdx,

		Compression: seg.CompressNone,
		Name:        kv.LogAddrIdx,
		Accessors:   statecfg.AccessorHashMap,
	},
	LogTopicIdx: statecfg.InvIdx{
		FilenameBase: kv.FileLogTopicsIdx, KeysTable: kv.TblLogTopicsKeys, ValuesTable: kv.TblLogTopicsIdx,

		Compression: seg.CompressNone,
		Name:        kv.LogTopicIdx,
		Accessors:   statecfg.AccessorHashMap,
	},
	TracesFromIdx: statecfg.InvIdx{
		FilenameBase: kv.FileTracesFromIdx, KeysTable: kv.TblTracesFromKeys, ValuesTable: kv.TblTracesFromIdx,

		Compression: seg.CompressNone,
		Name:        kv.TracesFromIdx,
		Accessors:   statecfg.AccessorHashMap,
	},
	TracesToIdx: statecfg.InvIdx{
		FilenameBase: kv.FileTracesToIdx, KeysTable: kv.TblTracesToKeys, ValuesTable: kv.TblTracesToIdx,

		Compression: seg.CompressNone,
		Name:        kv.TracesToIdx,
		Accessors:   statecfg.AccessorHashMap,
	},
}

func EnableHistoricalCommitment() {
	cfg := Schema.CommitmentDomain
	cfg.hist.HistoryDisabled = false
	cfg.hist.SnapshotsDisabled = false
	Schema.CommitmentDomain = cfg
}

/*
  - v1.0 -> v2.0  is a breaking change. It causes a change in interpretation of "logFirstIdx" stored in receipt domain.
  - We wanted backwards compatibility however, so that was done with if checks, See `ReceiptStoresFirstLogIdx`
  - This brings problem that data coming from v1.0 vs v2.0 is interpreted by app in different ways,
    and so the version needs to be floated up to the application.
  - So to simplify matters, we need to do- v1.0 files, if it appears, must appear alone (no v2.0 etc.)
  - This function updates current version to v1.1  (to differentiate file created from 3.0 vs 3.1 erigon)
    issue: https://github.com/erigontech/erigon/issues/16293

Use this before creating aggregator.
*/
func AdjustReceiptCurrentVersionIfNeeded(dirs datadir.Dirs, logger log.Logger) error {
	found := false
	return filepath.WalkDir(dirs.SnapDomain, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if found {
			return nil
		}
		if entry.IsDir() {
			return nil
		}

		name := entry.Name()
		res, isE3Seedable, ok := snaptype.ParseFileName(path, name)
		if !isE3Seedable {
			return nil
		}
		if !ok {
			return fmt.Errorf("[adjust_receipt] couldn't parse: %s at %s", name, path)
		}

		if res.TypeString != "receipt" || res.Ext != ".kv" {
			return nil
		}

		found = true

		if res.Version.Cmp(version.V2_0) >= 0 {
			return nil
		}

		logger.Info("adjusting receipt current version to v1.1")

		// else v1.0 -- need to adjust version
		Schema.ReceiptDomain.version.DataKV = version.V1_1_standart
		Schema.ReceiptDomain.hist.Version.DataV = version.V1_1_standart

		return nil
	})
}

var DomainCompressCfg = seg.Cfg{
	MinPatternScore:      1000,
	DictReducerSoftLimit: 2000000,
	MinPatternLen:        20,
	MaxPatternLen:        128,
	SamplingFactor:       1,
	MaxDictPatterns:      64 * 1024,
	Workers:              1,
}

var HistoryCompressCfg = seg.Cfg{
	MinPatternScore:      4000,
	DictReducerSoftLimit: 2000000,
	MinPatternLen:        20,
	MaxPatternLen:        128,
	SamplingFactor:       1,
	MaxDictPatterns:      64 * 1024,
	Workers:              1,
}

func EnableHistoricalRCache() {
	cfg := Schema.RCacheDomain
	cfg.hist.IiCfg.Disable = false
	cfg.hist.HistoryDisabled = false
	cfg.hist.SnapshotsDisabled = false
	Schema.RCacheDomain = cfg
}

var SchemeMinSupportedVersions = map[string]map[string]snaptype.Version{}

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
					"new version: `erigon seg update-to-new-ver-format`")
			}
			fileInfo, _, _ := snaptype.ParseFileName("", name)

			currentFileVersion := fileInfo.Version

			msVs, ok := SchemeMinSupportedVersions[fileInfo.TypeString]
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
