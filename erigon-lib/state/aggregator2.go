package state

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/erigontech/erigon-lib/snaptype"
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
	if err := a.registerDomain(kv.AccountsDomain, salt, dirs, logger); err != nil {
		return nil, err
	}
	if err := a.registerDomain(kv.StorageDomain, salt, dirs, logger); err != nil {
		return nil, err
	}
	if err := a.registerDomain(kv.CodeDomain, salt, dirs, logger); err != nil {
		return nil, err
	}
	if err := a.registerDomain(kv.CommitmentDomain, salt, dirs, logger); err != nil {
		return nil, err
	}
	if err := a.registerDomain(kv.ReceiptDomain, salt, dirs, logger); err != nil {
		return nil, err
	}
	if err := a.registerDomain(kv.RCacheDomain, salt, dirs, logger); err != nil {
		return nil, err
	}
	if err := a.registerII(kv.LogAddrIdx, salt, dirs, logger); err != nil {
		return nil, err
	}
	if err := a.registerII(kv.LogTopicIdx, salt, dirs, logger); err != nil {
		return nil, err
	}
	if err := a.registerII(kv.TracesFromIdx, salt, dirs, logger); err != nil {
		return nil, err
	}
	if err := a.registerII(kv.TracesToIdx, salt, dirs, logger); err != nil {
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
		Schema.CommitmentDomain.Accessors = AccessorBTree | AccessorExistence
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
	LogAddrIdx       iiCfg
	LogTopicIdx      iiCfg
	TracesFromIdx    iiCfg
	TracesToIdx      iiCfg
}

type Versioned interface {
	GetVersions() VersionTypes
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
	v.hist.iiCfg.salt = new(atomic.Pointer[uint32])
	return v
}

func (s *SchemaGen) GetIICfg(name kv.InvertedIdx) iiCfg {
	var v iiCfg
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
		v = iiCfg{}
	}
	v.salt = new(atomic.Pointer[uint32])
	return v
}

var ExperimentalConcurrentCommitment = false // set true to use concurrent commitment by default

var Schema = SchemaGen{
	AccountsDomain: domainCfg{
		name: kv.AccountsDomain, valuesTable: kv.TblAccountVals,
		CompressCfg: DomainCompressCfg, Compression: seg.CompressNone,

		Accessors: AccessorBTree | AccessorExistence,

		hist: histCfg{
			valuesTable:   kv.TblAccountHistoryVals,
			CompressorCfg: seg.DefaultCfg, Compression: seg.CompressNone,

			historyLargeValues: false,
			historyIdx:         kv.AccountsHistoryIdx,

			iiCfg: iiCfg{
				filenameBase: kv.AccountsDomain.String(), keysTable: kv.TblAccountHistoryKeys, valuesTable: kv.TblAccountIdx,
				CompressorCfg: seg.DefaultCfg,
				Accessors:     AccessorHashMap,
			},
		},
	},
	StorageDomain: domainCfg{
		name: kv.StorageDomain, valuesTable: kv.TblStorageVals,
		CompressCfg: DomainCompressCfg, Compression: seg.CompressKeys,

		Accessors: AccessorBTree | AccessorExistence,

		hist: histCfg{
			valuesTable:   kv.TblStorageHistoryVals,
			CompressorCfg: seg.DefaultCfg, Compression: seg.CompressNone,

			historyLargeValues: false,
			historyIdx:         kv.StorageHistoryIdx,

			iiCfg: iiCfg{
				filenameBase: kv.StorageDomain.String(), keysTable: kv.TblStorageHistoryKeys, valuesTable: kv.TblStorageIdx,
				CompressorCfg: seg.DefaultCfg,
				Accessors:     AccessorHashMap,
			},
		},
	},
	CodeDomain: domainCfg{
		name: kv.CodeDomain, valuesTable: kv.TblCodeVals,
		CompressCfg: DomainCompressCfg, Compression: seg.CompressVals, // compressing Code with keys doesn't show any benefits. Compression of values shows 4x ratio on eth-mainnet and 2.5x ratio on bor-mainnet

		Accessors:   AccessorBTree | AccessorExistence,
		largeValues: true,

		hist: histCfg{
			valuesTable:   kv.TblCodeHistoryVals,
			CompressorCfg: seg.DefaultCfg, Compression: seg.CompressKeys | seg.CompressVals,

			historyLargeValues: true,
			historyIdx:         kv.CodeHistoryIdx,

			iiCfg: iiCfg{
				filenameBase: kv.CodeDomain.String(), keysTable: kv.TblCodeHistoryKeys, valuesTable: kv.TblCodeIdx,
				CompressorCfg: seg.DefaultCfg,
				Accessors:     AccessorHashMap,
			},
		},
	},
	CommitmentDomain: domainCfg{
		name: kv.CommitmentDomain, valuesTable: kv.TblCommitmentVals,
		CompressCfg: DomainCompressCfg, Compression: seg.CompressKeys,

		Accessors:           AccessorHashMap,
		replaceKeysInValues: AggregatorSqueezeCommitmentValues,

		hist: histCfg{
			valuesTable:   kv.TblCommitmentHistoryVals,
			CompressorCfg: HistoryCompressCfg, Compression: seg.CompressNone, // seg.CompressKeys | seg.CompressVals,
			historyIdx: kv.CommitmentHistoryIdx,

			historyLargeValues:            false,
			historyValuesOnCompressedPage: 16,

			snapshotsDisabled: true,
			historyDisabled:   true,

			iiCfg: iiCfg{
				filenameBase: kv.CommitmentDomain.String(), keysTable: kv.TblCommitmentHistoryKeys, valuesTable: kv.TblCommitmentIdx,
				CompressorCfg: seg.DefaultCfg,
				Accessors:     AccessorHashMap,
			},
		},
	},
	ReceiptDomain: domainCfg{
		name: kv.ReceiptDomain, valuesTable: kv.TblReceiptVals,
		CompressCfg: seg.DefaultCfg, Compression: seg.CompressNone,
		largeValues: false,

		Accessors: AccessorBTree | AccessorExistence,

		hist: histCfg{
			valuesTable:   kv.TblReceiptHistoryVals,
			CompressorCfg: seg.DefaultCfg, Compression: seg.CompressNone,

			historyLargeValues: false,
			historyIdx:         kv.ReceiptHistoryIdx,

			iiCfg: iiCfg{
				filenameBase: kv.ReceiptDomain.String(), keysTable: kv.TblReceiptHistoryKeys, valuesTable: kv.TblReceiptIdx,
				CompressorCfg: seg.DefaultCfg,
				Accessors:     AccessorHashMap,
			},
		},
	},
	RCacheDomain: domainCfg{
		name: kv.RCacheDomain, valuesTable: kv.TblRCacheVals,
		largeValues: true,

		Accessors:   AccessorHashMap,
		CompressCfg: DomainCompressCfg, Compression: seg.CompressNone, //seg.CompressKeys | seg.CompressVals,

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
				CompressorCfg: seg.DefaultCfg,
				Accessors:     AccessorHashMap,
			},
		},
	},

	LogAddrIdx: iiCfg{
		filenameBase: kv.FileLogAddressIdx, keysTable: kv.TblLogAddressKeys, valuesTable: kv.TblLogAddressIdx,

		Compression: seg.CompressNone,
		name:        kv.LogAddrIdx,
		Accessors:   AccessorHashMap,
	},
	LogTopicIdx: iiCfg{
		filenameBase: kv.FileLogTopicsIdx, keysTable: kv.TblLogTopicsKeys, valuesTable: kv.TblLogTopicsIdx,

		Compression: seg.CompressNone,
		name:        kv.LogTopicIdx,
		Accessors:   AccessorHashMap,
	},
	TracesFromIdx: iiCfg{
		filenameBase: kv.FileTracesFromIdx, keysTable: kv.TblTracesFromKeys, valuesTable: kv.TblTracesFromIdx,

		Compression: seg.CompressNone,
		name:        kv.TracesFromIdx,
		Accessors:   AccessorHashMap,
	},
	TracesToIdx: iiCfg{
		filenameBase: kv.FileTracesToIdx, keysTable: kv.TblTracesToKeys, valuesTable: kv.TblTracesToIdx,

		Compression: seg.CompressNone,
		name:        kv.TracesToIdx,
		Accessors:   AccessorHashMap,
	},
}

func EnableHistoricalCommitment() {
	cfg := Schema.CommitmentDomain
	cfg.hist.historyDisabled = false
	cfg.hist.snapshotsDisabled = false
	Schema.CommitmentDomain = cfg
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
	cfg.hist.iiCfg.disable = false
	cfg.hist.historyDisabled = false
	cfg.hist.snapshotsDisabled = false
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
				return err
			}

			if !entry.IsDir() {
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
			}
			return nil
		})

		if err != nil {
			return err
		}
	}

	return nil
}
