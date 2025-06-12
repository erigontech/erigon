package state

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/seg"
)

// this is supposed to register domains/iis

func NewAggregator2(ctx context.Context, dirs datadir.Dirs, aggregationStep uint64, db kv.RoDB, logger log.Logger) (*Aggregator, error) {
	salt, err := getStateIndicesSalt(dirs.Snap)
	if err != nil {
		return nil, err
	}

	a, err := NewAggregator(ctx, dirs, aggregationStep, db, logger)
	if err != nil {
		return nil, err
	}
	if err := a.registerDomain(kv.AccountsDomain, salt, dirs, aggregationStep, logger); err != nil {
		return nil, err
	}
	if err := a.registerDomain(kv.StorageDomain, salt, dirs, aggregationStep, logger); err != nil {
		return nil, err
	}
	if err := a.registerDomain(kv.CodeDomain, salt, dirs, aggregationStep, logger); err != nil {
		return nil, err
	}
	if err := a.registerDomain(kv.CommitmentDomain, salt, dirs, aggregationStep, logger); err != nil {
		return nil, err
	}
	if err := a.registerDomain(kv.ReceiptDomain, salt, dirs, aggregationStep, logger); err != nil {
		return nil, err
	}
	if err := a.registerDomain(kv.RCacheDomain, salt, dirs, aggregationStep, logger); err != nil {
		return nil, err
	}
	if err := a.registerII(kv.LogAddrIdx, salt, dirs, aggregationStep, kv.FileLogAddressIdx, kv.TblLogAddressKeys, kv.TblLogAddressIdx, logger); err != nil {
		return nil, err
	}
	if err := a.registerII(kv.LogTopicIdx, salt, dirs, aggregationStep, kv.FileLogTopicsIdx, kv.TblLogTopicsKeys, kv.TblLogTopicsIdx, logger); err != nil {
		return nil, err
	}
	if err := a.registerII(kv.TracesFromIdx, salt, dirs, aggregationStep, kv.FileTracesFromIdx, kv.TblTracesFromKeys, kv.TblTracesFromIdx, logger); err != nil {
		return nil, err
	}
	if err := a.registerII(kv.TracesToIdx, salt, dirs, aggregationStep, kv.FileTracesToIdx, kv.TblTracesToKeys, kv.TblTracesToIdx, logger); err != nil {
		return nil, err
	}
	a.KeepRecentTxnsOfHistoriesWithDisabledSnapshots(100_000) // ~1k blocks of history

	a.dirtyFilesLock.Lock()
	defer a.dirtyFilesLock.Unlock()
	a.recalcVisibleFiles(a.dirtyFilesEndTxNumMinimax())

	return a, nil
}

var dbgCommBtIndex = dbg.EnvBool("AGG_COMMITMENT_BT", false)

func init() {
	if dbgCommBtIndex {
		cfg := Schema[kv.CommitmentDomain]
		cfg.Accessors = AccessorBTree | AccessorExistence
		Schema[kv.CommitmentDomain] = cfg
	}
}

var Schema = map[kv.Domain]domainCfg{
	kv.AccountsDomain: {
		name: kv.AccountsDomain, valuesTable: kv.TblAccountVals,

		Accessors:            AccessorBTree | AccessorExistence,
		crossDomainIntegrity: domainIntegrityCheck,
		Compression:          seg.CompressNone,
		CompressCfg:          DomainCompressCfg,

		hist: histCfg{
			valuesTable: kv.TblAccountHistoryVals,
			Compression: seg.CompressNone,

			historyLargeValues: false,
			filenameBase:       kv.AccountsDomain.String(), //TODO: looks redundant
			historyIdx:         kv.AccountsHistoryIdx,

			iiCfg: iiCfg{
				keysTable: kv.TblAccountHistoryKeys, valuesTable: kv.TblAccountIdx,
				CompressorCfg: seg.DefaultCfg,
				filenameBase:  kv.AccountsDomain.String(), //TODO: looks redundant
			},
		},
	},
	kv.StorageDomain: {
		name: kv.StorageDomain, valuesTable: kv.TblStorageVals,

		Accessors:   AccessorBTree | AccessorExistence,
		Compression: seg.CompressKeys,
		CompressCfg: DomainCompressCfg,

		hist: histCfg{
			valuesTable: kv.TblStorageHistoryVals,
			Compression: seg.CompressNone,

			historyLargeValues: false,
			filenameBase:       kv.StorageDomain.String(),
			historyIdx:         kv.StorageHistoryIdx,

			iiCfg: iiCfg{
				keysTable: kv.TblStorageHistoryKeys, valuesTable: kv.TblStorageIdx,
				CompressorCfg: seg.DefaultCfg,
				filenameBase:  kv.StorageDomain.String(),
			},
		},
	},
	kv.CodeDomain: {
		name: kv.CodeDomain, valuesTable: kv.TblCodeVals,

		Accessors:   AccessorBTree | AccessorExistence,
		Compression: seg.CompressVals, // compress Code with keys doesn't show any profit. compress of values show 4x ratio on eth-mainnet and 2.5x ratio on bor-mainnet
		CompressCfg: DomainCompressCfg,
		largeValues: true,

		hist: histCfg{
			valuesTable: kv.TblCodeHistoryVals,
			Compression: seg.CompressKeys | seg.CompressVals,

			historyLargeValues: true,
			filenameBase:       kv.CodeDomain.String(),
			historyIdx:         kv.CodeHistoryIdx,

			iiCfg: iiCfg{
				CompressorCfg: seg.DefaultCfg,
				keysTable:     kv.TblCodeHistoryKeys, valuesTable: kv.TblCodeIdx,
				filenameBase: kv.CodeDomain.String(),
			},
		},
	},
	kv.CommitmentDomain: {
		name: kv.CommitmentDomain, valuesTable: kv.TblCommitmentVals,

		Accessors:           AccessorHashMap,
		Compression:         seg.CompressKeys,
		CompressCfg:         DomainCompressCfg,
		replaceKeysInValues: AggregatorSqueezeCommitmentValues,

		hist: histCfg{
			valuesTable:   kv.TblCommitmentHistoryVals,
			CompressorCfg: HistoryCompressCfg, Compression: seg.CompressNone,
			filenameBase: kv.CommitmentDomain.String(),
			historyIdx:   kv.CommitmentHistoryIdx,

			historyLargeValues:            false,
			historyValuesOnCompressedPage: 16,

			snapshotsDisabled: true,
			historyDisabled:   true,

			iiCfg: iiCfg{
				keysTable: kv.TblCommitmentHistoryKeys, valuesTable: kv.TblCommitmentIdx,
				CompressorCfg: seg.DefaultCfg,
				filenameBase:  kv.CommitmentDomain.String(),
			},
		},
	},
	kv.ReceiptDomain: {
		name: kv.ReceiptDomain, valuesTable: kv.TblReceiptVals,
		CompressCfg: seg.DefaultCfg, Compression: seg.CompressNone,
		largeValues: false,

		Accessors: AccessorBTree | AccessorExistence,

		hist: histCfg{
			valuesTable: kv.TblReceiptHistoryVals,
			Compression: seg.CompressNone,

			historyLargeValues: false,
			filenameBase:       kv.ReceiptDomain.String(),
			historyIdx:         kv.ReceiptHistoryIdx,

			iiCfg: iiCfg{
				keysTable: kv.TblReceiptHistoryKeys, valuesTable: kv.TblReceiptIdx,
				CompressorCfg: seg.DefaultCfg,
				filenameBase:  kv.ReceiptDomain.String(),
			},
		},
	},
	kv.RCacheDomain: {
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

			filenameBase: kv.RCacheDomain.String(),

			iiCfg: iiCfg{
				disable:      true, // disable everything by default
				filenameBase: kv.RCacheDomain.String(), keysTable: kv.TblRCacheHistoryKeys, valuesTable: kv.TblRCacheIdx,
				CompressorCfg: seg.DefaultCfg,
			},
		},
	},
}

var StandaloneIISchema = map[kv.InvertedIdx]iiCfg{
	kv.LogAddrIdx: {
		filenameBase: kv.FileLogAddressIdx, keysTable: kv.TblLogAddressKeys, valuesTable: kv.TblLogAddressIdx,

		Compression: seg.CompressNone,
		name:        kv.LogAddrIdx,
	},
	kv.LogTopicIdx: {
		filenameBase: kv.FileLogTopicsIdx, keysTable: kv.TblLogTopicsKeys, valuesTable: kv.TblLogTopicsIdx,

		Compression: seg.CompressNone,
		name:        kv.LogTopicIdx,
	},
	kv.TracesFromIdx: {
		filenameBase: kv.FileTracesFromIdx, keysTable: kv.TblTracesFromKeys, valuesTable: kv.TblTracesFromIdx,

		Compression: seg.CompressNone,
		name:        kv.TracesFromIdx,
	},
	kv.TracesToIdx: {
		filenameBase: kv.FileTracesToIdx, keysTable: kv.TblTracesToKeys, valuesTable: kv.TblTracesToIdx,

		Compression: seg.CompressNone,
		name:        kv.TracesToIdx,
	},
}

var DomainCompressCfg = seg.Cfg{
	MinPatternScore:      1000,
	DictReducerSoftLimit: 2000000,
	MinPatternLen:        20,
	MaxPatternLen:        128,
	SamplingFactor:       4,
	MaxDictPatterns:      64 * 1024 * 2,
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

func EnableHistoricalCommitment() {
	cfg := Schema[kv.CommitmentDomain]
	cfg.hist.historyDisabled = false
	cfg.hist.snapshotsDisabled = false
	Schema[kv.CommitmentDomain] = cfg
}
func EnableHistoricalRCache() {
	cfg := Schema[kv.RCacheDomain]
	cfg.hist.iiCfg.disable = false
	cfg.hist.historyDisabled = false
	cfg.hist.snapshotsDisabled = false
	Schema[kv.RCacheDomain] = cfg
}

var ExperimentalConcurrentCommitment = false // set true to use concurrent commitment by default

func Compatibility(d datadir.Dirs) error {
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
				if strings.HasPrefix(name, "v1.0-") {
					return errors.New("bad incompatible snapshots with current erigon version. " +
						"Check twice version or run `erigon seg update-to-new-ver-format` command")
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
