package state

import (
	"context"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/seg"
)

// this is supposed to register domains/iis

func NewAggregator(ctx context.Context, dirs datadir.Dirs, aggregationStep uint64, db kv.RoDB, logger log.Logger) (*Aggregator, error) {
	salt, err := getStateIndicesSalt(dirs.Snap)
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
		cfg.AccessorList = AccessorBTree | AccessorExistence
		Schema[kv.CommitmentDomain] = cfg
	}
}

var Schema = map[kv.Domain]domainCfg{
	kv.AccountsDomain: {
		name: kv.AccountsDomain, valuesTable: kv.TblAccountVals,
		CompressCfg: DomainCompressCfg, Compression: seg.CompressNone,

		AccessorList:         AccessorBTree | AccessorExistence,
		crossDomainIntegrity: domainIntegrityCheck,

		hist: histCfg{
			valuesTable:   kv.TblAccountHistoryVals,
			compressorCfg: seg.DefaultCfg, compression: seg.CompressNone,

			historyLargeValues: false,
			historyIdx:         kv.AccountsHistoryIdx,

			iiCfg: iiCfg{
				filenameBase: kv.AccountsDomain.String(), keysTable: kv.TblAccountHistoryKeys, valuesTable: kv.TblAccountIdx,
				compressorCfg: seg.DefaultCfg,
			},
		},
	},
	kv.StorageDomain: {
		name: kv.StorageDomain, valuesTable: kv.TblStorageVals,
		CompressCfg: DomainCompressCfg, Compression: seg.CompressKeys,

		AccessorList: AccessorBTree | AccessorExistence,

		hist: histCfg{
			valuesTable:   kv.TblStorageHistoryVals,
			compressorCfg: seg.DefaultCfg, compression: seg.CompressNone,

			historyLargeValues: false,
			historyIdx:         kv.StorageHistoryIdx,

			iiCfg: iiCfg{
				filenameBase: kv.StorageDomain.String(), keysTable: kv.TblStorageHistoryKeys, valuesTable: kv.TblStorageIdx,
				compressorCfg: seg.DefaultCfg,
			},
		},
	},
	kv.CodeDomain: {
		name: kv.CodeDomain, valuesTable: kv.TblCodeVals,
		CompressCfg: DomainCompressCfg, Compression: seg.CompressVals, // compress Code with keys doesn't show any profit. compress of values show 4x ratio on eth-mainnet and 2.5x ratio on bor-mainnet

		AccessorList: AccessorBTree | AccessorExistence,
		largeValues:  true,

		hist: histCfg{
			valuesTable:   kv.TblCodeHistoryVals,
			compressorCfg: seg.DefaultCfg, compression: seg.CompressKeys | seg.CompressVals,

			historyLargeValues: true,
			historyIdx:         kv.CodeHistoryIdx,

			iiCfg: iiCfg{
				filenameBase: kv.CodeDomain.String(), keysTable: kv.TblCodeHistoryKeys, valuesTable: kv.TblCodeIdx,
				compressorCfg: seg.DefaultCfg,
			},
		},
	},
	kv.CommitmentDomain: {
		name: kv.CommitmentDomain, valuesTable: kv.TblCommitmentVals,
		CompressCfg: DomainCompressCfg, Compression: seg.CompressKeys,

		AccessorList:        AccessorHashMap,
		replaceKeysInValues: AggregatorSqueezeCommitmentValues,

		hist: histCfg{
			valuesTable:   kv.TblCommitmentHistoryVals,
			compressorCfg: HistoryCompressCfg, compression: seg.CompressNone, // seg.CompressKeys | seg.CompressVals,
			historyIdx: kv.CommitmentHistoryIdx,

			historyLargeValues: false,
			compressSingleVal:  false,

			snapshotsDisabled: true,
			historyDisabled:   true,

			iiCfg: iiCfg{
				filenameBase: kv.CommitmentDomain.String(), keysTable: kv.TblCommitmentHistoryKeys, valuesTable: kv.TblCommitmentIdx,
				compressorCfg: seg.DefaultCfg,
			},
		},
	},
	kv.ReceiptDomain: {
		name: kv.ReceiptDomain, valuesTable: kv.TblReceiptVals,
		CompressCfg: seg.DefaultCfg, Compression: seg.CompressNone,

		AccessorList: AccessorBTree | AccessorExistence,

		hist: histCfg{
			valuesTable:   kv.TblReceiptHistoryVals,
			compressorCfg: seg.DefaultCfg, compression: seg.CompressNone,

			historyLargeValues: false,
			historyIdx:         kv.ReceiptHistoryIdx,

			iiCfg: iiCfg{
				filenameBase: kv.ReceiptDomain.String(), keysTable: kv.TblReceiptHistoryKeys, valuesTable: kv.TblReceiptIdx,
				compressorCfg: seg.DefaultCfg,
			},
		},
	},
}

func EnableHistoricalCommitment() {
	cfg := Schema[kv.CommitmentDomain]
	cfg.hist.historyDisabled = false
	cfg.hist.snapshotsDisabled = false
	Schema[kv.CommitmentDomain] = cfg
}

var ExperimentalConcurrentCommitment = false // set true to use concurrent commitment by default

var StandaloneIISchema = map[kv.InvertedIdx]iiCfg{
	kv.LogAddrIdx: {
		filenameBase: kv.FileLogAddressIdx, keysTable: kv.TblLogAddressKeys, valuesTable: kv.TblLogAddressIdx,

		compression: seg.CompressNone,
		name:        kv.LogAddrIdx,
	},
	kv.LogTopicIdx: {
		filenameBase: kv.FileLogTopicsIdx, keysTable: kv.TblLogTopicsKeys, valuesTable: kv.TblLogTopicsIdx,

		compression: seg.CompressNone,
		name:        kv.LogTopicIdx,
	},
	kv.TracesFromIdx: {
		filenameBase: kv.FileTracesFromIdx, keysTable: kv.TblTracesFromKeys, valuesTable: kv.TblTracesFromIdx,

		compression: seg.CompressNone,
		name:        kv.TracesFromIdx,
	},
	kv.TracesToIdx: {
		filenameBase: kv.FileTracesToIdx, keysTable: kv.TblTracesToKeys, valuesTable: kv.TblTracesToIdx,

		compression: seg.CompressNone,
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
