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
		Schema.CommitmentDomain.AccessorList = AccessorBTree | AccessorExistence
	}
	InitSchemas()
	InitAccountSchemaIntegrity()
}

type SchemaGen struct {
	AccountsDomain   domainCfg
	StorageDomain    domainCfg
	CodeDomain       domainCfg
	CommitmentDomain domainCfg
	ReceiptDomain    domainCfg
	LogAddrIdx       iiCfg
	LogTopicIdx      iiCfg
	TracesFromIdx    iiCfg
	TracesToIdx      iiCfg
}

func (s *SchemaGen) GetDomainCfg(name kv.Domain) domainCfg {
	switch name {
	case kv.AccountsDomain:
		return s.AccountsDomain
	case kv.StorageDomain:
		return s.StorageDomain
	case kv.CodeDomain:
		return s.CodeDomain
	case kv.CommitmentDomain:
		return s.CommitmentDomain
	case kv.ReceiptDomain:
		return s.ReceiptDomain
	default:
		return domainCfg{}
	}
}

func (s *SchemaGen) GetIICfg(name kv.InvertedIdx) iiCfg {
	switch name {
	case kv.LogAddrIdx:
		return s.LogAddrIdx
	case kv.LogTopicIdx:
		return s.LogTopicIdx
	case kv.TracesFromIdx:
		return s.TracesFromIdx
	case kv.TracesToIdx:
		return s.TracesToIdx
	default:
		return iiCfg{}
	}
}

var Schema = SchemaGen{
	AccountsDomain: domainCfg{
		name: kv.AccountsDomain, valuesTable: kv.TblAccountVals,
		CompressCfg: DomainCompressCfg, Compression: seg.CompressNone,

		AccessorList:         AccessorBTree | AccessorExistence,
		crossDomainIntegrity: domainIntegrityCheck,

		hist: histCfg{
			valuesTable:   kv.TblAccountHistoryVals,
			compressorCfg: seg.DefaultCfg, compression: seg.CompressNone,

			historyLargeValues: false,
			filenameBase:       kv.AccountsDomain.String(), //TODO: looks redundant
			historyIdx:         kv.AccountsHistoryIdx,

			iiCfg: iiCfg{
				keysTable: kv.TblAccountHistoryKeys, valuesTable: kv.TblAccountIdx,
				compressorCfg: seg.DefaultCfg,
				filenameBase:  kv.AccountsDomain.String(), //TODO: looks redundant
			},
		},
	},
	StorageDomain: domainCfg{
		name: kv.StorageDomain, valuesTable: kv.TblStorageVals,
		CompressCfg: DomainCompressCfg, Compression: seg.CompressKeys,

		AccessorList: AccessorBTree | AccessorExistence,

		hist: histCfg{
			valuesTable:   kv.TblStorageHistoryVals,
			compressorCfg: seg.DefaultCfg, compression: seg.CompressNone,

			historyLargeValues: false,
			filenameBase:       kv.StorageDomain.String(),
			historyIdx:         kv.StorageHistoryIdx,

			iiCfg: iiCfg{
				keysTable: kv.TblStorageHistoryKeys, valuesTable: kv.TblStorageIdx,
				compressorCfg: seg.DefaultCfg,
				filenameBase:  kv.StorageDomain.String(),
			},
		},
	},
	CodeDomain: domainCfg{
		name: kv.CodeDomain, valuesTable: kv.TblCodeVals,
		CompressCfg: DomainCompressCfg, Compression: seg.CompressVals, // compress Code with keys doesn't show any profit. compress of values show 4x ratio on eth-mainnet and 2.5x ratio on bor-mainnet

		AccessorList: AccessorBTree | AccessorExistence,
		largeValues:  true,

		hist: histCfg{
			valuesTable:   kv.TblCodeHistoryVals,
			compressorCfg: seg.DefaultCfg, compression: seg.CompressKeys | seg.CompressVals,

			historyLargeValues: true,
			filenameBase:       kv.CodeDomain.String(),
			historyIdx:         kv.CodeHistoryIdx,

			iiCfg: iiCfg{
				keysTable: kv.TblCodeHistoryKeys, valuesTable: kv.TblCodeIdx,
				compressorCfg: seg.DefaultCfg,
				filenameBase:  kv.CodeDomain.String(),
			},
		},
	},
	CommitmentDomain: domainCfg{
		name: kv.CommitmentDomain, valuesTable: kv.TblCommitmentVals,
		CompressCfg: DomainCompressCfg, Compression: seg.CompressKeys,

		AccessorList:        AccessorHashMap,
		replaceKeysInValues: AggregatorSqueezeCommitmentValues,

		hist: histCfg{
			valuesTable:   kv.TblCommitmentHistoryVals,
			compressorCfg: HistoryCompressCfg, compression: seg.CompressNone,

			snapshotsDisabled:  true,
			historyLargeValues: false,
			filenameBase:       kv.CommitmentDomain.String(),
			historyIdx:         kv.CommitmentHistoryIdx,
			historyDisabled:    true,

			iiCfg: iiCfg{
				keysTable: kv.TblCommitmentHistoryKeys, valuesTable: kv.TblCommitmentIdx,
				compressorCfg: seg.DefaultCfg,
				filenameBase:  kv.CommitmentDomain.String(),
			},
		},
	},
	ReceiptDomain: domainCfg{
		name: kv.ReceiptDomain, valuesTable: kv.TblReceiptVals,
		CompressCfg: seg.DefaultCfg, Compression: seg.CompressNone,

		AccessorList: AccessorBTree | AccessorExistence,

		hist: histCfg{
			valuesTable:   kv.TblReceiptHistoryVals,
			compressorCfg: seg.DefaultCfg, compression: seg.CompressNone,

			historyLargeValues: false,
			filenameBase:       kv.ReceiptDomain.String(),
			historyIdx:         kv.ReceiptHistoryIdx,

			iiCfg: iiCfg{
				keysTable: kv.TblReceiptHistoryKeys, valuesTable: kv.TblReceiptIdx,
				compressorCfg: seg.DefaultCfg,
				filenameBase:  kv.ReceiptDomain.String(),
			},
		},
	},
	LogAddrIdx: iiCfg{
		filenameBase: kv.FileLogAddressIdx, keysTable: kv.TblLogAddressKeys, valuesTable: kv.TblLogAddressIdx,

		compression: seg.CompressNone,
		name:        kv.LogAddrIdx,
	},
	LogTopicIdx: iiCfg{
		filenameBase: kv.FileLogTopicsIdx, keysTable: kv.TblLogTopicsKeys, valuesTable: kv.TblLogTopicsIdx,

		compression: seg.CompressNone,
		name:        kv.LogTopicIdx,
	},
	TracesFromIdx: iiCfg{
		filenameBase: kv.FileTracesFromIdx, keysTable: kv.TblTracesFromKeys, valuesTable: kv.TblTracesFromIdx,

		compression: seg.CompressNone,
		name:        kv.TracesFromIdx,
	},
	TracesToIdx: iiCfg{
		filenameBase: kv.FileTracesToIdx, keysTable: kv.TblTracesToKeys, valuesTable: kv.TblTracesToIdx,

		compression: seg.CompressNone,
		name:        kv.TracesToIdx,
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
	SamplingFactor:       4,
	MaxDictPatterns:      64 * 1024 * 2,
	Workers:              1,
}

var HistoryCompressCfg = seg.Cfg{
	MinPatternScore:      8000,
	DictReducerSoftLimit: 2000000,
	MinPatternLen:        20,
	MaxPatternLen:        128,
	SamplingFactor:       1,
	MaxDictPatterns:      64 * 1024 * 2,
	Workers:              1,
}
