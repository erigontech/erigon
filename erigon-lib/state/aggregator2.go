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
	a.recalcVisibleFiles(a.DirtyFilesEndTxNumMinimax())

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

		AccessorList:         AccessorBTree | AccessorExistence,
		crossDomainIntegrity: domainIntegrityCheck,
		Compression:          seg.CompressNone,
		CompressCfg:          DomainCompressCfg,

		hist: histCfg{
			valuesTable: kv.TblAccountHistoryVals,
			compression: seg.CompressNone,

			historyLargeValues: false,
			filenameBase:       kv.AccountsDomain.String(), //TODO: looks redundant
			historyIdx:         kv.AccountsHistoryIdx,

			iiCfg: iiCfg{
				keysTable: kv.TblAccountHistoryKeys, valuesTable: kv.TblAccountIdx,
				withExistence: false, compressorCfg: seg.DefaultCfg,
				filenameBase: kv.AccountsDomain.String(), //TODO: looks redundant
			},
		},
	},
	kv.StorageDomain: {
		name: kv.StorageDomain, valuesTable: kv.TblStorageVals,

		AccessorList: AccessorBTree | AccessorExistence,
		Compression:  seg.CompressKeys,
		CompressCfg:  DomainCompressCfg,

		hist: histCfg{
			valuesTable: kv.TblStorageHistoryVals,
			compression: seg.CompressNone,

			historyLargeValues: false,
			filenameBase:       kv.StorageDomain.String(),
			historyIdx:         kv.StorageHistoryIdx,

			iiCfg: iiCfg{
				keysTable: kv.TblStorageHistoryKeys, valuesTable: kv.TblStorageIdx,
				withExistence: false, compressorCfg: seg.DefaultCfg,
				filenameBase: kv.StorageDomain.String(),
			},
		},
	},
	kv.CodeDomain: {
		name: kv.CodeDomain, valuesTable: kv.TblCodeVals,

		AccessorList: AccessorBTree | AccessorExistence,
		Compression:  seg.CompressVals, // compress Code with keys doesn't show any profit. compress of values show 4x ratio on eth-mainnet and 2.5x ratio on bor-mainnet
		CompressCfg:  DomainCompressCfg,
		largeValues:  true,

		hist: histCfg{
			valuesTable: kv.TblCodeHistoryVals,
			compression: seg.CompressKeys | seg.CompressVals,

			historyLargeValues: true,
			filenameBase:       kv.CodeDomain.String(),
			historyIdx:         kv.CodeHistoryIdx,

			iiCfg: iiCfg{
				withExistence: false, compressorCfg: seg.DefaultCfg,
				keysTable: kv.TblCodeHistoryKeys, valuesTable: kv.TblCodeIdx,
				filenameBase: kv.CodeDomain.String(),
			},
		},
	},
	kv.CommitmentDomain: {
		name: kv.CommitmentDomain, valuesTable: kv.TblCommitmentVals,

		AccessorList:        AccessorHashMap,
		Compression:         seg.CompressKeys,
		CompressCfg:         DomainCompressCfg,
		replaceKeysInValues: AggregatorSqueezeCommitmentValues,

		hist: histCfg{
			valuesTable: kv.TblCommitmentHistoryVals,
			compression: seg.CompressNone,

			snapshotsDisabled:  true,
			historyLargeValues: false,
			filenameBase:       kv.CommitmentDomain.String(),
			historyIdx:         kv.CommitmentHistoryIdx,

			iiCfg: iiCfg{
				keysTable: kv.TblCommitmentHistoryKeys, valuesTable: kv.TblCommitmentIdx,
				withExistence: false, compressorCfg: seg.DefaultCfg,
				filenameBase: kv.CommitmentDomain.String(),
			},
		},
	},
	kv.ReceiptDomain: {
		name: kv.ReceiptDomain, valuesTable: kv.TblReceiptVals,

		AccessorList: AccessorBTree | AccessorExistence,
		Compression:  seg.CompressNone, //seg.CompressKeys | seg.CompressVals,
		CompressCfg:  DomainCompressCfg,

		hist: histCfg{
			valuesTable: kv.TblReceiptHistoryVals,
			compression: seg.CompressNone,

			historyLargeValues: false,
			filenameBase:       kv.ReceiptDomain.String(),
			historyIdx:         kv.ReceiptHistoryIdx,

			iiCfg: iiCfg{
				keysTable: kv.TblReceiptHistoryKeys, valuesTable: kv.TblReceiptIdx,
				withExistence: false, compressorCfg: seg.DefaultCfg,
				filenameBase: kv.ReceiptDomain.String(),
			},
		},
	},
}
