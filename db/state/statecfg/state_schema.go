package statecfg

import (
	"fmt"
	"io/fs"
	"path/filepath"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
)

// AggSetters interface - allow break deps to `state` package and keep all biz-logic in current package
type AggSetters interface {
	RegisterDomain(cfg DomainCfg, salt *uint32, dirs datadir.Dirs, logger log.Logger) error
	RegisterII(cfg InvIdxCfg, salt *uint32, dirs datadir.Dirs, logger log.Logger) error
	AddDependencyBtwnDomains(dependency kv.Domain, dependent kv.Domain)
	KeepRecentTxnsOfHistoriesWithDisabledSnapshots(recentTxs uint64)
}

func Configure(a AggSetters, dirs datadir.Dirs, salt *uint32, logger log.Logger) error {
	if err := AdjustReceiptCurrentVersionIfNeeded(dirs, logger); err != nil {
		return err
	}
	if err := a.RegisterDomain(Schema.GetDomainCfg(kv.AccountsDomain), salt, dirs, logger); err != nil {
		return err
	}
	if err := a.RegisterDomain(Schema.GetDomainCfg(kv.StorageDomain), salt, dirs, logger); err != nil {
		return err
	}
	if err := a.RegisterDomain(Schema.GetDomainCfg(kv.CodeDomain), salt, dirs, logger); err != nil {
		return err
	}
	if err := a.RegisterDomain(Schema.GetDomainCfg(kv.CommitmentDomain), salt, dirs, logger); err != nil {
		return err
	}
	if err := a.RegisterDomain(Schema.GetDomainCfg(kv.ReceiptDomain), salt, dirs, logger); err != nil {
		return err
	}
	if err := a.RegisterDomain(Schema.GetDomainCfg(kv.RCacheDomain), salt, dirs, logger); err != nil {
		return err
	}
	if err := a.RegisterII(Schema.GetIICfg(kv.LogAddrIdx), salt, dirs, logger); err != nil {
		return err
	}
	if err := a.RegisterII(Schema.GetIICfg(kv.LogTopicIdx), salt, dirs, logger); err != nil {
		return err
	}
	if err := a.RegisterII(Schema.GetIICfg(kv.TracesFromIdx), salt, dirs, logger); err != nil {
		return err
	}
	if err := a.RegisterII(Schema.GetIICfg(kv.TracesToIdx), salt, dirs, logger); err != nil {
		return err
	}

	a.AddDependencyBtwnDomains(kv.AccountsDomain, kv.CommitmentDomain)
	a.AddDependencyBtwnDomains(kv.StorageDomain, kv.CommitmentDomain)

	a.KeepRecentTxnsOfHistoriesWithDisabledSnapshots(100_000) // ~1k blocks of history
	return nil
}

const AggregatorSqueezeCommitmentValues = true
const MaxNonFuriousDirtySpacePerTx = 64 * datasize.MB

var dbgCommBtIndex = dbg.EnvBool("AGG_COMMITMENT_BT", false)

func init() {
	if dbgCommBtIndex {
		Schema.CommitmentDomain.Accessors = AccessorBTree | AccessorExistence
	}
	InitSchemas()
}

type SchemaGen struct {
	AccountsDomain   DomainCfg
	StorageDomain    DomainCfg
	CodeDomain       DomainCfg
	CommitmentDomain DomainCfg
	ReceiptDomain    DomainCfg
	RCacheDomain     DomainCfg
	LogAddrIdx       InvIdxCfg
	LogTopicIdx      InvIdxCfg
	TracesFromIdx    InvIdxCfg
	TracesToIdx      InvIdxCfg
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

func (s *SchemaGen) GetDomainCfg(name kv.Domain) DomainCfg {
	var v DomainCfg
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
		v = DomainCfg{}
	}
	return v
}

func (s *SchemaGen) GetIICfg(name kv.InvertedIdx) InvIdxCfg {
	var v InvIdxCfg
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
		v = InvIdxCfg{}
	}
	return v
}

var ExperimentalConcurrentCommitment = false // set true to use concurrent commitment by default

var Schema = SchemaGen{
	AccountsDomain: DomainCfg{
		Name: kv.AccountsDomain, ValuesTable: kv.TblAccountVals,
		CompressCfg: DomainCompressCfg, Compression: seg.CompressNone,

		Accessors: AccessorBTree | AccessorExistence,

		Hist: HistCfg{
			ValuesTable:   kv.TblAccountHistoryVals,
			CompressorCfg: seg.DefaultCfg, Compression: seg.CompressNone,

			HistoryLargeValues: false,
			HistoryIdx:         kv.AccountsHistoryIdx,

			IiCfg: InvIdxCfg{
				FilenameBase: kv.AccountsDomain.String(), KeysTable: kv.TblAccountHistoryKeys, ValuesTable: kv.TblAccountIdx,
				CompressorCfg: seg.DefaultCfg,
				Accessors:     AccessorHashMap,
			},
		},
	},
	StorageDomain: DomainCfg{
		Name: kv.StorageDomain, ValuesTable: kv.TblStorageVals,
		CompressCfg: DomainCompressCfg, Compression: seg.CompressKeys,

		Accessors: AccessorBTree | AccessorExistence,

		Hist: HistCfg{
			ValuesTable:   kv.TblStorageHistoryVals,
			CompressorCfg: seg.DefaultCfg, Compression: seg.CompressNone,

			HistoryLargeValues: false,
			HistoryIdx:         kv.StorageHistoryIdx,

			IiCfg: InvIdxCfg{
				FilenameBase: kv.StorageDomain.String(), KeysTable: kv.TblStorageHistoryKeys, ValuesTable: kv.TblStorageIdx,
				CompressorCfg: seg.DefaultCfg,
				Accessors:     AccessorHashMap,
			},
		},
	},
	CodeDomain: DomainCfg{
		Name: kv.CodeDomain, ValuesTable: kv.TblCodeVals,
		CompressCfg: DomainCompressCfg, Compression: seg.CompressVals, // compressing Code with keys doesn't show any benefits. Compression of values shows 4x ratio on eth-mainnet and 2.5x ratio on bor-mainnet

		Accessors:   AccessorBTree | AccessorExistence,
		LargeValues: true,

		Hist: HistCfg{
			ValuesTable:   kv.TblCodeHistoryVals,
			CompressorCfg: seg.DefaultCfg, Compression: seg.CompressKeys | seg.CompressVals,

			HistoryLargeValues: true,
			HistoryIdx:         kv.CodeHistoryIdx,

			IiCfg: InvIdxCfg{
				FilenameBase: kv.CodeDomain.String(), KeysTable: kv.TblCodeHistoryKeys, ValuesTable: kv.TblCodeIdx,
				CompressorCfg: seg.DefaultCfg,
				Accessors:     AccessorHashMap,
			},
		},
	},
	CommitmentDomain: DomainCfg{
		Name: kv.CommitmentDomain, ValuesTable: kv.TblCommitmentVals,
		CompressCfg: DomainCompressCfg, Compression: seg.CompressKeys,

		Accessors:           AccessorHashMap,
		ReplaceKeysInValues: AggregatorSqueezeCommitmentValues, // when true, keys are replaced in values during merge once file range reaches threshold

		Hist: HistCfg{
			ValuesTable:   kv.TblCommitmentHistoryVals,
			CompressorCfg: HistoryCompressCfg, Compression: seg.CompressNone, // seg.CompressKeys | seg.CompressVals,
			HistoryIdx: kv.CommitmentHistoryIdx,

			HistoryLargeValues:            false,
			HistoryValuesOnCompressedPage: 64,

			SnapshotsDisabled: true,
			HistoryDisabled:   true,

			IiCfg: InvIdxCfg{
				FilenameBase: kv.CommitmentDomain.String(), KeysTable: kv.TblCommitmentHistoryKeys, ValuesTable: kv.TblCommitmentIdx,
				CompressorCfg: seg.DefaultCfg,
				Accessors:     AccessorHashMap,
			},
		},
	},
	ReceiptDomain: DomainCfg{
		Name: kv.ReceiptDomain, ValuesTable: kv.TblReceiptVals,
		CompressCfg: seg.DefaultCfg, Compression: seg.CompressNone,
		LargeValues: false,

		Accessors: AccessorBTree | AccessorExistence,

		Hist: HistCfg{
			ValuesTable:   kv.TblReceiptHistoryVals,
			CompressorCfg: seg.DefaultCfg, Compression: seg.CompressNone,

			HistoryLargeValues: false,
			HistoryIdx:         kv.ReceiptHistoryIdx,

			IiCfg: InvIdxCfg{
				FilenameBase: kv.ReceiptDomain.String(), KeysTable: kv.TblReceiptHistoryKeys, ValuesTable: kv.TblReceiptIdx,
				CompressorCfg: seg.DefaultCfg,
				Accessors:     AccessorHashMap,
			},
		},
	},
	RCacheDomain: DomainCfg{
		Name: kv.RCacheDomain, ValuesTable: kv.TblRCacheVals,
		LargeValues: true,

		Accessors:   AccessorHashMap,
		CompressCfg: DomainCompressCfg, Compression: seg.CompressNone, //seg.CompressKeys | seg.CompressVals,

		Hist: HistCfg{
			ValuesTable: kv.TblRCacheHistoryVals,
			Compression: seg.CompressNone, //seg.CompressKeys | seg.CompressVals,

			HistoryLargeValues: true,
			HistoryIdx:         kv.RCacheHistoryIdx,

			SnapshotsDisabled:             true,
			HistoryValuesOnCompressedPage: 16,

			IiCfg: InvIdxCfg{
				Disable:      true, // disable everything by default
				FilenameBase: kv.RCacheDomain.String(), KeysTable: kv.TblRCacheHistoryKeys, ValuesTable: kv.TblRCacheIdx,
				CompressorCfg: seg.DefaultCfg,
				Accessors:     AccessorHashMap,
			},
		},
	},

	LogAddrIdx: InvIdxCfg{
		FilenameBase: kv.FileLogAddressIdx, KeysTable: kv.TblLogAddressKeys, ValuesTable: kv.TblLogAddressIdx,

		Compression: seg.CompressNone,
		Name:        kv.LogAddrIdx,
		Accessors:   AccessorHashMap,
	},
	LogTopicIdx: InvIdxCfg{
		FilenameBase: kv.FileLogTopicsIdx, KeysTable: kv.TblLogTopicsKeys, ValuesTable: kv.TblLogTopicsIdx,

		Compression: seg.CompressNone,
		Name:        kv.LogTopicIdx,
		Accessors:   AccessorHashMap,
	},
	TracesFromIdx: InvIdxCfg{
		FilenameBase: kv.FileTracesFromIdx, KeysTable: kv.TblTracesFromKeys, ValuesTable: kv.TblTracesFromIdx,

		Compression: seg.CompressNone,
		Name:        kv.TracesFromIdx,
		Accessors:   AccessorHashMap,
	},
	TracesToIdx: InvIdxCfg{
		FilenameBase: kv.FileTracesToIdx, KeysTable: kv.TblTracesToKeys, ValuesTable: kv.TblTracesToIdx,

		Compression: seg.CompressNone,
		Name:        kv.TracesToIdx,
		Accessors:   AccessorHashMap,
	},
}

func EnableHistoricalCommitment() {
	cfg := Schema.CommitmentDomain
	cfg.Hist.HistoryDisabled = false
	cfg.Hist.SnapshotsDisabled = false
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
		Schema.ReceiptDomain.Version.DataKV = version.V1_1_standart
		Schema.ReceiptDomain.Hist.Version.DataV = version.V1_1_standart

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
	cfg.Hist.IiCfg.Disable = false
	cfg.Hist.HistoryDisabled = false
	cfg.Hist.SnapshotsDisabled = false
	Schema.RCacheDomain = cfg
}

var SchemeMinSupportedVersions = map[string]map[string]snaptype.Version{}
