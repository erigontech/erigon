package statecfg

import (
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/version"
)

type DomainCfg struct {
	Hist HistCfg

	Name        kv.Domain
	Compression seg.FileCompression
	CompressCfg seg.Cfg
	Accessors   Accessors // list of indexes for given domain
	ValuesTable string    // bucket to store domain values; key -> inverted_step + values (Dupsort)
	LargeValues bool

	// replaceKeysInValues allows to replace commitment branch values with shorter keys.
	// for commitment domain only
	ReplaceKeysInValues bool

	Version DomainVersionTypes
}

func (d DomainCfg) Tables() []string {
	return []string{d.ValuesTable, d.Hist.ValuesTable, d.Hist.IiCfg.KeysTable, d.Hist.IiCfg.ValuesTable}
}

func (d DomainCfg) GetVersions() VersionTypes {
	return VersionTypes{
		Domain: &d.Version,
		Hist:   &d.Hist.Version,
		II:     &d.Hist.IiCfg.Version,
	}
}

type HistCfg struct {
	IiCfg InvIdxCfg

	ValuesTable string // bucket for history values; key1+key2+txnNum -> oldValue , stores values BEFORE change

	KeepRecentTxnInDB uint64 // When snapshotsDisabled=true, keepRecentTxnInDB is used to keep this amount of txn in db before pruning

	// historyLargeValues: used to store values > 2kb (pageSize/2)
	// small values - can be stored in more compact ways in db (DupSort feature)
	// can't use DupSort optimization (aka. prefix-compression) if values size > 4kb

	// historyLargeValues=true - doesn't support keys of various length (all keys must have same length)
	// not large:
	//   keys: txNum -> key1+key2
	//   vals: key1+key2 -> txNum + value (DupSort)
	// large:
	//   keys: txNum -> key1+key2
	//   vals: key1+key2+txNum -> value (not DupSort)
	HistoryLargeValues bool
	SnapshotsDisabled  bool // don't produce .v and .ef files, keep in db table. old data will be pruned anyway.
	HistoryDisabled    bool // skip all write operations to this History (even in DB)

	HistoryValuesOnCompressedPage int // when collating .v files: concat 16 values and snappy them

	Accessors     Accessors
	CompressorCfg seg.Cfg             // Compression settings for history files
	Compression   seg.FileCompression // defines type of Compression for history files
	HistoryIdx    kv.InvertedIdx

	Version HistVersionTypes
}

func (h HistCfg) GetVersions() VersionTypes {
	return VersionTypes{
		Hist: &h.Version,
		II:   &h.IiCfg.Version,
	}
}

type InvIdxCfg struct {
	Disable bool // totally disable Domain/History/InvertedIndex - ignore all writes, don't produce files

	Version IIVersionTypes

	FilenameBase string // filename base for all files of this inverted index
	KeysTable    string // bucket name for index keys;    txnNum_u64 -> key (k+auto_increment)
	ValuesTable  string // bucket name for index values;  k -> txnNum_u64 , Needs to be table with DupSort
	Name         kv.InvertedIdx

	Compression   seg.FileCompression // compression type for inverted index keys and values
	CompressorCfg seg.Cfg             // advanced configuration for compressor encodings

	Accessors Accessors
}

func (ii InvIdxCfg) GetVersions() VersionTypes {
	return VersionTypes{
		II: &ii.Version,
	}
}

type DomainVersionTypes struct {
	DataKV       version.Versions
	AccessorBT   version.Versions
	AccessorKVEI version.Versions
	AccessorKVI  version.Versions
}

type HistVersionTypes struct {
	DataV      version.Versions
	AccessorVI version.Versions
}

type IIVersionTypes struct {
	DataEF      version.Versions
	AccessorEFI version.Versions
}

type VersionTypes struct {
	Hist   *HistVersionTypes
	Domain *DomainVersionTypes
	II     *IIVersionTypes
}
