package statecfg

import (
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
)

type HistCfg struct {
	IiCfg InvIdx

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
