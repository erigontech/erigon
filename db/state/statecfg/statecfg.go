package statecfg

import (
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/version"
)

type InvIdx struct {
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

func (ii InvIdx) GetVersions() VersionTypes {
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
