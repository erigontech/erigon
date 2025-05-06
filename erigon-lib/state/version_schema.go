package state

import (
	"github.com/erigontech/erigon-lib/downloader/snaptype"
)

var commitmentDomainVersion snaptype.Version

func InitSchemas() {
	Schema.AccountsDomain.version.DataKV = snaptype.V1_0
	Schema.AccountsDomain.version.AccessorBT = snaptype.V1_0
	Schema.AccountsDomain.version.AccessorKVEI = snaptype.V1_0
	Schema.AccountsDomain.hist.version.DataV = snaptype.V1_0
	Schema.AccountsDomain.hist.version.AccessorVI = snaptype.V1_0
	Schema.AccountsDomain.hist.iiCfg.version.DataEF = snaptype.V1_0
	Schema.AccountsDomain.hist.iiCfg.version.AccessorEFI = snaptype.V1_0

	Schema.StorageDomain.version.DataKV = snaptype.V1_0
	Schema.StorageDomain.version.AccessorBT = snaptype.V1_0
	Schema.StorageDomain.version.AccessorKVEI = snaptype.V1_0
	Schema.StorageDomain.hist.version.DataV = snaptype.V1_0
	Schema.StorageDomain.hist.version.AccessorVI = snaptype.V1_0
	Schema.StorageDomain.hist.iiCfg.version.DataEF = snaptype.V1_0
	Schema.StorageDomain.hist.iiCfg.version.AccessorEFI = snaptype.V1_0

	Schema.CodeDomain.version.DataKV = snaptype.V1_0
	Schema.CodeDomain.version.AccessorBT = snaptype.V1_0
	Schema.CodeDomain.version.AccessorKVEI = snaptype.V1_0
	Schema.CodeDomain.hist.version.DataV = snaptype.V1_0
	Schema.CodeDomain.hist.version.AccessorVI = snaptype.V1_0
	Schema.CodeDomain.hist.iiCfg.version.DataEF = snaptype.V1_0
	Schema.CodeDomain.hist.iiCfg.version.AccessorEFI = snaptype.V1_0

	Schema.CommitmentDomain.version.DataKV = snaptype.V1_0
	Schema.CommitmentDomain.version.AccessorKVI = snaptype.V1_0
	Schema.CommitmentDomain.hist.version.DataV = snaptype.V1_0
	Schema.CommitmentDomain.hist.version.AccessorVI = snaptype.V1_0
	Schema.CommitmentDomain.hist.iiCfg.version.DataEF = snaptype.V1_0
	Schema.CommitmentDomain.hist.iiCfg.version.AccessorEFI = snaptype.V1_0
	commitmentDomainVersion = Schema.CommitmentDomain.version.DataKV

	Schema.ReceiptDomain.version.DataKV = snaptype.V1_0
	Schema.ReceiptDomain.version.AccessorBT = snaptype.V1_0
	Schema.ReceiptDomain.version.AccessorKVEI = snaptype.V1_0
	Schema.ReceiptDomain.hist.version.DataV = snaptype.V1_0
	Schema.ReceiptDomain.hist.version.AccessorVI = snaptype.V1_0
	Schema.ReceiptDomain.hist.iiCfg.version.DataEF = snaptype.V1_0
	Schema.ReceiptDomain.hist.iiCfg.version.AccessorEFI = snaptype.V1_0

	Schema.RCacheDomain.version.DataKV = snaptype.V1_0
	Schema.RCacheDomain.version.AccessorKVI = snaptype.V1_0
	Schema.RCacheDomain.hist.version.DataV = snaptype.V1_0
	Schema.RCacheDomain.hist.version.AccessorVI = snaptype.V1_0
	Schema.RCacheDomain.hist.iiCfg.version.DataEF = snaptype.V1_0
	Schema.RCacheDomain.hist.iiCfg.version.AccessorEFI = snaptype.V1_0

	Schema.LogAddrIdx.version.DataEF = snaptype.V1_0
	Schema.LogAddrIdx.version.AccessorEFI = snaptype.V1_0

	Schema.LogTopicIdx.version.DataEF = snaptype.V1_0
	Schema.LogTopicIdx.version.AccessorEFI = snaptype.V1_0

	Schema.TracesFromIdx.version.DataEF = snaptype.V1_0
	Schema.TracesFromIdx.version.AccessorEFI = snaptype.V1_0

	Schema.TracesToIdx.version.DataEF = snaptype.V1_0
	Schema.TracesToIdx.version.AccessorEFI = snaptype.V1_0
}

type DomainVersionTypes struct {
	DataKV       snaptype.Version
	AccessorBT   snaptype.Version
	AccessorKVEI snaptype.Version
	AccessorKVI  snaptype.Version
}

type HistVersionTypes struct {
	DataV      snaptype.Version
	AccessorVI snaptype.Version
}

type IIVersionTypes struct {
	DataEF      snaptype.Version
	AccessorEFI snaptype.Version
}

type VersionTypes struct {
	Hist   *HistVersionTypes
	Domain *DomainVersionTypes
	II     *IIVersionTypes
}
