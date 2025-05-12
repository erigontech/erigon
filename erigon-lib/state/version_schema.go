package state

import (
	"github.com/erigontech/erigon-lib/version"
)

var commitmentDomainVersion version.Version

func InitSchemas() {
	Schema.AccountsDomain.version.DataKV = version.V1_0
	Schema.AccountsDomain.version.AccessorBT = version.V1_0
	Schema.AccountsDomain.version.AccessorKVEI = version.V1_0
	Schema.AccountsDomain.hist.version.DataV = version.V1_0
	Schema.AccountsDomain.hist.version.AccessorVI = version.V1_0
	Schema.AccountsDomain.hist.iiCfg.version.DataEF = version.V2_0
	Schema.AccountsDomain.hist.iiCfg.version.AccessorEFI = version.V1_1

	Schema.StorageDomain.version.DataKV = version.V1_0
	Schema.StorageDomain.version.AccessorBT = version.V1_0
	Schema.StorageDomain.version.AccessorKVEI = version.V1_0
	Schema.StorageDomain.hist.version.DataV = version.V1_0
	Schema.StorageDomain.hist.version.AccessorVI = version.V1_0
	Schema.StorageDomain.hist.iiCfg.version.DataEF = version.V2_0
	Schema.StorageDomain.hist.iiCfg.version.AccessorEFI = version.V1_1

	Schema.CodeDomain.version.DataKV = version.V1_0
	Schema.CodeDomain.version.AccessorBT = version.V1_0
	Schema.CodeDomain.version.AccessorKVEI = version.V1_0
	Schema.CodeDomain.hist.version.DataV = version.V1_0
	Schema.CodeDomain.hist.version.AccessorVI = version.V1_0
	Schema.CodeDomain.hist.iiCfg.version.DataEF = version.V2_0
	Schema.CodeDomain.hist.iiCfg.version.AccessorEFI = version.V1_1

	Schema.CommitmentDomain.version.DataKV = version.V1_0
	Schema.CommitmentDomain.version.AccessorKVI = version.V1_0
	Schema.CommitmentDomain.hist.version.DataV = version.V1_0
	Schema.CommitmentDomain.hist.version.AccessorVI = version.V1_0
	Schema.CommitmentDomain.hist.iiCfg.version.DataEF = version.V2_0
	Schema.CommitmentDomain.hist.iiCfg.version.AccessorEFI = version.V1_1
	commitmentDomainVersion = Schema.CommitmentDomain.version.DataKV

	Schema.ReceiptDomain.version.DataKV = version.V1_0
	Schema.ReceiptDomain.version.AccessorBT = version.V1_0
	Schema.ReceiptDomain.version.AccessorKVEI = version.V1_0
	Schema.ReceiptDomain.hist.version.DataV = version.V1_0
	Schema.ReceiptDomain.hist.version.AccessorVI = version.V1_0
	Schema.ReceiptDomain.hist.iiCfg.version.DataEF = version.V2_0
	Schema.ReceiptDomain.hist.iiCfg.version.AccessorEFI = version.V1_1

	Schema.RCacheDomain.version.DataKV = version.V1_0
	Schema.RCacheDomain.version.AccessorKVI = version.V1_0
	Schema.RCacheDomain.hist.version.DataV = version.V1_0
	Schema.RCacheDomain.hist.version.AccessorVI = version.V1_0
	Schema.RCacheDomain.hist.iiCfg.version.DataEF = version.V2_0
	Schema.RCacheDomain.hist.iiCfg.version.AccessorEFI = version.V1_1

	Schema.LogAddrIdx.version.DataEF = version.V2_0
	Schema.LogAddrIdx.version.AccessorEFI = version.V1_1

	Schema.LogTopicIdx.version.DataEF = version.V2_0
	Schema.LogTopicIdx.version.AccessorEFI = version.V1_1

	Schema.TracesFromIdx.version.DataEF = version.V2_0
	Schema.TracesFromIdx.version.AccessorEFI = version.V1_1

	Schema.TracesToIdx.version.DataEF = version.V2_0
	Schema.TracesToIdx.version.AccessorEFI = version.V1_1
}

type DomainVersionTypes struct {
	DataKV       version.Version
	AccessorBT   version.Version
	AccessorKVEI version.Version
	AccessorKVI  version.Version
}

type HistVersionTypes struct {
	DataV      version.Version
	AccessorVI version.Version
}

type IIVersionTypes struct {
	DataEF      version.Version
	AccessorEFI version.Version
}

type VersionTypes struct {
	Hist   *HistVersionTypes
	Domain *DomainVersionTypes
	II     *IIVersionTypes
}
