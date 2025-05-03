package state

import (
	"github.com/erigontech/erigon-lib/version"
)

var commitmentDomainVersion version.Versions

func InitSchemas() {
	Schema.AccountsDomain.version.DataKV = version.V1_0_standart
	Schema.AccountsDomain.version.AccessorBT = version.V1_0_standart
	Schema.AccountsDomain.version.AccessorKVEI = version.V1_0_standart
	Schema.AccountsDomain.hist.version.DataV = version.V1_0_standart
	Schema.AccountsDomain.hist.version.AccessorVI = version.V1_0_standart
	Schema.AccountsDomain.hist.iiCfg.version.DataEF = version.V2_0_standart
	Schema.AccountsDomain.hist.iiCfg.version.AccessorEFI = version.V1_1_standart
	Schema.AccountsDomain.hist.iiCfg.version.AccessorEFEI = snaptype.V1_0_standart

	Schema.StorageDomain.version.DataKV = version.V1_0_standart
	Schema.StorageDomain.version.AccessorBT = version.V1_0_standart
	Schema.StorageDomain.version.AccessorKVEI = version.V1_0_standart
	Schema.StorageDomain.hist.version.DataV = version.V1_0_standart
	Schema.StorageDomain.hist.version.AccessorVI = version.V1_0_standart
	Schema.StorageDomain.hist.iiCfg.version.DataEF = version.V2_0_standart
	Schema.StorageDomain.hist.iiCfg.version.AccessorEFI = version.V1_1_standart
	Schema.StorageDomain.hist.iiCfg.version.AccessorEFEI = snaptype.V1_0_standart

	Schema.CodeDomain.version.DataKV = version.V1_0_standart
	Schema.CodeDomain.version.AccessorBT = version.V1_0_standart
	Schema.CodeDomain.version.AccessorKVEI = version.V1_0_standart
	Schema.CodeDomain.hist.version.DataV = version.V1_0_standart
	Schema.CodeDomain.hist.version.AccessorVI = version.V1_0_standart
	Schema.CodeDomain.hist.iiCfg.version.DataEF = version.V2_0_standart
	Schema.CodeDomain.hist.iiCfg.version.AccessorEFI = version.V1_1_standart
	Schema.CodeDomain.hist.iiCfg.version.AccessorEFEI = snaptype.V1_0_standart

	Schema.CommitmentDomain.version.DataKV = version.V1_0_standart
	Schema.CommitmentDomain.version.AccessorKVI = version.V1_0_standart
	Schema.CommitmentDomain.hist.version.DataV = version.V1_0_standart
	Schema.CommitmentDomain.hist.version.AccessorVI = version.V1_0_standart
	Schema.CommitmentDomain.hist.iiCfg.version.DataEF = version.V2_0_standart
	Schema.CommitmentDomain.hist.iiCfg.version.AccessorEFI = version.V1_1_standart
	Schema.CommitmentDomain.hist.iiCfg.version.AccessorEFEI = snaptype.V1_0_standart
	commitmentDomainVersion = Schema.CommitmentDomain.version.DataKV

	Schema.ReceiptDomain.version.DataKV = version.V1_0_standart
	Schema.ReceiptDomain.version.AccessorBT = version.V1_0_standart
	Schema.ReceiptDomain.version.AccessorKVEI = version.V1_0_standart
	Schema.ReceiptDomain.hist.version.DataV = version.V1_0_standart
	Schema.ReceiptDomain.hist.version.AccessorVI = version.V1_0_standart
	Schema.ReceiptDomain.hist.iiCfg.version.DataEF = version.V2_0_standart
	Schema.ReceiptDomain.hist.iiCfg.version.AccessorEFI = version.V1_1_standart
	Schema.ReceiptDomain.hist.iiCfg.version.AccessorEFEI = snaptype.V1_0_standart

	Schema.RCacheDomain.version.DataKV = version.V1_0_standart
	Schema.RCacheDomain.version.AccessorKVI = version.V1_0_standart
	Schema.RCacheDomain.hist.version.DataV = version.V1_0_standart
	Schema.RCacheDomain.hist.version.AccessorVI = version.V1_0_standart
	Schema.RCacheDomain.hist.iiCfg.version.DataEF = version.V2_0_standart
	Schema.RCacheDomain.hist.iiCfg.version.AccessorEFI = version.V1_1_standart
	Schema.RCacheDomain.hist.iiCfg.version.AccessorEFEI = snaptype.V1_0_standart

	Schema.LogAddrIdx.version.DataEF = version.V2_0_standart
	Schema.LogAddrIdx.version.AccessorEFI = version.V1_1_standart
	Schema.LogAddrIdx.version.AccessorEFEI = snaptype.V1_0_standart

	Schema.LogTopicIdx.version.DataEF = version.V2_0_standart
	Schema.LogTopicIdx.version.AccessorEFI = version.V1_1_standart
	Schema.LogTopicIdx.version.AccessorEFEI = snaptype.V1_0_standart

	Schema.TracesFromIdx.version.DataEF = version.V2_0_standart
	Schema.TracesFromIdx.version.AccessorEFI = version.V1_1_standart
	Schema.TracesFromIdx.version.AccessorEFEI = snaptype.V1_0_standart

	Schema.TracesToIdx.version.DataEF = version.V2_0_standart
	Schema.TracesToIdx.version.AccessorEFI = version.V1_1_standart
	Schema.TracesToIdx.version.AccessorEFEI = snaptype.V1_0_standart
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
	AccessorEFEI snaptype.Version
}

type VersionTypes struct {
	Hist   *HistVersionTypes
	Domain *DomainVersionTypes
	II     *IIVersionTypes
}
