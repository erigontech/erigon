package state

import (
	"github.com/erigontech/erigon-lib/downloader/snaptype"
)

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

	Schema.ReceiptDomain.version.DataKV = snaptype.V1_1
	Schema.ReceiptDomain.version.AccessorBT = snaptype.V1_1
	Schema.ReceiptDomain.version.AccessorKVEI = snaptype.V1_1
	Schema.ReceiptDomain.hist.version.DataV = snaptype.V1_1
	Schema.ReceiptDomain.hist.version.AccessorVI = snaptype.V1_1
	Schema.ReceiptDomain.hist.iiCfg.version.DataEF = snaptype.V1_1
	Schema.ReceiptDomain.hist.iiCfg.version.AccessorEFI = snaptype.V1_1

	Schema.LogAddrIdx.version.DataEF = snaptype.V1_1
	Schema.LogAddrIdx.version.AccessorEFI = snaptype.V1_1

	Schema.LogAddrIdx.version.DataEF = snaptype.V1_1
	Schema.LogAddrIdx.version.AccessorEFI = snaptype.V1_1

	Schema.LogAddrIdx.version.DataEF = snaptype.V1_0
	Schema.LogAddrIdx.version.AccessorEFI = snaptype.V1_0

	Schema.LogAddrIdx.version.DataEF = snaptype.V1_0
	Schema.LogAddrIdx.version.AccessorEFI = snaptype.V1_0
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
