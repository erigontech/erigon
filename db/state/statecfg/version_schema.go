package statecfg

import (
	"github.com/erigontech/erigon/db/snaptype"
)

func InitSchemas() {
	InitSchemasGen()

	SchemeMinSupportedVersions = map[string]map[string]snaptype.Version{
		"accounts": {
			".kv":  Schema.AccountsDomain.Version.DataKV.MinSupported,
			".bt":  Schema.AccountsDomain.Version.AccessorBT.MinSupported,
			".kvi": Schema.AccountsDomain.Version.AccessorKVI.MinSupported,
			".efi": Schema.AccountsDomain.Hist.IiCfg.Version.AccessorEFI.MinSupported,
			".ef":  Schema.AccountsDomain.Hist.IiCfg.Version.DataEF.MinSupported,
			".vi":  Schema.AccountsDomain.Hist.Version.AccessorVI.MinSupported,
			".v":   Schema.AccountsDomain.Hist.Version.DataV.MinSupported,
		},
		"code": {
			".kv":  Schema.CodeDomain.Version.DataKV.MinSupported,
			".bt":  Schema.CodeDomain.Version.AccessorBT.MinSupported,
			".kvi": Schema.CodeDomain.Version.AccessorKVI.MinSupported,
			".efi": Schema.CodeDomain.Hist.IiCfg.Version.AccessorEFI.MinSupported,
			".ef":  Schema.CodeDomain.Hist.IiCfg.Version.DataEF.MinSupported,
			".vi":  Schema.CodeDomain.Hist.Version.AccessorVI.MinSupported,
			".v":   Schema.CodeDomain.Hist.Version.DataV.MinSupported,
		},
		"commitment": {
			".kv":  Schema.CommitmentDomain.Version.DataKV.MinSupported,
			".bt":  Schema.CommitmentDomain.Version.AccessorBT.MinSupported,
			".kvi": Schema.CommitmentDomain.Version.AccessorKVI.MinSupported,
			".efi": Schema.CommitmentDomain.Hist.IiCfg.Version.AccessorEFI.MinSupported,
			".ef":  Schema.CommitmentDomain.Hist.IiCfg.Version.DataEF.MinSupported,
			".vi":  Schema.CommitmentDomain.Hist.Version.AccessorVI.MinSupported,
			".v":   Schema.CommitmentDomain.Hist.Version.DataV.MinSupported,
		},
		"storage": {
			".kv":  Schema.StorageDomain.Version.DataKV.MinSupported,
			".bt":  Schema.StorageDomain.Version.AccessorBT.MinSupported,
			".kvi": Schema.StorageDomain.Version.AccessorKVI.MinSupported,
			".efi": Schema.StorageDomain.Hist.IiCfg.Version.AccessorEFI.MinSupported,
			".ef":  Schema.StorageDomain.Hist.IiCfg.Version.DataEF.MinSupported,
			".vi":  Schema.StorageDomain.Hist.Version.AccessorVI.MinSupported,
			".v":   Schema.StorageDomain.Hist.Version.DataV.MinSupported,
		},
		"receipt": {
			".kv":  Schema.ReceiptDomain.Version.DataKV.MinSupported,
			".bt":  Schema.ReceiptDomain.Version.AccessorBT.MinSupported,
			".kvi": Schema.ReceiptDomain.Version.AccessorKVI.MinSupported,
			".efi": Schema.ReceiptDomain.Hist.IiCfg.Version.AccessorEFI.MinSupported,
			".ef":  Schema.ReceiptDomain.Hist.IiCfg.Version.DataEF.MinSupported,
			".vi":  Schema.ReceiptDomain.Hist.Version.AccessorVI.MinSupported,
			".v":   Schema.ReceiptDomain.Hist.Version.DataV.MinSupported,
		},
		"rcache": {
			".kv":  Schema.RCacheDomain.Version.DataKV.MinSupported,
			".bt":  Schema.RCacheDomain.Version.AccessorBT.MinSupported,
			".kvi": Schema.RCacheDomain.Version.AccessorKVI.MinSupported,
			".efi": Schema.RCacheDomain.Hist.IiCfg.Version.AccessorEFI.MinSupported,
			".ef":  Schema.RCacheDomain.Hist.IiCfg.Version.DataEF.MinSupported,
			".vi":  Schema.RCacheDomain.Hist.Version.AccessorVI.MinSupported,
			".v":   Schema.RCacheDomain.Hist.Version.DataV.MinSupported,
		},
		"logaddrs": {
			".ef":  Schema.LogAddrIdx.Version.DataEF.MinSupported,
			".efi": Schema.LogAddrIdx.Version.AccessorEFI.MinSupported,
		},
		"logtopics": {
			".ef":  Schema.LogTopicIdx.Version.DataEF.MinSupported,
			".efi": Schema.LogTopicIdx.Version.AccessorEFI.MinSupported,
		},
		"tracesfrom": {
			".ef":  Schema.TracesFromIdx.Version.DataEF.MinSupported,
			".efi": Schema.TracesFromIdx.Version.AccessorEFI.MinSupported,
		},
		"tracesto": {
			".ef":  Schema.TracesToIdx.Version.DataEF.MinSupported,
			".efi": Schema.TracesToIdx.Version.AccessorEFI.MinSupported,
		},
		"headers": {
			".seg": Schema.HeadersBlock.Version.DataSeg.MinSupported,
			".idx": Schema.HeadersBlock.Version.AccessorIdx.MinSupported,
		},
		"transactions": {
			".seg": Schema.TransactionsBlock.Version.DataSeg.MinSupported,
			".idx": Schema.TransactionsBlock.Version.AccessorIdx.MinSupported,
		},
		"bodies": {
			".seg": Schema.BodiesBlock.Version.DataSeg.MinSupported,
			".idx": Schema.BodiesBlock.Version.AccessorIdx.MinSupported,
		},
		"transaction-to-block": {
			".idx": Schema.TxnHash2BlockNumBlock.Version.AccessorIdx.MinSupported,
		},
	}
}
