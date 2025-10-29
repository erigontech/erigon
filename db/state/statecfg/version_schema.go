package statecfg

import (
	"github.com/erigontech/erigon/db/snaptype"
)

func InitSchemas() {
	InitSchemasGen()

	SchemeMinSupportedVersions = map[string]map[string]snaptype.Version{
		"accounts": {
			".kv":  Schema.AccountsDomain.FileVersion.DataKV.MinSupported,
			".bt":  Schema.AccountsDomain.FileVersion.AccessorBT.MinSupported,
			".kvi": Schema.AccountsDomain.FileVersion.AccessorKVI.MinSupported,
			".efi": Schema.AccountsDomain.Hist.IiCfg.FileVersion.AccessorEFI.MinSupported,
			".ef":  Schema.AccountsDomain.Hist.IiCfg.FileVersion.DataEF.MinSupported,
			".vi":  Schema.AccountsDomain.Hist.FileVersion.AccessorVI.MinSupported,
			".v":   Schema.AccountsDomain.Hist.FileVersion.DataV.MinSupported,
		},
		"code": {
			".kv":  Schema.CodeDomain.FileVersion.DataKV.MinSupported,
			".bt":  Schema.CodeDomain.FileVersion.AccessorBT.MinSupported,
			".kvi": Schema.CodeDomain.FileVersion.AccessorKVI.MinSupported,
			".efi": Schema.CodeDomain.Hist.IiCfg.FileVersion.AccessorEFI.MinSupported,
			".ef":  Schema.CodeDomain.Hist.IiCfg.FileVersion.DataEF.MinSupported,
			".vi":  Schema.CodeDomain.Hist.FileVersion.AccessorVI.MinSupported,
			".v":   Schema.CodeDomain.Hist.FileVersion.DataV.MinSupported,
		},
		"commitment": {
			".kv":  Schema.CommitmentDomain.FileVersion.DataKV.MinSupported,
			".bt":  Schema.CommitmentDomain.FileVersion.AccessorBT.MinSupported,
			".kvi": Schema.CommitmentDomain.FileVersion.AccessorKVI.MinSupported,
			".efi": Schema.CommitmentDomain.Hist.IiCfg.FileVersion.AccessorEFI.MinSupported,
			".ef":  Schema.CommitmentDomain.Hist.IiCfg.FileVersion.DataEF.MinSupported,
			".vi":  Schema.CommitmentDomain.Hist.FileVersion.AccessorVI.MinSupported,
			".v":   Schema.CommitmentDomain.Hist.FileVersion.DataV.MinSupported,
		},
		"storage": {
			".kv":  Schema.StorageDomain.FileVersion.DataKV.MinSupported,
			".bt":  Schema.StorageDomain.FileVersion.AccessorBT.MinSupported,
			".kvi": Schema.StorageDomain.FileVersion.AccessorKVI.MinSupported,
			".efi": Schema.StorageDomain.Hist.IiCfg.FileVersion.AccessorEFI.MinSupported,
			".ef":  Schema.StorageDomain.Hist.IiCfg.FileVersion.DataEF.MinSupported,
			".vi":  Schema.StorageDomain.Hist.FileVersion.AccessorVI.MinSupported,
			".v":   Schema.StorageDomain.Hist.FileVersion.DataV.MinSupported,
		},
		"receipt": {
			".kv":  Schema.ReceiptDomain.FileVersion.DataKV.MinSupported,
			".bt":  Schema.ReceiptDomain.FileVersion.AccessorBT.MinSupported,
			".kvi": Schema.ReceiptDomain.FileVersion.AccessorKVI.MinSupported,
			".efi": Schema.ReceiptDomain.Hist.IiCfg.FileVersion.AccessorEFI.MinSupported,
			".ef":  Schema.ReceiptDomain.Hist.IiCfg.FileVersion.DataEF.MinSupported,
			".vi":  Schema.ReceiptDomain.Hist.FileVersion.AccessorVI.MinSupported,
			".v":   Schema.ReceiptDomain.Hist.FileVersion.DataV.MinSupported,
		},
		"rcache": {
			".kv":  Schema.RCacheDomain.FileVersion.DataKV.MinSupported,
			".bt":  Schema.RCacheDomain.FileVersion.AccessorBT.MinSupported,
			".kvi": Schema.RCacheDomain.FileVersion.AccessorKVI.MinSupported,
			".efi": Schema.RCacheDomain.Hist.IiCfg.FileVersion.AccessorEFI.MinSupported,
			".ef":  Schema.RCacheDomain.Hist.IiCfg.FileVersion.DataEF.MinSupported,
			".vi":  Schema.RCacheDomain.Hist.FileVersion.AccessorVI.MinSupported,
			".v":   Schema.RCacheDomain.Hist.FileVersion.DataV.MinSupported,
		},
		"logaddrs": {
			".ef":  Schema.LogAddrIdx.FileVersion.DataEF.MinSupported,
			".efi": Schema.LogAddrIdx.FileVersion.AccessorEFI.MinSupported,
		},
		"logtopics": {
			".ef":  Schema.LogTopicIdx.FileVersion.DataEF.MinSupported,
			".efi": Schema.LogTopicIdx.FileVersion.AccessorEFI.MinSupported,
		},
		"tracesfrom": {
			".ef":  Schema.TracesFromIdx.FileVersion.DataEF.MinSupported,
			".efi": Schema.TracesFromIdx.FileVersion.AccessorEFI.MinSupported,
		},
		"tracesto": {
			".ef":  Schema.TracesToIdx.FileVersion.DataEF.MinSupported,
			".efi": Schema.TracesToIdx.FileVersion.AccessorEFI.MinSupported,
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
