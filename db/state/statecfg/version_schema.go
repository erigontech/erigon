package statecfg

import (
	"github.com/erigontech/erigon/db/snaptype"
)

func InitSchemas() {
	InitSchemasGen()

	SchemeMinSupportedVersions = map[string]map[string]snaptype.Version{
		"accounts": {
			".kv":  Schema.AccountsDomain.GetVersions().Domain.DataKV.MinSupported,
			".bt":  Schema.AccountsDomain.GetVersions().Domain.AccessorBT.MinSupported,
			".kvi": Schema.AccountsDomain.GetVersions().Domain.AccessorKVI.MinSupported,
			".efi": Schema.AccountsDomain.GetVersions().II.AccessorEFI.MinSupported,
			".ef":  Schema.AccountsDomain.GetVersions().II.DataEF.MinSupported,
			".vi":  Schema.AccountsDomain.GetVersions().Hist.AccessorVI.MinSupported,
			".v":   Schema.AccountsDomain.GetVersions().Hist.DataV.MinSupported,
		},
		"code": {
			".kv":  Schema.CodeDomain.GetVersions().Domain.DataKV.MinSupported,
			".bt":  Schema.CodeDomain.GetVersions().Domain.AccessorBT.MinSupported,
			".kvi": Schema.CodeDomain.GetVersions().Domain.AccessorKVI.MinSupported,
			".efi": Schema.CodeDomain.GetVersions().II.AccessorEFI.MinSupported,
			".ef":  Schema.CodeDomain.GetVersions().II.DataEF.MinSupported,
			".vi":  Schema.CodeDomain.GetVersions().Hist.AccessorVI.MinSupported,
			".v":   Schema.CodeDomain.GetVersions().Hist.DataV.MinSupported,
		},
		"commitment": {
			".kv":  Schema.CommitmentDomain.GetVersions().Domain.DataKV.MinSupported,
			".bt":  Schema.CommitmentDomain.GetVersions().Domain.AccessorBT.MinSupported,
			".kvi": Schema.CommitmentDomain.GetVersions().Domain.AccessorKVI.MinSupported,
			".efi": Schema.CommitmentDomain.GetVersions().II.AccessorEFI.MinSupported,
			".ef":  Schema.CommitmentDomain.GetVersions().II.DataEF.MinSupported,
			".vi":  Schema.CommitmentDomain.GetVersions().Hist.AccessorVI.MinSupported,
			".v":   Schema.CommitmentDomain.GetVersions().Hist.DataV.MinSupported,
		},
		"storage": {
			".kv":  Schema.StorageDomain.GetVersions().Domain.DataKV.MinSupported,
			".bt":  Schema.StorageDomain.GetVersions().Domain.AccessorBT.MinSupported,
			".kvi": Schema.StorageDomain.GetVersions().Domain.AccessorKVI.MinSupported,
			".efi": Schema.StorageDomain.GetVersions().II.AccessorEFI.MinSupported,
			".ef":  Schema.StorageDomain.GetVersions().II.DataEF.MinSupported,
			".vi":  Schema.StorageDomain.GetVersions().Hist.AccessorVI.MinSupported,
			".v":   Schema.StorageDomain.GetVersions().Hist.DataV.MinSupported,
		},
		"receipt": {
			".kv":  Schema.ReceiptDomain.GetVersions().Domain.DataKV.MinSupported,
			".bt":  Schema.ReceiptDomain.GetVersions().Domain.AccessorBT.MinSupported,
			".kvi": Schema.ReceiptDomain.GetVersions().Domain.AccessorKVI.MinSupported,
			".efi": Schema.ReceiptDomain.GetVersions().II.AccessorEFI.MinSupported,
			".ef":  Schema.ReceiptDomain.GetVersions().II.DataEF.MinSupported,
			".vi":  Schema.ReceiptDomain.GetVersions().Hist.AccessorVI.MinSupported,
			".v":   Schema.ReceiptDomain.GetVersions().Hist.DataV.MinSupported,
		},
		"rcache": {
			".kv":  Schema.RCacheDomain.GetVersions().Domain.DataKV.MinSupported,
			".bt":  Schema.RCacheDomain.GetVersions().Domain.AccessorBT.MinSupported,
			".kvi": Schema.RCacheDomain.GetVersions().Domain.AccessorKVI.MinSupported,
			".efi": Schema.RCacheDomain.GetVersions().II.AccessorEFI.MinSupported,
			".ef":  Schema.RCacheDomain.GetVersions().II.DataEF.MinSupported,
			".vi":  Schema.RCacheDomain.GetVersions().Hist.AccessorVI.MinSupported,
			".v":   Schema.RCacheDomain.GetVersions().Hist.DataV.MinSupported,
		},
		"logaddrs": {
			".ef":  Schema.LogAddrIdx.GetVersions().II.DataEF.MinSupported,
			".efi": Schema.LogAddrIdx.GetVersions().II.AccessorEFI.MinSupported,
		},
		"logtopics": {
			".ef":  Schema.LogTopicIdx.GetVersions().II.DataEF.MinSupported,
			".efi": Schema.LogTopicIdx.GetVersions().II.AccessorEFI.MinSupported,
		},
		"tracesfrom": {
			".ef":  Schema.TracesFromIdx.GetVersions().II.DataEF.MinSupported,
			".efi": Schema.TracesFromIdx.GetVersions().II.AccessorEFI.MinSupported,
		},
		"tracesto": {
			".ef":  Schema.TracesToIdx.GetVersions().II.DataEF.MinSupported,
			".efi": Schema.TracesToIdx.GetVersions().II.AccessorEFI.MinSupported,
		},
	}
}
