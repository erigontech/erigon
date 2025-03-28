package state

import (
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/kv"
)

const (
	DataKV = iota
	DataV
	DataEF
	DataSEG
	AccessorKVI
	AccessorKVEI
	AccessorVI
	AccessorEFI
	AccessorBT
	AccessorIDX
)

func InitSchemas() {
	SchemaAccountsDomain := Schema[kv.AccountsDomain]
	SchemaAccountsDomain.versionType.DataKV = snaptype.V1_0
	SchemaAccountsDomain.versionType.AccessorBT = snaptype.V1_0
	SchemaAccountsDomain.versionType.AccessorKVEI = snaptype.V1_0
	SchemaAccountsDomain.hist.versionType.DataV = snaptype.V1_0
	SchemaAccountsDomain.hist.versionType.AccessorVI = snaptype.V1_0
	SchemaAccountsDomain.hist.iiCfg.versionType.DataEF = snaptype.V1_0
	SchemaAccountsDomain.hist.iiCfg.versionType.AccessorEFI = snaptype.V1_0
	Schema[kv.AccountsDomain] = SchemaAccountsDomain

	SchemaStorageDomain := Schema[kv.StorageDomain]
	SchemaStorageDomain.versionType.DataKV = snaptype.V1_0
	SchemaStorageDomain.versionType.AccessorBT = snaptype.V1_0
	SchemaStorageDomain.versionType.AccessorKVEI = snaptype.V1_0
	SchemaStorageDomain.hist.versionType.DataV = snaptype.V1_0
	SchemaStorageDomain.hist.versionType.AccessorVI = snaptype.V1_0
	SchemaStorageDomain.hist.iiCfg.versionType.DataEF = snaptype.V1_0
	SchemaStorageDomain.hist.iiCfg.versionType.AccessorEFI = snaptype.V1_0
	Schema[kv.StorageDomain] = SchemaStorageDomain

	SchemaCodeDomain := Schema[kv.CodeDomain]
	SchemaCodeDomain.versionType.DataKV = snaptype.V1_0
	SchemaCodeDomain.versionType.AccessorBT = snaptype.V1_0
	SchemaCodeDomain.versionType.AccessorKVEI = snaptype.V1_0
	SchemaCodeDomain.hist.versionType.DataV = snaptype.V1_0
	SchemaCodeDomain.hist.versionType.AccessorVI = snaptype.V1_0
	SchemaCodeDomain.hist.iiCfg.versionType.DataEF = snaptype.V1_0
	SchemaCodeDomain.hist.iiCfg.versionType.AccessorEFI = snaptype.V1_0
	Schema[kv.CodeDomain] = SchemaCodeDomain

	SchemaCommitmentDomain := Schema[kv.CommitmentDomain]
	SchemaCommitmentDomain.versionType.DataKV = snaptype.V1_0
	SchemaCommitmentDomain.versionType.AccessorKVI = snaptype.V1_0
	Schema[kv.CommitmentDomain] = SchemaCommitmentDomain

	SchemaReceiptDomain := Schema[kv.ReceiptDomain]
	SchemaReceiptDomain.versionType.DataKV = snaptype.V1_1
	SchemaReceiptDomain.versionType.AccessorBT = snaptype.V1_1
	SchemaReceiptDomain.versionType.AccessorKVEI = snaptype.V1_1
	SchemaReceiptDomain.hist.versionType.DataV = snaptype.V1_1
	SchemaReceiptDomain.hist.versionType.AccessorVI = snaptype.V1_1
	SchemaReceiptDomain.hist.iiCfg.versionType.DataEF = snaptype.V1_1
	SchemaReceiptDomain.hist.iiCfg.versionType.AccessorEFI = snaptype.V1_1
	Schema[kv.ReceiptDomain] = SchemaReceiptDomain

	StandaloneIISchemaLogAddrIdx := StandaloneIISchema[kv.LogAddrIdx]
	StandaloneIISchemaLogAddrIdx.versionType.DataEF = snaptype.V1_1
	StandaloneIISchemaLogAddrIdx.versionType.AccessorEFI = snaptype.V1_1
	StandaloneIISchema[kv.LogAddrIdx] = StandaloneIISchemaLogAddrIdx

	StandaloneIISchemaLogTopicIdx := StandaloneIISchema[kv.LogTopicIdx]
	StandaloneIISchemaLogTopicIdx.versionType.DataEF = snaptype.V1_1
	StandaloneIISchemaLogTopicIdx.versionType.AccessorEFI = snaptype.V1_1
	StandaloneIISchema[kv.LogTopicIdx] = StandaloneIISchemaLogTopicIdx

	StandaloneIISchemaTracesToIdx := StandaloneIISchema[kv.TracesToIdx]
	StandaloneIISchemaTracesToIdx.versionType.DataEF = snaptype.V1_0
	StandaloneIISchemaTracesToIdx.versionType.AccessorEFI = snaptype.V1_0
	StandaloneIISchema[kv.TracesToIdx] = StandaloneIISchemaTracesToIdx

	StandaloneIISchemaTracesFromIdx := StandaloneIISchema[kv.TracesFromIdx]
	StandaloneIISchemaTracesFromIdx.versionType.DataEF = snaptype.V1_0
	StandaloneIISchemaTracesFromIdx.versionType.AccessorEFI = snaptype.V1_0
	StandaloneIISchema[kv.TracesFromIdx] = StandaloneIISchemaTracesFromIdx
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
