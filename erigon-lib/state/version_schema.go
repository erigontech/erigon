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
	SchemaAccountsDomain.version.DataKV = snaptype.V1_0
	SchemaAccountsDomain.version.AccessorBT = snaptype.V1_0
	SchemaAccountsDomain.version.AccessorKVEI = snaptype.V1_0
	SchemaAccountsDomain.hist.version.DataV = snaptype.V1_0
	SchemaAccountsDomain.hist.version.AccessorVI = snaptype.V1_0
	SchemaAccountsDomain.hist.iiCfg.version.DataEF = snaptype.V1_0
	SchemaAccountsDomain.hist.iiCfg.version.AccessorEFI = snaptype.V1_0
	Schema[kv.AccountsDomain] = SchemaAccountsDomain

	SchemaStorageDomain := Schema[kv.StorageDomain]
	SchemaStorageDomain.version.DataKV = snaptype.V1_0
	SchemaStorageDomain.version.AccessorBT = snaptype.V1_0
	SchemaStorageDomain.version.AccessorKVEI = snaptype.V1_0
	SchemaStorageDomain.hist.version.DataV = snaptype.V1_0
	SchemaStorageDomain.hist.version.AccessorVI = snaptype.V1_0
	SchemaStorageDomain.hist.iiCfg.version.DataEF = snaptype.V1_0
	SchemaStorageDomain.hist.iiCfg.version.AccessorEFI = snaptype.V1_0
	Schema[kv.StorageDomain] = SchemaStorageDomain

	SchemaCodeDomain := Schema[kv.CodeDomain]
	SchemaCodeDomain.version.DataKV = snaptype.V1_0
	SchemaCodeDomain.version.AccessorBT = snaptype.V1_0
	SchemaCodeDomain.version.AccessorKVEI = snaptype.V1_0
	SchemaCodeDomain.hist.version.DataV = snaptype.V1_0
	SchemaCodeDomain.hist.version.AccessorVI = snaptype.V1_0
	SchemaCodeDomain.hist.iiCfg.version.DataEF = snaptype.V1_0
	SchemaCodeDomain.hist.iiCfg.version.AccessorEFI = snaptype.V1_0
	Schema[kv.CodeDomain] = SchemaCodeDomain

	SchemaCommitmentDomain := Schema[kv.CommitmentDomain]
	SchemaCommitmentDomain.version.DataKV = snaptype.V1_0
	SchemaCommitmentDomain.version.AccessorKVI = snaptype.V1_0
	Schema[kv.CommitmentDomain] = SchemaCommitmentDomain

	SchemaReceiptDomain := Schema[kv.ReceiptDomain]
	SchemaReceiptDomain.version.DataKV = snaptype.V1_1
	SchemaReceiptDomain.version.AccessorBT = snaptype.V1_1
	SchemaReceiptDomain.version.AccessorKVEI = snaptype.V1_1
	SchemaReceiptDomain.hist.version.DataV = snaptype.V1_1
	SchemaReceiptDomain.hist.version.AccessorVI = snaptype.V1_1
	SchemaReceiptDomain.hist.iiCfg.version.DataEF = snaptype.V1_1
	SchemaReceiptDomain.hist.iiCfg.version.AccessorEFI = snaptype.V1_1
	Schema[kv.ReceiptDomain] = SchemaReceiptDomain

	StandaloneIISchemaLogAddrIdx := StandaloneIISchema[kv.LogAddrIdx]
	StandaloneIISchemaLogAddrIdx.version.DataEF = snaptype.V1_1
	StandaloneIISchemaLogAddrIdx.version.AccessorEFI = snaptype.V1_1
	StandaloneIISchema[kv.LogAddrIdx] = StandaloneIISchemaLogAddrIdx

	StandaloneIISchemaLogTopicIdx := StandaloneIISchema[kv.LogTopicIdx]
	StandaloneIISchemaLogTopicIdx.version.DataEF = snaptype.V1_1
	StandaloneIISchemaLogTopicIdx.version.AccessorEFI = snaptype.V1_1
	StandaloneIISchema[kv.LogTopicIdx] = StandaloneIISchemaLogTopicIdx

	StandaloneIISchemaTracesToIdx := StandaloneIISchema[kv.TracesToIdx]
	StandaloneIISchemaTracesToIdx.version.DataEF = snaptype.V1_0
	StandaloneIISchemaTracesToIdx.version.AccessorEFI = snaptype.V1_0
	StandaloneIISchema[kv.TracesToIdx] = StandaloneIISchemaTracesToIdx

	StandaloneIISchemaTracesFromIdx := StandaloneIISchema[kv.TracesFromIdx]
	StandaloneIISchemaTracesFromIdx.version.DataEF = snaptype.V1_0
	StandaloneIISchemaTracesFromIdx.version.AccessorEFI = snaptype.V1_0
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
