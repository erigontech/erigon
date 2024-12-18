package appendables

// appendables which don't store non-canonical data
// or which tsId = tsNum always
// 1. block or slot number is step key

// type SourceKeyGenerator[SourceKey any] interface {
// 	FromStepKey(stepKeyFrom, stepKeyTo uint64, tx kv.Tx) stream.Uno[SourceKey]
// 	FromTsNum(tsNum uint64, tx kv.Tx) SourceKey
// 	FromTsId(tsId uint64, forkId []byte, tx kv.Tx) SourceKey
// }

// type ValueFetcher[SourceKey any, SourceValue any] interface {
// 	GetValues(sourceKey SourceKey, tx kv.Tx) (values []SourceValue, shouldSkip bool, err error)
// }

// type ValueProcessor[SourceKey any, SourceValue any] interface {
// 	Process(sourceKey SourceKey, values []SourceValue) (data any, shouldSkip bool, err error)
// }

// type ValuePutter interface {
// 	Put(tsId uint64, forkId []byte, value []byte, tx kv.RwTx)
// }
