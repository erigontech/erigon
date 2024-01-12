package evmtypes

// IntraBlockState is an EVM database for full state querying.
type ZKIntraBlockState interface {
	IntraBlockState
	GetTxCount() (uint64, error)
}
