package state

// Config is state config
type Config struct {
	// MaxCumulativeGasUsed is the max gas allowed per batch
	MaxCumulativeGasUsed uint64

	// ChainID is the L2 ChainID provided by the Network Config
	ChainID uint64

	// ForkIdIntervals is the list of fork id intervals
	ForkIDIntervals []ForkIDInterval
}
