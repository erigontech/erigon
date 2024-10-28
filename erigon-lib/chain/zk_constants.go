package chain

type ForkId uint64

const (
	_              = iota
	ForkID4 ForkId = iota + 3
	ForkID5Dragonfruit
	ForkID6IncaBerry
	ForkID7Etrog
	ForkID8Elderberry
	ForkID9Elderberry2
	ForkID10
	ForkID11
	ForkID12Banana
	ForkId13Durian

	// ImpossibleForkId is a fork ID that is greater than any possible fork ID
	// Nothing should be added after this line
	ImpossibleForkId
)
