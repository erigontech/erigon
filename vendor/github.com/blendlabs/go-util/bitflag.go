package util

var (
	// BitFlag is a namespace for bitflag functions.
	BitFlag = bitFlag{}
)

type bitFlag struct{}

// All returns if all the reference bits are set for a given value
func (bf bitFlag) All(reference, value uint64) bool {
	return reference&value == value
}

// Any returns if any the reference bits are set for a given value
func (bf bitFlag) Any(reference, value uint64) bool {
	return reference&value > 0
}

// Zero makes a given flag zero'd in the set.
func (bf bitFlag) Zero(flagSet, value uint64) uint64 {
	return flagSet ^ ((-(0) ^ value) & flagSet)
}

// Set sets a flag value to 1.
func (bf bitFlag) Set(flagSet, value uint64) uint64 {
	return flagSet | value
}

// Combine combines all the values into one flag.
func (bf bitFlag) Combine(values ...uint64) uint64 {
	var outputFlag uint64
	for _, value := range values {
		outputFlag = outputFlag | value
	}
	return outputFlag
}
