package common

import "fmt"

type number interface {
	int | int64 | uint | uint64
}

const (
	numK = uint64(1_000)
	numM = numK * numK
	numB = numM * numK
	numT = numB * numK
	numQ = numT * numK
)

// PrettyCounter print counter number in human readable format
func PrettyCounter[N number](num N) string {
	if num < N(numK) {
		return fmt.Sprintf("%d", num)
	}
	if num < N(numM) {
		// sequence %02d does not always print 2 first digits but prints whole value so we have to divide by expected /100th part
		return fmt.Sprintf("%d.%02dk", num/N(numK), num%N(numK)/N(10))
	}
	if num < N(numB) {
		return fmt.Sprintf("%d.%02dM", num/N(numM), num%N(numM)/N(numK*10))
	}
	if num < N(numT) {
		return fmt.Sprintf("%d.%02dB", num/N(numB), num%N(numB)/N(numM*10))
	}
	if num < N(numQ) {
		return fmt.Sprintf("%d.%02dT", num/N(numT), num%N(numT)/N(numB*10))
	}
	return fmt.Sprintf("%d.%02dQ", num/N(numQ), num%N(numQ)/N(numT*10))
}
