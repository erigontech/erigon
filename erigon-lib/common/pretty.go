package common

import (
	"fmt"
	"time"
)

type number interface {
	int | int64 | uint | uint64 | float64
}

const (
	numK = 1_000
	numM = numK * numK
	numG = numM * numK
	numT = numG * numK
	numQ = numT * numK
)

// PrettyCounter print counter number in human readable format
func PrettyCounter[N number](num N) string {
	if num < N(numK) {
		if num < 1 && num > 0 {
			return fmt.Sprintf("%.2f", float64(num))
		}
		return fmt.Sprintf("%d", uint64(num))
	}
	if num < N(numM) {
		// sequence %02d does not always print 2 first digits but prints whole value so we have to divide by expected /100th part
		return fmt.Sprintf("%d.%02dk", uint64(num)/numK, (uint64(num)%numK)/10)
	}
	if num < N(numG) {
		return fmt.Sprintf("%d.%02dM", uint64(num)/numM, (uint64(num)%numM)/(numK*10))
	}
	if num < N(numT) {
		return fmt.Sprintf("%d.%02dG", uint64(num)/numG, (uint64(num)%numG)/(numM*10))
	}
	if num < N(numQ) {
		return fmt.Sprintf("%d.%02dT", uint64(num)/numT, (uint64(num)%numT)/(numG*10))
	}
	return fmt.Sprintf("%d.%02dQ", uint64(num)/numQ, (uint64(num)%numQ)/(numT*10))
}

var divs = []time.Duration{
	time.Duration(1), time.Duration(10), time.Duration(100), time.Duration(1000)}

func round(d time.Duration, digits int) time.Duration {
	switch {
	case d > time.Second:
		d = d.Round(time.Second / divs[digits])
	case d > time.Millisecond:
		d = d.Round(time.Millisecond / divs[digits])
	case d > time.Microsecond:
		d = d.Round(time.Microsecond / divs[digits])
	}
	return d
}

func Round(d time.Duration, digits int) time.Duration {
	return round(d, digits)
}
