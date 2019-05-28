package util

import "time"

// Ternary is a collection of ternary functions.
var Ternary = ternaryUtil{}

type ternaryUtil struct{}

func (tu *ternaryUtil) OfRune(condition bool, trueResult, falseResult rune) rune {
	if condition {
		return trueResult
	}
	return falseResult
}

func (tu *ternaryUtil) OfBytes(condition bool, trueResult, falseResult []byte) []byte {
	if condition {
		return trueResult
	}
	return falseResult
}

func (tu *ternaryUtil) OfInt(condition bool, trueResult, falseResult int) int {
	if condition {
		return trueResult
	}
	return falseResult
}

func (tu *ternaryUtil) OfInt64(condition bool, trueResult, falseResult int64) int64 {
	if condition {
		return trueResult
	}
	return falseResult
}

func (tu *ternaryUtil) OfFloat64(condition bool, trueResult, falseResult float64) float64 {
	if condition {
		return trueResult
	}
	return falseResult
}

func (tu *ternaryUtil) OfTime(condition bool, trueResult, falseResult time.Time) time.Time {
	if condition {
		return trueResult
	}
	return falseResult
}

func (tu *ternaryUtil) OfDuration(condition bool, trueResult, falseResult time.Duration) time.Duration {
	if condition {
		return trueResult
	}
	return falseResult
}

func (tu *ternaryUtil) OfString(condition bool, trueResult, falseResult string) string {
	if condition {
		return trueResult
	}
	return falseResult
}
