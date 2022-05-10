package cmp

import (
	"golang.org/x/exp/constraints"
)

// InRange - ensure val is in [min,max] range
func InRange[T constraints.Ordered](min, max, val T) T {
	if min >= val {
		return min
	}
	if max <= val {
		return max
	}
	return val
}

func Min[T constraints.Ordered](a, b T) T {
	if a <= b {
		return a
	}
	return b
}

func Max[T constraints.Ordered](a, b T) T {
	if a >= b {
		return a
	}
	return b
}
