package generics

import (
	"golang.org/x/exp/constraints"
)

func InRange[T constraints.Ordered](a, b, c T) T {
	return Min(b, Max(a, c))
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
